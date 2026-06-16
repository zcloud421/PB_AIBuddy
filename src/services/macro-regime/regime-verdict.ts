import type { DailyPriceBar } from '../../data/massive-fetcher';
import type { FredPoint } from '../../data/fred-series-fetcher';
import type {
    CreditRegimeState,
    MacroRegimeSnapshot,
    RegimeVerdict,
    RegimeVerdictBrakeStatus,
    RegimeVerdictMechanism
} from './types';

export interface RealRateBrake {
    status: RegimeVerdictBrakeStatus;
    latest_real_rate_pct: number | null;
    delta_8w_bp: number | null;
    equity_drawdown_pct: number | null;
    equity_below_ma50: boolean | null;
    equity_below_ma200: boolean | null;
    notes: string[];
}

interface PriceVolStress {
    stress: boolean;
    qqq_drawdown_pct: number | null;
    qqq_below_ma50: boolean | null;
    vix_elevated: boolean;
}

const REAL_RATE_FORMING_DELTA_8W_BP = 25;
const REAL_RATE_VERDICT_FORMING_DELTA_8W_BP = 40;
const REAL_RATE_CONFIRMED_DELTA_8W_BP = 50;
const EQUITY_DRAWDOWN_DURATION_PRESSURE_PCT = 3;
const EQUITY_DRAWDOWN_STRESS_PCT = 5;

export function computeRealRateBrake(
    dfii10Series: FredPoint[] | null,
    equityBars: DailyPriceBar[],
    creditRegimeState: CreditRegimeState = 'NOISE'
): RealRateBrake {
    const notes: string[] = [];
    const latest = latestFredPoint(dfii10Series ?? []);
    const eightWeeksBack = pointCalendarDaysBack(dfii10Series ?? [], 56);
    const equity = computeEquityState(equityBars);

    if (!latest || !eightWeeksBack) {
        return {
            status: 'quiet',
            latest_real_rate_pct: latest?.value ?? null,
            delta_8w_bp: null,
            equity_drawdown_pct: equity.drawdown_pct,
            equity_below_ma50: equity.below_ma50,
            equity_below_ma200: equity.below_ma200,
            notes: ['DFII10 8周历史不足,实际利率刹车保持安静']
        };
    }

    const delta8wBp = (latest.value - eightWeeksBack.value) * 100;
    notes.push(`DFII10 ${latest.value.toFixed(2)}%, 8周变化 ${signedBp(delta8wBp)}`);
    if (equity.drawdown_pct !== null) {
        notes.push(`QQQ 距 60日高点 -${equity.drawdown_pct.toFixed(1)}%`);
    }

    const equityStress =
        (equity.drawdown_pct !== null && equity.drawdown_pct >= EQUITY_DRAWDOWN_STRESS_PCT) ||
        equity.below_ma50 === true ||
        equity.below_ma200 === true;
    const creditCalm = creditRegimeState === 'NOISE';

    if (!creditCalm) {
        notes.push('信用刹车已启动,实际利率不重复确认同一 break');
        return {
            status: 'quiet',
            latest_real_rate_pct: round2(latest.value),
            delta_8w_bp: round1(delta8wBp),
            equity_drawdown_pct: equity.drawdown_pct,
            equity_below_ma50: equity.below_ma50,
            equity_below_ma200: equity.below_ma200,
            notes
        };
    }

    let status: RegimeVerdictBrakeStatus = 'quiet';
    if (delta8wBp >= REAL_RATE_CONFIRMED_DELTA_8W_BP && equityStress) {
        status = 'confirmed';
        notes.push('实际利率 8周重定价 + 权益承压,利率/久期刹车确认');
    } else if (delta8wBp >= REAL_RATE_FORMING_DELTA_8W_BP) {
        status = 'forming';
        notes.push('实际利率上行进入观察区,等待权益压力确认');
    }

    return {
        status,
        latest_real_rate_pct: round2(latest.value),
        delta_8w_bp: round1(delta8wBp),
        equity_drawdown_pct: equity.drawdown_pct,
        equity_below_ma50: equity.below_ma50,
        equity_below_ma200: equity.below_ma200,
        notes
    };
}

export function computeRegimeVerdict(
    snapshot: MacroRegimeSnapshot,
    realRateBrake: RealRateBrake,
    equityBars: DailyPriceBar[] = []
): RegimeVerdict {
    const priceVol = computePriceVolStress(snapshot, equityBars);
    const vixCross = priceVol.vix_elevated;
    const creditStatus = creditBrakeStatus(snapshot.credit_funding_stress.credit_regime_state ?? 'NOISE', vixCross);
    const fundamentalStatus = fundamentalBrakeStatus(
        snapshot.fundamental_modifier.state,
        snapshot.fundamental_modifier.escalation_level
    );
    const ratesStatus = realRateBrake.status;
    const verdictRatesStatus = verdictDrivingRatesStatus(realRateBrake);
    const crowdingStatus = crowdingElevated(snapshot) ? 'elevated' : 'quiet';

    const brakes = {
        credit: creditStatus,
        rates: ratesStatus,
        fundamental: fundamentalStatus,
        crowding: crowdingStatus
    } as RegimeVerdict['brakes'];

    const confirmed = firstMechanism([
        ['credit', creditStatus],
        ['rates', verdictRatesStatus],
        ['fundamental', fundamentalStatus]
    ], 'confirmed');
    if (confirmed) {
        return {
            state: 'CONFIRMED_BREAK',
            mechanism: confirmed,
            one_line: confirmedLine(confirmed, snapshot, realRateBrake),
            confidence: confidenceFor(brakes, 'CONFIRMED_BREAK'),
            watch: watchLine(snapshot, realRateBrake, priceVol),
            brakes
        };
    }

    const forming = firstMechanism([
        ['credit', creditStatus],
        ['rates', verdictRatesStatus],
        ['fundamental', fundamentalStatus]
    ], 'forming');
    if (forming) {
        return {
            state: 'BREAK_FORMING',
            mechanism: forming,
            one_line: formingLine(forming, snapshot, realRateBrake),
            confidence: confidenceFor(brakes, 'BREAK_FORMING'),
            watch: watchLine(snapshot, realRateBrake, priceVol),
            brakes
        };
    }

    if (priceVol.stress) {
        return {
            state: 'NOISE',
            mechanism: null,
            one_line: '价格回落 / 波动上升,但信用、实际利率、基本面均无异常;历史上多数此类回调属短期噪音,数周内收复居多。',
            confidence: priceVol.vix_elevated ? 'medium' : 'low',
            watch: watchLine(snapshot, realRateBrake, priceVol),
            brakes
        };
    }

    return {
        state: 'STABLE',
        mechanism: null,
        one_line: stableLine(brakes),
        confidence: confidenceFor(brakes, 'STABLE'),
        watch: watchLine(snapshot, realRateBrake, priceVol),
        brakes
    };
}

function creditBrakeStatus(
    creditRegimeState: CreditRegimeState,
    vixCross: boolean
): RegimeVerdictBrakeStatus {
    if (!vixCross) return 'quiet';
    if (creditRegimeState === 'BREAK') return 'confirmed';
    if (creditRegimeState === 'BREAK_FORMING') return 'forming';
    return 'quiet';
}

function fundamentalBrakeStatus(
    state: MacroRegimeSnapshot['fundamental_modifier']['state'],
    escalationLevel: number
): RegimeVerdictBrakeStatus {
    if (state === 'cracking' && escalationLevel >= 1) return 'confirmed';
    if (state === 'weakening') return 'forming';
    return 'quiet';
}

function verdictDrivingRatesStatus(realRateBrake: RealRateBrake): RegimeVerdictBrakeStatus {
    if (realRateBrake.status === 'confirmed') return 'confirmed';
    if (realRateBrake.status !== 'forming') return 'quiet';

    const delta = realRateBrake.delta_8w_bp ?? 0;
    const durationPressure =
        (realRateBrake.equity_drawdown_pct !== null &&
            realRateBrake.equity_drawdown_pct >= EQUITY_DRAWDOWN_DURATION_PRESSURE_PCT) ||
        realRateBrake.equity_below_ma50 === true;

    if (delta >= REAL_RATE_VERDICT_FORMING_DELTA_8W_BP) return 'forming';
    if (delta >= REAL_RATE_FORMING_DELTA_8W_BP && durationPressure) return 'forming';
    return 'quiet';
}

function computePriceVolStress(snapshot: MacroRegimeSnapshot, equityBars: DailyPriceBar[]): PriceVolStress {
    const equity = computeEquityState(equityBars);
    const vix = snapshot.indicators.VIX;
    const vixElevated =
        vix.status === 'Warning' ||
        vix.status === 'Critical' ||
        (typeof vix.value === 'number' && vix.value >= 25);
    const stress =
        vixElevated ||
        (equity.drawdown_pct !== null && equity.drawdown_pct >= EQUITY_DRAWDOWN_STRESS_PCT) ||
        equity.below_ma50 === true;
    return {
        stress,
        qqq_drawdown_pct: equity.drawdown_pct,
        qqq_below_ma50: equity.below_ma50,
        vix_elevated: vixElevated
    };
}

function computeEquityState(bars: DailyPriceBar[]): {
    drawdown_pct: number | null;
    below_ma50: boolean | null;
    below_ma200: boolean | null;
} {
    const sorted = [...bars]
        .filter((bar) => Number.isFinite(bar.close) && bar.close > 0)
        .sort((a, b) => a.date.localeCompare(b.date));
    if (sorted.length < 50) {
        return { drawdown_pct: null, below_ma50: null, below_ma200: null };
    }
    const closes = sorted.map((bar) => bar.close);
    const latest = closes[closes.length - 1];
    const high60 = Math.max(...closes.slice(-60));
    const drawdownPct = high60 > 0 ? Math.max(0, (1 - latest / high60) * 100) : null;
    const ma50 = average(closes.slice(-50));
    const ma200 = closes.length >= 200 ? average(closes.slice(-200)) : null;
    return {
        drawdown_pct: drawdownPct === null ? null : round1(drawdownPct),
        below_ma50: latest < ma50,
        below_ma200: ma200 === null ? null : latest < ma200
    };
}

function crowdingElevated(snapshot: MacroRegimeSnapshot): boolean {
    return (
        snapshot.indicators.CONCENTRATION.status === 'Warning' ||
        snapshot.indicators.CONCENTRATION.status === 'Critical' ||
        snapshot.indicators.SOX_200DMA_DEVIATION.status === 'Warning' ||
        snapshot.indicators.SOX_200DMA_DEVIATION.status === 'Critical' ||
        snapshot.indicators.AI_BREADTH.status === 'Warning' ||
        snapshot.indicators.AI_BREADTH.status === 'Critical'
    );
}

function firstMechanism(
    entries: Array<[Exclude<RegimeVerdictMechanism, null>, RegimeVerdictBrakeStatus]>,
    status: RegimeVerdictBrakeStatus
): Exclude<RegimeVerdictMechanism, null> | null {
    return entries.find(([, brakeStatus]) => brakeStatus === status)?.[0] ?? null;
}

function confidenceFor(brakes: RegimeVerdict['brakes'], state: RegimeVerdict['state']): RegimeVerdict['confidence'] {
    const confirmedCount = [brakes.credit, brakes.rates, brakes.fundamental].filter((status) => status === 'confirmed').length;
    const formingCount = [brakes.credit, brakes.rates, brakes.fundamental].filter((status) => status === 'forming').length;
    if (state === 'CONFIRMED_BREAK') return confirmedCount >= 2 || formingCount >= 1 ? 'high' : 'medium';
    if (state === 'BREAK_FORMING') return formingCount >= 2 || brakes.crowding === 'elevated' ? 'medium' : 'low';
    if (state === 'STABLE') {
        if (formingCount === 1 && confirmedCount === 0) return 'low';
        return brakes.crowding === 'elevated' ? 'medium' : 'high';
    }
    return 'medium';
}

function confirmedLine(
    mechanism: Exclude<RegimeVerdictMechanism, null>,
    snapshot: MacroRegimeSnapshot,
    realRateBrake: RealRateBrake
): string {
    if (mechanism === 'credit') {
        return '信用利差已确认走阔并加速,系统性信用压力成立(类 2008 / 2020)。';
    }
    if (mechanism === 'rates') {
        return `实际利率近 8 周上行 ${formatBp(realRateBrake.delta_8w_bp)},科技股已承压,久期重定价确立(类 2022)。`;
    }
    return '基本面已确认恶化:AI 资本开支与营收背离扩大,高估值面临压缩(类 2000)。';
}

function formingLine(
    mechanism: Exclude<RegimeVerdictMechanism, null>,
    snapshot: MacroRegimeSnapshot,
    realRateBrake: RealRateBrake
): string {
    if (mechanism === 'credit') {
        return '信用利差出现领先异动(最差档先走阔),价格可能尚未反映;系统性压力初现、待确认。';
    }
    if (mechanism === 'rates') {
        return `实际利率近 8 周上行 ${formatBp(realRateBrake.delta_8w_bp)},尚未传导到科技股;久期风险升温、待确认。`;
    }
    return '基本面边际走弱:AI 资本开支与营收差距扩大,尚未确认恶化。';
}

function stableLine(brakes: RegimeVerdict['brakes']): string {
    const stirring: string[] = [];
    if (brakes.credit === 'forming') stirring.push('信用');
    if (brakes.rates === 'forming') stirring.push('利率');
    if (brakes.fundamental === 'forming') stirring.push('基本面');
    const crowdElevated = brakes.crowding === 'elevated';
    if (stirring.length === 0 && !crowdElevated) {
        return '市场平稳,信用、利率、基本面机制均正常。';
    }
    const parts: string[] = [];
    if (stirring.length > 0) parts.push(`${stirring.join('、')}出现初步异动`);
    if (crowdElevated) parts.push('拥挤度偏高');
    return `市场整体平稳;${parts.join('、')},但均未确认,暂未对股市构成系统性风险。`;
}

function watchLine(
    snapshot: MacroRegimeSnapshot,
    realRateBrake: RealRateBrake,
    priceVol: PriceVolStress
): string {
    const credit = snapshot.credit_funding_stress;
    const creditState = credit.credit_regime_state ?? 'NOISE';
    const creditZh = creditState === 'BREAK' ? '走阔确认' : creditState === 'BREAK_FORMING' ? '领先异动' : '平稳';
    const pieces = [
        `信用利差 ${creditZh}`,
        `VIX ${snapshot.indicators.VIX.value ?? 'N/A'}`,
        `实际利率8周 ${formatBp(realRateBrake.delta_8w_bp)}`,
        `纳指距高点 ${priceVol.qqq_drawdown_pct !== null ? `-${priceVol.qqq_drawdown_pct.toFixed(1)}%` : 'N/A'}`
    ];
    return pieces.join(' · ');
}

function latestFredPoint(points: FredPoint[]): FredPoint | null {
    if (points.length === 0) return null;
    return [...points].sort((a, b) => a.date.localeCompare(b.date))[points.length - 1] ?? null;
}

function pointCalendarDaysBack(points: FredPoint[], daysBack: number): FredPoint | null {
    const sorted = [...points].sort((a, b) => a.date.localeCompare(b.date));
    const latest = sorted[sorted.length - 1];
    if (!latest) return null;
    const target = new Date(`${latest.date}T00:00:00Z`);
    target.setUTCDate(target.getUTCDate() - daysBack);
    const targetIso = target.toISOString().slice(0, 10);
    for (let i = sorted.length - 1; i >= 0; i -= 1) {
        if (sorted[i].date <= targetIso) return sorted[i];
    }
    return null;
}

function average(values: number[]): number {
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function round1(value: number): number {
    return Math.round(value * 10) / 10;
}

function round2(value: number): number {
    return Math.round(value * 100) / 100;
}

function signedBp(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(0)}bp`;
}

function formatBp(value: number | null): string {
    return value === null ? 'N/A' : signedBp(value);
}
