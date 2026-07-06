import type { DailyPriceBar } from '../../data/massive-fetcher';
import type { FredPoint } from '../../data/fred-series-fetcher';
import type {
    CreditRegimeState,
    MacroRegimeSnapshot,
    RegimeVerdict,
    RegimeVerdictBrakeStatus,
    RegimeVerdictContextStatus,
    RegimeVerdictEvidence,
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

// Faithful-to-spec rates/credit context that lives outside snapshot.indicators:
// 30Y nominal (long-end / term-premium) and HYG/IEF (credit vs duration).
export interface VerdictExtras {
    dgs30_pct?: number | null;
    dgs30_delta_8w_bp?: number | null;
    hyg_ief_ratio?: number | null;
    hyg_ief_delta_4w_pct?: number | null;
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
    const durationPressure =
        (equity.drawdown_pct !== null && equity.drawdown_pct >= EQUITY_DRAWDOWN_DURATION_PRESSURE_PCT) ||
        equity.below_ma50 === true;

    if (delta8wBp >= REAL_RATE_CONFIRMED_DELTA_8W_BP && equityStress) {
        status = 'confirmed';
        notes.push('实际利率 8周重定价 + 权益承压,利率/久期刹车确认');
    } else if (
        delta8wBp >= REAL_RATE_VERDICT_FORMING_DELTA_8W_BP ||
        (delta8wBp >= REAL_RATE_FORMING_DELTA_8W_BP && durationPressure)
    ) {
        status = 'forming';
        notes.push('实际利率上行进入风险累积区,等待进一步确认');
    } else if (delta8wBp >= REAL_RATE_FORMING_DELTA_8W_BP) {
        status = 'watch';
        notes.push('实际利率进入观察区,但 QQQ 尚未承压');
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
    equityBars: DailyPriceBar[] = [],
    extras: VerdictExtras = {}
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
    const context = buildContext(snapshot);
    const crowdingStatus = context.status === 'normal' ? 'quiet' : 'elevated';

    const brakes = {
        credit: creditStatus,
        rates: ratesStatus,
        fundamental: fundamentalStatus,
        crowding: crowdingStatus
    } as RegimeVerdict['brakes'];
    const mechanisms = buildMechanismViews(snapshot, realRateBrake, priceVol, {
        credit: creditStatus,
        rates: ratesStatus,
        fundamental: fundamentalStatus
    }, extras);
    const nearestWatch = buildNearestWatch(mechanisms);

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
            nearest_watch: nearestWatch,
            mechanisms,
            context,
            watch: nearestWatch ?? '信用、利率、基本面暂无临近触发项。',
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
            nearest_watch: nearestWatch,
            mechanisms,
            context,
            watch: nearestWatch ?? '信用、利率、基本面暂无临近触发项。',
            brakes
        };
    }

    if (priceVol.stress) {
        return {
            state: 'NOISE',
            mechanism: null,
            one_line: '价格回落 / 波动上升,但信用、实际利率、基本面均无异常;历史上此类技术性回调多在数周内收复。',
            confidence: priceVol.vix_elevated ? 'medium' : 'low',
            nearest_watch: nearestWatch,
            mechanisms,
            context,
            watch: nearestWatch ?? '信用、利率、基本面暂无临近触发项。',
            brakes
        };
    }

    return {
        state: 'STABLE',
        mechanism: null,
        one_line: stableLine(brakes),
        confidence: confidenceFor(brakes, 'STABLE'),
        nearest_watch: nearestWatch,
        mechanisms,
        context,
        watch: nearestWatch ?? '信用、利率、基本面暂无临近触发项。',
        brakes
    };
}

function creditBrakeStatus(
    creditRegimeState: CreditRegimeState,
    vixCross: boolean
): RegimeVerdictBrakeStatus {
    if (!vixCross) {
        return creditRegimeState === 'BREAK' || creditRegimeState === 'BREAK_FORMING' ? 'watch' : 'quiet';
    }
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
    if (realRateBrake.status === 'forming') return 'forming';
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

function firstMechanism(
    entries: Array<[Exclude<RegimeVerdictMechanism, null>, RegimeVerdictBrakeStatus]>,
    status: RegimeVerdictBrakeStatus
): Exclude<RegimeVerdictMechanism, null> | null {
    return entries.find(([, brakeStatus]) => brakeStatus === status)?.[0] ?? null;
}

function confidenceFor(brakes: RegimeVerdict['brakes'], state: RegimeVerdict['state']): RegimeVerdict['confidence'] {
    const confirmedCount = [brakes.credit, brakes.rates, brakes.fundamental].filter((status) => status === 'confirmed').length;
    const formingCount = [brakes.credit, brakes.rates, brakes.fundamental].filter((status) => status === 'forming').length;
    const watchCount = [brakes.credit, brakes.rates, brakes.fundamental].filter((status) => status === 'watch').length;
    if (state === 'CONFIRMED_BREAK') return confirmedCount >= 2 || formingCount >= 1 ? 'high' : 'medium';
    if (state === 'BREAK_FORMING') return formingCount >= 2 || brakes.crowding === 'elevated' ? 'medium' : 'low';
    if (state === 'STABLE') {
        if (watchCount > 0 || (formingCount === 1 && confirmedCount === 0)) return 'low';
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
    if (brakes.credit === 'watch' || brakes.credit === 'forming') stirring.push('信用');
    if (brakes.rates === 'watch' || brakes.rates === 'forming') stirring.push('利率');
    if (brakes.fundamental === 'watch' || brakes.fundamental === 'forming') stirring.push('基本面');
    const crowdElevated = brakes.crowding === 'elevated';
    if (stirring.length === 0 && !crowdElevated) {
        return '市场平稳,信用、利率、基本面机制均正常。';
    }
    const parts: string[] = [];
    if (stirring.length > 0) parts.push(`${stirring.join('、')}出现初步异动`);
    if (crowdElevated) parts.push('拥挤度偏高');
    return `市场整体平稳;${parts.join('、')},但均未确认,暂未对股市构成系统性风险。`;
}

function buildMechanismViews(
    snapshot: MacroRegimeSnapshot,
    realRateBrake: RealRateBrake,
    priceVol: PriceVolStress,
    statuses: {
        credit: RegimeVerdictBrakeStatus;
        rates: RegimeVerdictBrakeStatus;
        fundamental: RegimeVerdictBrakeStatus;
    },
    extras: VerdictExtras = {}
): RegimeVerdict['mechanisms'] {
    const credit = snapshot.credit_funding_stress;
    return {
        credit: {
            status: statuses.credit,
            evidence: [
                { label: 'HY利差', value: formatBpValue(snapshot.indicators.HY_OAS.value) },
                { label: 'HYG/IEF', value: formatHygIef(extras.hyg_ief_delta_4w_pct) },
                { label: 'CCC领先', value: formatBp(credit.ccc_leads_hy_signal?.value ?? null) },
                { label: 'VIX交叉', value: priceVol.vix_elevated ? '是' : '否' },
                { label: '股信背离', value: (credit.credit_equity_divergence_signal?.score ?? 0) >= 2 ? '出现' : '无' }
            ],
            next_trigger: statuses.credit === 'confirmed'
                ? null
                : 'CCC 持续领先 HY 且第二信号相互印证（VIX 交叉 / 背离 / HYG/IEF 走弱）→ 升级为风险累积'
        },
        rates: {
            status: statuses.rates,
            evidence: [
                { label: '实际利率8周', value: formatBp(realRateBrake.delta_8w_bp) },
                { label: '10Y', value: formatPct(snapshot.indicators.DGS10_ABS_LEVEL.value) },
                { label: '30Y', value: formatPct(extras.dgs30_pct ?? null) }
            ],
            next_trigger: statuses.rates === 'confirmed'
                ? null
                : '升至 +40bp，或 +25bp 且 QQQ 回撤 ≥3% → 升级为风险累积'
        },
        fundamental: {
            status: statuses.fundamental,
            evidence: [
                { label: 'capex指引', value: capexGuidanceValue(snapshot) },
                { label: '营收背离', value: fundamentalStateValue(snapshot) },
                { label: 'SOX偏离', value: formatSignedPct(snapshot.indicators.SOX_200DMA_DEVIATION.value) }
            ],
            next_trigger: statuses.fundamental === 'confirmed'
                ? null
                : 'capex 指引下调或营收-capex 背离扩大 → 升级为风险累积'
        }
    };
}

function buildNearestWatch(mechanisms: RegimeVerdict['mechanisms']): string | null {
    const ordered: Array<keyof RegimeVerdict['mechanisms']> = ['credit', 'rates', 'fundamental'];
    const priority: Record<RegimeVerdictBrakeStatus, number> = {
        quiet: 0,
        watch: 1,
        forming: 2,
        confirmed: 3
    };
    const selected = ordered
        .map((key) => ({ key, view: mechanisms[key] }))
        .filter((item) => item.view.status !== 'quiet')
        .sort((a, b) => priority[b.view.status] - priority[a.view.status])[0];
    if (!selected) return null;

    if (selected.key === 'rates') {
        const delta = mechanisms.rates.evidence.find((item) => item.label === '实际利率8周')?.value ?? '—';
        const qqq = mechanisms.rates.evidence.find((item) => item.label === 'QQQ距高')?.value ?? '—';
        return `实际利率 8周 ${delta}，QQQ 距高 ${qqq};若升至 +40bp 或 QQQ 回撤 ≥3%,利率机制升级为风险累积。`;
    }
    if (selected.key === 'credit') {
        const ccc = mechanisms.credit.evidence.find((item) => item.label === 'CCC领先')?.value ?? '—';
        const vixCross = mechanisms.credit.evidence.find((item) => item.label === 'VIX交叉')?.value ?? '—';
        return `信用观察:CCC 领先 ${ccc}，VIX交叉 ${vixCross};若 VIX 交叉或股信背离相互印证,信用机制升级为风险累积。`;
    }
    return '基本面观察:若 capex 指引下调或营收-capex 背离扩大,基本面机制升级为风险累积。';
}

function buildContext(snapshot: MacroRegimeSnapshot): RegimeVerdict['context'] {
    const evidence: RegimeVerdictEvidence[] = [
        { label: '集中度', value: formatPct(snapshot.indicators.CONCENTRATION.value) },
        { label: 'SOX偏离', value: formatSignedPct(snapshot.indicators.SOX_200DMA_DEVIATION.value) },
        { label: '宽度', value: formatPct(snapshot.indicators.BROAD_BREADTH.value) },
        { label: 'F&G', value: fearGreedValue(snapshot) }
    ];
    const statuses = [
        snapshot.indicators.CONCENTRATION.status,
        snapshot.indicators.SOX_200DMA_DEVIATION.status,
        snapshot.indicators.BROAD_BREADTH.status,
        fearGreedContextSeverity(snapshot)
    ];
    const status: RegimeVerdictContextStatus = statuses.includes('Critical')
        ? 'extreme'
        : statuses.includes('Warning')
            ? 'elevated'
            : 'normal';
    return { status, evidence };
}

function formatBpValue(value: number | null): string {
    return value === null ? '—' : `${value.toFixed(0)}bp`;
}

function formatPct(value: number | null): string {
    return value === null ? '—' : `${value.toFixed(1)}%`;
}

function formatSignedPct(value: number | null): string {
    return value === null ? '—' : `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

// HYG/IEF: the 4w ratio trend is the signal — falling = high-yield underperforming
// duration = credit stress. Show the signed 4w change.
function formatHygIef(delta4wPct: number | null | undefined): string {
    if (delta4wPct === null || delta4wPct === undefined || !Number.isFinite(delta4wPct)) return '—';
    return `4周 ${delta4wPct >= 0 ? '+' : ''}${delta4wPct.toFixed(1)}%`;
}

function capexGuidanceValue(snapshot: MacroRegimeSnapshot): string {
    const summary = snapshot.fundamental_modifier.evidence_summary.join(' ');
    const match = summary.match(/(\d+)\s*\/\s*(\d+)/);
    if (match) return `${match[1]}/${match[2]}上调或维持`;
    return snapshot.fundamental_modifier.state === 'intact' ? '维持' : snapshot.fundamental_modifier.state;
}

function fundamentalStateValue(snapshot: MacroRegimeSnapshot): string {
    if (snapshot.fundamental_modifier.state === 'intact') return '未扩大';
    if (snapshot.fundamental_modifier.state === 'weakening') return '观察';
    return '扩大';
}

function fearGreedValue(snapshot: MacroRegimeSnapshot): string {
    const summary = snapshot.late_cycle_context.pillars.sentiment_manual.summary;
    const match = summary.match(/F&G\s+(\d+(?:\.\d+)?)(?:\s*·\s*(.+))?/);
    if (!match) return '—';
    const label = match[2]?.trim();
    return label ? `${match[1]} ${label}` : match[1];
}

function fearGreedContextSeverity(snapshot: MacroRegimeSnapshot): 'Healthy' | 'Neutral' | 'Warning' | 'Critical' {
    const pillar = snapshot.late_cycle_context.pillars.sentiment_manual;
    if (pillar.state === 'extreme') return 'Critical';
    if (pillar.state === 'elevated') return 'Warning';
    return 'Neutral';
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
