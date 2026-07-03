import type { PitchInputs } from './llm-stitcher';

export function buildDealStructureSentence(p: PitchInputs): string {
    return `让您以 $${p.recommended_strike}（较现价低 ${p.discount_pct}%）承接 ${p.symbol}，年化票息 ${p.coupon_low}%-${p.coupon_high}%，期限 ${p.tenor_label}；若股价未跌破 $${p.recommended_strike}，您收取票息并赎回本金，若跌破则以 $${p.recommended_strike} 持有该标的。`;
}

export function buildHybridPitch(commReference: string, p: PitchInputs, _bridge = pickBridge(p.symbol)): string {
    return normalizeSentence(commReference);
}

export function buildDeterministicPitch(p: PitchInputs): string {
    return buildHybridPitch(buildTemplateWhySentence(p, true), p);
}

export function buildMinimalPitch(p: PitchInputs): string {
    return buildHybridPitch(buildTemplateWhySentence(p, false), p);
}

export function pickBridge(symbol: string): string {
    const BRIDGES = ['在这个背景下，', '对应到结构上，', '条款上，'] as const;
    const hash = Array.from(symbol).reduce((sum, char) => sum + char.charCodeAt(0), 0);
    return BRIDGES[hash % BRIDGES.length];
}

// Deterministic fallback (LLM 不可用时). 原则:有真数据就干净陈述事实,
// 没数据就短;不堆"显示经营兑现有具体支撑/有助于强化产业链位置"这类换皮废话,
// 也不引用英文新闻标题(只用中文 tag 催化)。
function buildTemplateWhySentence(p: PitchInputs, includeTags: boolean): string {
    // 3 个事实位:财务数据(收入/分部,unshift 到最前)优先占位,价格位置殿后。
    const facts = buildSpecificSignals(p).slice(0, 3);
    // 段③ 催化与 holding 补充取材自重叠的 tag 集,只取其一,避免「近期指引维持」+「管理层指引维持」这类重复。
    const closing = buildCatalystSignal(p) || (includeTags ? buildTagSupplements(p).slice(0, 1)[0] : undefined);

    // 段① 定位 + 硬数据 / 段② 前瞻窗口(对齐 FCN 期限,用户认可的措辞)/ 段③ 催化或护城河。
    // 不堆「显示经营兑现有具体支撑 / 主要看收入和订单节奏能否延续」这类换皮 so-what。
    const segments: string[] = [p.company_short_desc.replace(/[。.]$/, '')];
    if (facts.length > 0) segments.push(facts.join('、'));
    segments.push('未来 3-6 个月基本面相对稳健');
    if (closing) segments.push(closing);
    return `${segments.join('，')}。`;
}

function buildSpecificSignals(p: PitchInputs): string[] {
    const signals: string[] = [];

    if (typeof p.change_5d_pct === 'number' && Math.abs(p.change_5d_pct) >= 1) {
        const sign = p.change_5d_pct >= 0 ? '+' : '';
        signals.push(`近 5 日 ${sign}${p.change_5d_pct.toFixed(1)}%`);
    }

    if (typeof p.pct_from_52w_high === 'number') {
        const distance = Math.abs(p.pct_from_52w_high);
        if (distance < 5) {
            signals.push(`接近 52 周高点（距高 ${distance.toFixed(1)}%）`);
        } else if (distance <= 25) {
            signals.push(`距 52 周高点 ${distance.toFixed(1)}%`);
        }
    }

    if (typeof p.days_since_earnings === 'number' && p.days_since_earnings <= 14) {
        signals.push(`财报已于 ${p.days_since_earnings} 天前发布`);
    }

    if (p.earnings_surprise) {
        signals.push(`${p.earnings_surprise.period} EPS 超预期 ${p.earnings_surprise.eps_surprise_pct.toFixed(1)}%`);
    }

    if (typeof p.revenue_yoy_pct === 'number') {
        signals.unshift(`${p.financials_latest_quarter ?? '最近一季'} 收入同比 ${formatSignedPct(p.revenue_yoy_pct)}`);
    }

    if (p.top_segment) {
        signals.unshift(`${p.top_segment.name} 收入同比 ${formatSignedPct(p.top_segment.yoy_pct)}`);
    }

    if (typeof p.gross_margin_yoy_pp === 'number') {
        signals.push(`${p.financials_latest_quarter ?? '最近一季'} 毛利率同比 ${formatSignedNumber(p.gross_margin_yoy_pp)}pct`);
    }

    return signals;
}

// 兜底只用中文 tag 催化;英文新闻标题留给 LLM 路径用流畅中文转述,不在确定性兜底里直引。
function buildCatalystSignal(p: PitchInputs): string {
    if (p.lit_tags.holding.includes('index_inclusion')) return '近期纳入重要指数,机构可见度提升';
    if (p.lit_tags.holding.includes('guidance_reaffirmed_or_raised')) return '管理层指引维持或上调';
    if (p.lit_tags.holding.includes('infrastructure_capacity_cycle')) return '数据中心扩容周期延续';
    if (p.lit_tags.holding.includes('backlog')) return '订单积压提供收入能见度';
    return '';
}

function buildTagSupplements(p: PitchInputs): string[] {
    const tags: string[] = [];
    if (p.lit_tags.holding.includes('index_inclusion')) tags.push('近期指数纳入提升关注度');
    if (p.lit_tags.holding.includes('guidance_reaffirmed_or_raised')) tags.push('近期指引维持或上调');
    if (p.lit_tags.holding.includes('earnings_strong_beat')) tags.push('最近一期 EPS 明显超预期');
    if (p.lit_tags.holding.includes('earnings_modest_beat')) tags.push('最近一期 EPS 小幅超预期');
    if (p.lit_tags.holding.includes('infrastructure_capacity_cycle')) tags.push('数据中心基础设施扩容周期延续');
    if (p.lit_tags.holding.includes('guide_raise')) tags.push('财报指引强劲');
    if (p.lit_tags.holding.includes('super_cycle')) tags.push('所在行业处于上行周期');
    if (p.lit_tags.holding.includes('backlog')) tags.push('订单积压提供能见度');
    if (p.lit_tags.timing.includes('momentum_intact')) tags.push('趋势仍保持在关键均线上方');
    return tags;
}

function normalizeSentence(text: string): string {
    const trimmed = text.trim();
    if (!trimmed) return '';
    return /[。！？.!?]$/.test(trimmed) ? trimmed : `${trimmed}。`;
}

function formatSignedPct(value: number): string {
    return `${formatSignedNumber(value)}%`;
}

function formatSignedNumber(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}`;
}
