import type { NarrativeSourceQuality } from '../narrative-generator';

export const PITCH_ENGINE_VERSION = '2026-07-03-whyfcn-v7';

const CURRENT_PITCH_SOURCE_QUALITIES = new Set<NarrativeSourceQuality>([
    'go_pitch_hybrid_validated',
    'go_pitch_template',
    'go_pitch_minimal',
    'caution_pitch_hybrid_validated',
    'caution_pitch_template',
    'avoid_pitch_deterministic'
]);

export function isCurrentPitchSourceQuality(sourceQuality: NarrativeSourceQuality | null | undefined): boolean {
    return Boolean(sourceQuality && CURRENT_PITCH_SOURCE_QUALITIES.has(sourceQuality));
}

export function narrativeSourceQualityPriority(sourceQuality: NarrativeSourceQuality | null | undefined): number {
    switch (sourceQuality) {
        case 'go_pitch_hybrid_validated':
        case 'caution_pitch_hybrid_validated':
            return 60;
        case 'go_pitch_template':
        case 'caution_pitch_template':
            return 50;
        case 'go_pitch_minimal':
            return 45;
        case 'avoid_pitch_deterministic':
            return 40;
        case 'deterministic':
            return 35;
        case 'llm_validated':
        case 'llm_retry_validated':
        case 'go_pitch_llm_validated':
            return 30;
        case 'template_fallback':
        case 'llm_failed_validation':
            return 20;
        case 'blocked':
            return 0;
        default:
            return 0;
    }
}

export function hasCompanyIntroPrepend(whyNow: string | null | undefined): boolean {
    if (!whyNow) return false;
    const firstSentence = whyNow.split(/[。！？.!?]/)[0] ?? '';
    return firstSentence.includes('是');
}

export function getPitchNarrativeStaleReason(input: {
    source_quality: NarrativeSourceQuality | null | undefined;
    why_now: string | null | undefined;
    engine_version?: string | null | undefined;
}): string | null {
    if (!isCurrentPitchSourceQuality(input.source_quality)) {
        return `stale_source_quality:${input.source_quality ?? 'null'}`;
    }
    if (input.engine_version !== PITCH_ENGINE_VERSION) {
        return `stale_engine_version:${input.engine_version ?? 'null'}->${PITCH_ENGINE_VERSION}`;
    }
    if (!hasCompanyIntroPrepend(input.why_now)) {
        return `missing_company_intro:${PITCH_ENGINE_VERSION}`;
    }
    // GO 的降级产物(minimal/template)多因生成时财务/价格数据临时缺失(如 FMP 配额、
    // 限流)。视为 stale,下次读取/screener 时重试,直到升级成 hybrid_validated,
    // 避免空壳 pitch 被缓存后永久停留。
    if (input.source_quality === 'go_pitch_minimal' || input.source_quality === 'go_pitch_template') {
        return `degraded_pitch_retry:${input.source_quality}`;
    }
    return null;
}
