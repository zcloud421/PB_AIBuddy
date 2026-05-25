import assert from 'node:assert/strict';
import {
    getPitchNarrativeStaleReason,
    hasCompanyIntroPrepend,
    isCurrentPitchSourceQuality,
    narrativeSourceQualityPriority
} from './pitch-engine-version';

assert.equal(isCurrentPitchSourceQuality('go_pitch_hybrid_validated'), true);
assert.equal(isCurrentPitchSourceQuality('caution_pitch_template'), true);
assert.equal(isCurrentPitchSourceQuality('avoid_pitch_deterministic'), true);
assert.equal(isCurrentPitchSourceQuality('llm_validated'), false);
assert.equal(isCurrentPitchSourceQuality(null), false);
assert.ok(narrativeSourceQualityPriority('go_pitch_hybrid_validated') > narrativeSourceQualityPriority('go_pitch_template'));
assert.ok(narrativeSourceQualityPriority('go_pitch_template') > narrativeSourceQualityPriority('go_pitch_minimal'));
assert.ok(narrativeSourceQualityPriority('go_pitch_minimal') > narrativeSourceQualityPriority('deterministic'));
assert.ok(narrativeSourceQualityPriority('deterministic') > narrativeSourceQualityPriority('template_fallback'));
assert.ok(narrativeSourceQualityPriority('template_fallback') > narrativeSourceQualityPriority('blocked'));

assert.equal(hasCompanyIntroPrepend('NVIDIA 是 AI 算力 GPU 全球龙头供应商。条款上...'), true);
assert.equal(hasCompanyIntroPrepend('订单可见度较高，当前结构可看。'), false);

assert.equal(
    getPitchNarrativeStaleReason({
        source_quality: 'go_pitch_hybrid_validated',
        why_now: 'NVIDIA 是 AI 算力 GPU 全球龙头供应商。条款上...'
    }),
    null
);
assert.match(
    getPitchNarrativeStaleReason({
        source_quality: 'template_fallback',
        why_now: 'NVIDIA 是 AI 算力 GPU 全球龙头供应商。'
    }) ?? '',
    /^stale_source_quality:template_fallback/
);
assert.match(
    getPitchNarrativeStaleReason({
        source_quality: 'caution_pitch_template',
        why_now: '订单可见度较高，当前结构可看。'
    }) ?? '',
    /^missing_company_intro:/
);

console.log('pitch-engine-version tests passed');
