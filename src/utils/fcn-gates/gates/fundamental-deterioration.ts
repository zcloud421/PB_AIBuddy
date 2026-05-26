import type { GateDecision } from '../types';
import { decision, type GateInput } from './shared';

export function evaluateFundamentalDeteriorationGate(input: GateInput): GateDecision | null {
    if (!input.earningsMiss && !input.guideCut) return null;

    if (input.earningsMiss && input.guideCut) {
        return decision({
            type: 'FUNDAMENTAL_DETERIORATION',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: '财报不及预期且指引下调,基本面信号转弱',
            details: { earnings_miss: true, guide_cut: true }
        });
    }

    return decision({
        type: 'FUNDAMENTAL_DETERIORATION',
        failType: 'SUITABILITY_FAIL',
        severity: 'WARN',
        message: input.earningsMiss ? '财报不及预期,暂不适合 GO' : '指引下调,暂不适合 GO',
        details: { earnings_miss: Boolean(input.earningsMiss), guide_cut: Boolean(input.guideCut) }
    });
}
