import type { OverallGrade } from '../../scoring-engine';
import type { MacroGateContext } from './macro-context';
import { DEFAULT_MACRO_CONTEXT } from './macro-context';
import type { GateDecision } from './types';
import { evaluateBearishStructureGate } from './gates/bearish-structure';
import { evaluateBufferFloorGate } from './gates/buffer-floor';
import { evaluateDistributionFallingKnifeGate } from './gates/distribution-falling-knife';
import { evaluateEarningsWindowGate } from './gates/earnings-window';
import { evaluateFundamentalDeteriorationGate } from './gates/fundamental-deterioration';
import { evaluatePathRiskGate } from './gates/path-risk';
import { decision, type GateEvaluationResult, type GateInput } from './gates/shared';

export const INFRASTRUCTURE_CAPACITY_TICKERS = new Set([
    'NVDA', 'AVGO', 'TSM', 'MU', 'LITE', 'VRT', 'ANET', 'CIEN', 'CRWV', 'NBIS', 'DELL', 'SMCI', 'MRVL'
]);

const macroUnavailableLogged = new Set<string>();

export function runAllGates(input: Omit<GateInput, 'macro'> & { macro?: MacroGateContext }): GateEvaluationResult {
    const macro = input.macro ?? DEFAULT_MACRO_CONTEXT;
    const gateInput: GateInput = { ...input, macro };
    const decisions: GateDecision[] = [];

    const orderedEvaluators = [
        evaluateBufferFloorGate,
        evaluateBearishStructureGate,
        evaluateEarningsWindowGate,
        evaluateFundamentalDeteriorationGate,
        evaluateDistributionFallingKnifeGate,
        evaluatePathRiskGate
    ];

    for (const evaluator of orderedEvaluators) {
        const result = evaluator(gateInput);
        if (result) {
            decisions.push(result);
            if (result.failType === 'HARD_FAIL') break;
        }
    }

    applyMacroOverrides(input.symbol, decisions, macro);
    return resolveGateGrade(decisions);
}

export function resolveGateGrade(decisions: GateDecision[]): GateEvaluationResult {
    const hardFails = decisions.filter((d) => d.failType === 'HARD_FAIL');
    const timingFails = decisions.filter((d) => d.failType === 'TIMING_FAIL');
    const suitabilityFails = decisions.filter((d) => d.failType === 'SUITABILITY_FAIL');

    if (hardFails.length > 0) return { decisions, grade: 'AVOID' };
    if (timingFails.length > 0) {
        return {
            decisions,
            grade: 'AVOID',
            wait_reason: 'WAIT_EARNINGS_RISK'
        };
    }
    if (suitabilityFails.length > 0) return { decisions, grade: 'CAUTION' };
    return { decisions, grade: 'GO' };
}

function applyMacroOverrides(symbol: string, decisions: GateDecision[], macro: MacroGateContext): void {
    const normalized = symbol.toUpperCase();
    const isAiCapex = INFRASTRUCTURE_CAPACITY_TICKERS.has(normalized);

    if (macro.snapshot_stale) {
        decisions.push(decision({
            type: 'MACRO_CONTEXT_UNAVAILABLE',
            passed: true,
            severity: 'WARN',
            message: 'Macro regime snapshot 缺失或过期,本次 gate 使用中性默认值',
            details: { as_of: macro.as_of }
        }));
        if (!macroUnavailableLogged.has(normalized)) {
            macroUnavailableLogged.add(normalized);
            console.warn(JSON.stringify({
                tag: 'macro_context_unavailable',
                symbol: normalized,
                as_of: macro.as_of,
                ts: new Date().toISOString()
            }));
        }
    }

    if (macro.credit_funding_status === 'crisis') {
        decisions.push(decision({
            type: 'MACRO_CREDIT_CRISIS',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: '信用 / 融资压力处于 crisis,全市场新增 FCN 风险过高',
            details: { credit_funding_status: macro.credit_funding_status, macro_overall: macro.overall }
        }));
        return;
    }

    if (isAiCapex && (macro.ai_cloud_status === 'Crisis' || macro.fundamental_state === 'cracking')) {
        decisions.push(decision({
            type: 'MACRO_AI_CAPEX_STRESS',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: 'AI capex 压力或基本面 cracking,AI 产业链标的暂不推进',
            details: {
                ai_cloud_status: macro.ai_cloud_status,
                fundamental_state: macro.fundamental_state,
                fundamental_escalation: macro.fundamental_escalation
            }
        }));
        return;
    }

    const bearishDecision = decisions.find((d) => d.type === 'BEARISH_STRUCTURE');
    if (
        macro.overall === 'Warning' &&
        (macro.credit_funding_status === 'stress' || macro.vix_status === 'Warning') &&
        bearishDecision &&
        bearishDecision.failType === 'SUITABILITY_FAIL'
    ) {
        bearishDecision.failType = 'HARD_FAIL';
        bearishDecision.severity = 'BLOCK';
        bearishDecision.message += ' (macro Warning + 信用/VIX 共振升级)';
        bearishDecision.details = {
            ...(bearishDecision.details ?? {}),
            macro_overall: macro.overall,
            credit_funding_status: macro.credit_funding_status,
            vix_status: macro.vix_status
        };
        return;
    }

    if (isAiCapex && (macro.ai_cloud_status === 'Stress' || macro.fundamental_escalation >= 1)) {
        decisions.push(decision({
            type: 'MACRO_AI_CAPEX_CAP',
            failType: 'SUITABILITY_FAIL',
            severity: 'WARN',
            message: 'AI capex 压力升温,AI 产业链标的 cap at CAUTION',
            details: {
                ai_cloud_status: macro.ai_cloud_status,
                fundamental_escalation: macro.fundamental_escalation
            }
        }));
    }

    if (macro.guardrail_applied && isAiCapex) {
        decisions.push(decision({
            type: 'MACRO_GUARDRAIL_AI_CAPEX_CAP',
            failType: 'SUITABILITY_FAIL',
            severity: 'WARN',
            message: 'Macro guardrail-derived Warning 下,AI capex 标的维持 CAUTION cap',
            details: { guardrail_applied: true, macro_overall: macro.overall, base_overall: macro.base_overall }
        }));
    }

    // The legacy nine-indicator aggregate can reach Critical on price/crowding alone.
    // A global suitability cap now requires a mechanism-confirmed break; BREAK_FORMING
    // remains shadow context and does not blanket-downgrade otherwise healthy names.
    if (macro.regime_verdict_state === 'CONFIRMED_BREAK') {
        decisions.push(decision({
            type: 'MACRO_REGIME_CRITICAL_CAP',
            failType: 'SUITABILITY_FAIL',
            severity: 'WARN',
            message: '宏观机制已确认断裂,全市场 GO cap at CAUTION',
            details: {
                macro_overall: macro.overall,
                base_overall: macro.base_overall,
                regime_verdict_state: macro.regime_verdict_state,
                regime_persistence_days: macro.regime_persistence_days
            }
        }));
    }
}

export function gradeToSymbol(grade: OverallGrade): number {
    if (grade === 'GO') return 2;
    if (grade === 'CAUTION') return 1;
    return 0;
}
