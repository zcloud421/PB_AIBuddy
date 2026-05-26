import type { OverallGrade } from '../../scoring-engine';

export type GateFailType =
    | 'HARD_FAIL'
    | 'TIMING_FAIL'
    | 'SUITABILITY_FAIL';

export type GateDecisionType =
    | 'OUTSIDE_UNIVERSE'
    | 'STATUS_BLOCK'
    | 'RESTRICTED'
    | 'BEARISH_STRUCTURE'
    | 'KI_BARRIER_RISK'
    | 'PATH_RISK'
    | 'EARNINGS_IMMINENT'
    | 'EARNINGS_WINDOW'
    | 'HARD_AVOID_TRIGGERED'
    | 'HIGH_COUPON_OVERRIDE'
    | 'QUALITY_DIP_RESCUE'
    | 'HIGH_VOL_CAUTION_OVERRIDE'
    | 'GRADE_CAP_COMMODITY_BETA'
    | 'GRADE_CAP_HIGH_BETA'
    | 'GRADE_CAP_NEWS_SHOCK'
    | 'GRADE_CAP_OVEREXTENDED'
    | 'GRADE_CAP_ASSIGNMENT_QUALITY';

export type GateSeverity = 'INFO' | 'WARN' | 'BLOCK';

export type FcnEngineMode = 'weighted' | 'gated_shadow' | 'gated_live';

export interface GateDecision {
    type: GateDecisionType;
    failType?: GateFailType;
    passed: boolean;
    severity: GateSeverity;
    message: string;
    details?: Record<string, unknown>;
    old_grade?: OverallGrade;
    new_grade?: OverallGrade;
    shadow?: boolean;
}

export interface GateDecisionSet {
    decisions: GateDecision[];
    final_grade: OverallGrade;
    engine_mode: FcnEngineMode;
}
