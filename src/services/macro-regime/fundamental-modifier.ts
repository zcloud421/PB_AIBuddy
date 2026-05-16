/**
 * Loader for the manually-maintained AI capex fundamental state file.
 *
 * The file lives at repo root: data/ai_capex_fundamental_state.json
 * Updated quarterly during AI capex review; not driven by automation.
 *
 * State mapping → escalation_level applied on top of base severity:
 *   - intact   (0): no modifier
 *   - weakening (1): bump overall up one ladder step
 *   - cracking  (2): force to Critical
 *
 * If file is missing or malformed, returns a safe "intact" default and logs a
 * warning. Snapshot generation never fails on this layer.
 */

import fs from 'fs';
import path from 'path';

import type { FundamentalModifier, FundamentalState } from './types';

const DEFAULT_MODIFIER: FundamentalModifier = {
    state: 'intact',
    escalation_level: 0,
    review_quarter: 'unknown',
    next_review_date: 'unknown',
    evidence_summary: []
};

function resolveModifierPath(): string {
    // Repo root has data/ — same dir layout as deploy uses.
    return path.resolve(__dirname, '../../../data/ai_capex_fundamental_state.json');
}

function parseEscalationLevel(state: unknown, raw: unknown): 0 | 1 | 2 {
    if (typeof raw === 'number') {
        if (raw === 0 || raw === 1 || raw === 2) return raw;
    }
    if (state === 'intact') return 0;
    if (state === 'weakening') return 1;
    if (state === 'cracking') return 2;
    return 0;
}

function parseState(raw: unknown): FundamentalState {
    if (raw === 'intact' || raw === 'weakening' || raw === 'cracking') return raw;
    return 'intact';
}

export function loadFundamentalModifier(): FundamentalModifier {
    const filePath = resolveModifierPath();
    let raw: string;
    try {
        raw = fs.readFileSync(filePath, 'utf-8');
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[macro-regime] fundamental-modifier file missing or unreadable: ${message}`);
        return DEFAULT_MODIFIER;
    }

    let parsed: Record<string, unknown>;
    try {
        parsed = JSON.parse(raw) as Record<string, unknown>;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[macro-regime] fundamental-modifier file invalid JSON: ${message}`);
        return DEFAULT_MODIFIER;
    }

    const state = parseState(parsed.state);
    const escalation = parseEscalationLevel(state, parsed.escalation_level);
    const evidence = Array.isArray(parsed.evidence_summary)
        ? parsed.evidence_summary.filter((line): line is string => typeof line === 'string')
        : [];

    return {
        state,
        escalation_level: escalation,
        review_quarter: typeof parsed.review_quarter === 'string' ? parsed.review_quarter : 'unknown',
        next_review_date:
            typeof parsed.next_review_date === 'string' ? parsed.next_review_date : 'unknown',
        evidence_summary: evidence
    };
}
