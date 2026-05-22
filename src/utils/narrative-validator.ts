import type { NarrativeInput } from './narrative-generator';

export interface NumericFact {
    value: number;
    unit: '%' | '$' | 'bp' | 'pp' | 'x';
    tolerance: number;
    displayFormats: string[];
    source: string;
}

interface ExtractedNumber {
    value: number;
    unit: string;
    raw: string;
}

export interface ValidationResult {
    passed: boolean;
    unauthorized: Array<{ raw: string; value: number; unit: string }>;
    authorizedCount: number;
}

const NUMBER_TOKEN_REGEX = /(\d+(?:\.\d+)?)\s*(%|bp|pp|倍|x)/gi;
const DOLLAR_PREFIX_REGEX = /\$\s*(\d+(?:\.\d+)?)/g;

export function buildAllowedFacts(input: NarrativeInput): NumericFact[] {
    const facts: NumericFact[] = [];

    if (typeof input.current_price === 'number' && input.current_price > 0) {
        facts.push({
            value: input.current_price,
            unit: '$',
            tolerance: 0.5,
            displayFormats: [
                `${input.current_price}`,
                `${input.current_price.toFixed(0)}`,
                `${input.current_price.toFixed(2)}`
            ],
            source: 'current_price'
        });
    }

    if (typeof input.recommended_strike === 'number' && input.recommended_strike > 0) {
        facts.push({
            value: input.recommended_strike,
            unit: '$',
            tolerance: 0.5,
            displayFormats: [
                `${input.recommended_strike}`,
                `${input.recommended_strike.toFixed(0)}`,
                `${input.recommended_strike.toFixed(2)}`
            ],
            source: 'recommended_strike'
        });

        if (input.current_price && input.current_price > 0) {
            const moneyness = (input.recommended_strike / input.current_price) * 100;
            facts.push({
                value: moneyness,
                unit: '%',
                tolerance: 1,
                displayFormats: [`${Math.round(moneyness)}`, `${moneyness.toFixed(0)}`],
                source: 'moneyness'
            });

            const protection = 100 - moneyness;
            facts.push({
                value: protection,
                unit: '%',
                tolerance: 1,
                displayFormats: [`${Math.round(protection)}`, `${protection.toFixed(0)}`],
                source: 'downside_protection'
            });
        }
    }

    if (typeof input.pct_from_52w_high === 'number') {
        const absHigh = Math.abs(input.pct_from_52w_high);
        facts.push({
            value: absHigh,
            unit: '%',
            tolerance: 0.5,
            displayFormats: [
                `${absHigh}`,
                `${absHigh.toFixed(1)}`,
                `${Math.round(absHigh)}`,
                `-${absHigh}`,
                `-${absHigh.toFixed(1)}`
            ],
            source: 'pct_from_52w_high'
        });
    }

    if (typeof input.change_1d_pct === 'number') {
        const abs1d = Math.abs(input.change_1d_pct);
        facts.push({
            value: abs1d,
            unit: '%',
            tolerance: 0.5,
            displayFormats: [
                `${abs1d}`,
                `${abs1d.toFixed(2)}`,
                `${abs1d.toFixed(1)}`,
                `${Math.round(abs1d)}`
            ],
            source: 'change_1d_pct'
        });
    }

    if (typeof input.change_5d_pct === 'number') {
        const abs5d = Math.abs(input.change_5d_pct);
        facts.push({
            value: abs5d,
            unit: '%',
            tolerance: 0.5,
            displayFormats: [
                `${abs5d}`,
                `${abs5d.toFixed(2)}`,
                `${abs5d.toFixed(1)}`,
                `${Math.round(abs5d)}`
            ],
            source: 'change_5d_pct'
        });
    }

    for (const [index, coupon] of parseCouponRange(input.estimated_coupon_range).entries()) {
        facts.push({
            value: coupon,
            unit: '%',
            tolerance: 0.5,
            displayFormats: [`${coupon}`, `${Math.round(coupon)}`],
            source: index === 0 ? 'coupon_range_low' : 'coupon_range_high'
        });
    }

    return facts;
}

export function validateNarrativeNumbers(
    text: string,
    input: NarrativeInput
): ValidationResult {
    const facts = buildAllowedFacts(input);
    const numbers = extractNumbers(text);

    const unauthorized: ValidationResult['unauthorized'] = [];
    let authorizedCount = 0;

    for (const num of numbers) {
        if (isAuthorized(num, facts)) {
            authorizedCount += 1;
        } else {
            unauthorized.push({ raw: num.raw, value: num.value, unit: num.unit });
        }
    }

    return {
        passed: unauthorized.length === 0,
        unauthorized,
        authorizedCount
    };
}

export function parseCouponRange(range: string | null | undefined): number[] {
    if (!range) {
        return [];
    }

    return Array.from(range.matchAll(/(\d+(?:\.\d+)?)\s*%/g))
        .map((match) => Number(match[1]))
        .filter((value) => Number.isFinite(value));
}

function extractNumbers(text: string): ExtractedNumber[] {
    const found: ExtractedNumber[] = [];

    let match: RegExpExecArray | null;
    while ((match = NUMBER_TOKEN_REGEX.exec(text)) !== null) {
        found.push({
            value: Number.parseFloat(match[1]),
            unit: normalizeUnit(match[2]),
            raw: match[0]
        });
    }

    while ((match = DOLLAR_PREFIX_REGEX.exec(text)) !== null) {
        found.push({
            value: Number.parseFloat(match[1]),
            unit: '$',
            raw: match[0]
        });
    }

    return found;
}

function isAuthorized(num: ExtractedNumber, facts: NumericFact[]): NumericFact | null {
    for (const fact of facts) {
        if (fact.unit.toLowerCase() !== num.unit.toLowerCase()) {
            continue;
        }

        if (Math.abs(fact.value - num.value) <= fact.tolerance) {
            return fact;
        }
    }

    return null;
}

function normalizeUnit(unit: string): string {
    const normalized = unit.toLowerCase();
    return normalized === '倍' ? 'x' : normalized;
}
