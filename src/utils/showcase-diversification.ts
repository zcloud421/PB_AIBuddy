export interface ShowcaseConcentrationCounts {
    subsector: number;
    cycle_family: number;
    theme: number;
    sector: number;
}

export interface ShowcaseConcentrationPenalty {
    total: number;
    subsector: number;
    cycle_family: number;
    theme: number;
    sector: number;
}

export function calculateShowcaseConcentrationPenalty(
    counts: ShowcaseConcentrationCounts
): ShowcaseConcentrationPenalty {
    const subsector = counts.subsector > 0 ? 0.16 : 0;
    const cycleFamily = counts.cycle_family > 0 ? 0.08 : 0;
    const theme = counts.theme > 0 ? 0.03 : 0;
    const sector = counts.sector >= 2 ? 0.12 : counts.sector === 1 ? 0.04 : 0;

    return {
        total: subsector + cycleFamily + theme + sector,
        subsector,
        cycle_family: cycleFamily,
        theme,
        sector
    };
}
