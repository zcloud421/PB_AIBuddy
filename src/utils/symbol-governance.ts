import fs from 'node:fs';
import path from 'node:path';

export interface RestrictedSymbolEntry {
    symbol: string;
    reason?: string;
    added_at?: string;
    reviewed_by?: string;
}

let restrictedSymbolsCache: Map<string, RestrictedSymbolEntry> | null = null;

export function getRestrictedSymbols(): Map<string, RestrictedSymbolEntry> {
    if (restrictedSymbolsCache) {
        return restrictedSymbolsCache;
    }

    const restrictedPath = path.join(__dirname, '../../data/restricted-symbols.json');
    try {
        const raw = fs.readFileSync(restrictedPath, 'utf8');
        const parsed = JSON.parse(raw) as RestrictedSymbolEntry[];
        restrictedSymbolsCache = new Map(
            (Array.isArray(parsed) ? parsed : [])
                .filter((entry) => typeof entry.symbol === 'string')
                .map((entry) => [entry.symbol.toUpperCase(), { ...entry, symbol: entry.symbol.toUpperCase() }])
        );
    } catch (error) {
        console.error('[symbol-governance] failed to load restricted-symbols.json', error);
        throw new Error('Restricted-symbol governance configuration is unavailable');
    }

    return restrictedSymbolsCache;
}

export function getRestrictedSymbol(symbol: string): RestrictedSymbolEntry | null {
    return getRestrictedSymbols().get(symbol.toUpperCase()) ?? null;
}

export function resetRestrictedSymbolsCacheForTests(): void {
    restrictedSymbolsCache = null;
}
