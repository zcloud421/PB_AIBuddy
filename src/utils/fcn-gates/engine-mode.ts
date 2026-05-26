import type { FcnEngineMode } from './types';

const VALID_ENGINE_MODES: FcnEngineMode[] = ['weighted', 'gated_shadow', 'gated_live'];

export function getEngineMode(): FcnEngineMode {
    const raw = process.env.FCN_ENGINE_MODE?.trim() || 'weighted';
    if (VALID_ENGINE_MODES.includes(raw as FcnEngineMode)) {
        return raw as FcnEngineMode;
    }
    console.warn(`[fcn-gates] unknown FCN_ENGINE_MODE=${raw}, falling back to weighted`);
    return 'weighted';
}

export const ENGINE_MODE = getEngineMode();

export function isShadowMode(): boolean {
    return getEngineMode() === 'gated_shadow';
}

export function isGatedLive(): boolean {
    return getEngineMode() === 'gated_live';
}
