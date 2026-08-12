export type HealthTelegramChannel = 'attribution' | 'gate_distribution' | 'narrative';

const TELEGRAM_ENABLE_ENV: Record<HealthTelegramChannel, string> = {
    attribution: 'ATTRIBUTION_HEALTH_TELEGRAM_ENABLED',
    gate_distribution: 'GATE_DISTRIBUTION_TELEGRAM_ENABLED',
    narrative: 'NARRATIVE_HEALTH_TELEGRAM_ENABLED'
};

export function isHealthTelegramEnabled(
    channel: HealthTelegramChannel,
    env: NodeJS.ProcessEnv = process.env
): boolean {
    return env[TELEGRAM_ENABLE_ENV[channel]]?.trim().toLowerCase() === 'true';
}

export function getHealthTelegramEnableEnv(channel: HealthTelegramChannel): string {
    return TELEGRAM_ENABLE_ENV[channel];
}
