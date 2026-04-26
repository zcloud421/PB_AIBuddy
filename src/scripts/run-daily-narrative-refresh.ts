import dotenv from 'dotenv';

dotenv.config();

function resolveFocusRefreshBaseUrl() {
    return (
        process.env.FOCUS_API_REFRESH_BASE_URL?.trim() ??
        process.env.API_BASE_URL?.trim() ??
        process.env.PUBLIC_API_BASE_URL?.trim() ??
        'https://backend-production-02fa.up.railway.app'
    );
}

async function main(): Promise<void> {
    const setupToken = process.env.SETUP_TOKEN?.trim();
    if (!setupToken) {
        throw new Error('Missing SETUP_TOKEN for daily narrative refresh');
    }

    const baseUrl = resolveFocusRefreshBaseUrl();
    console.log(`[focus-refresh] narrative refresh started (${baseUrl})`);

    const response = await fetch(`${baseUrl}/ideas/focus/daily-narrative/refresh`, {
        method: 'POST',
        headers: {
            'x-setup-token': setupToken
        }
    });

    if (!response.ok) {
        const body = await response.text();
        throw new Error(`refresh endpoint ${response.status}: ${body || 'empty body'}`);
    }

    const payload = (await response.json()) as {
        generated_at?: string;
        primary_slug?: string;
        daily_pitch_triggers?: Array<{ hook?: string }>;
        asset_buckets?: Array<{ bucket?: string }>;
    } | null;

    const pitchSummary = Array.isArray(payload?.daily_pitch_triggers)
        ? payload.daily_pitch_triggers
              .map((item) => item.hook)
              .filter((hook): hook is string => typeof hook === 'string' && hook.length > 0)
              .join(', ')
        : '';

    console.log(
        `[focus-refresh] narrative refreshed (${payload?.primary_slug ?? 'unknown'} / ${pitchSummary || 'no pitch cards'} / ${payload?.generated_at ?? 'no generated_at'})`
    );
}

main()
    .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(message);
        process.exitCode = 1;
    })
    .finally(() => {
        process.exit(process.exitCode ?? 0);
    });
