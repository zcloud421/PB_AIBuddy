import { pool } from '../client';

export async function ensureDeviceTables(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS device_favorites (
            device_id TEXT NOT NULL,
            symbol    TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (device_id, symbol)
        );

        CREATE TABLE IF NOT EXISTS device_tokens (
            device_id  TEXT PRIMARY KEY,
            push_token TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    `);
}

export async function getDeviceFavorites(deviceId: string): Promise<string[]> {
    const result = await pool.query<{ symbol: string }>(
        `SELECT symbol FROM device_favorites WHERE device_id = $1 ORDER BY created_at DESC`,
        [deviceId],
    );
    return result.rows.map((row) => row.symbol);
}

export async function addDeviceFavorite(deviceId: string, symbol: string): Promise<void> {
    await pool.query(
        `INSERT INTO device_favorites (device_id, symbol) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        [deviceId, symbol.toUpperCase()],
    );
}

export async function removeDeviceFavorite(deviceId: string, symbol: string): Promise<void> {
    await pool.query(
        `DELETE FROM device_favorites WHERE device_id = $1 AND symbol = $2`,
        [deviceId, symbol.toUpperCase()],
    );
}

export async function bulkAddDeviceFavorites(deviceId: string, symbols: string[]): Promise<void> {
    if (symbols.length === 0) {
        return;
    }
    const values = symbols.map((s, i) => `($1, $${i + 2})`).join(', ');
    await pool.query(
        `INSERT INTO device_favorites (device_id, symbol) VALUES ${values} ON CONFLICT DO NOTHING`,
        [deviceId, ...symbols.map((s) => s.toUpperCase())],
    );
}

export async function upsertDeviceToken(deviceId: string, pushToken: string): Promise<void> {
    await pool.query(
        `INSERT INTO device_tokens (device_id, push_token, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (device_id) DO UPDATE SET push_token = EXCLUDED.push_token, updated_at = NOW()`,
        [deviceId, pushToken],
    );
}

export async function getTokensForFavoritedSymbols(
    symbols: string[],
): Promise<Array<{ push_token: string; symbol: string }>> {
    if (symbols.length === 0) {
        return [];
    }
    const placeholders = symbols.map((_, i) => `$${i + 1}`).join(', ');
    const result = await pool.query<{ push_token: string; symbol: string }>(
        `SELECT dt.push_token, df.symbol
         FROM device_favorites df
         JOIN device_tokens dt ON dt.device_id = df.device_id
         WHERE df.symbol IN (${placeholders})`,
        symbols.map((s) => s.toUpperCase()),
    );
    return result.rows;
}
