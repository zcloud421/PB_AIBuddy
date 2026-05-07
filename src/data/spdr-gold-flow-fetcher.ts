import axios from 'axios';
import zlib from 'zlib';

const SPDR_GLD_ARCHIVE_URL = 'https://api.spdrgoldshares.com/api/v1/historical-archive';
const FETCH_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    Accept: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*'
};
const CACHE_TTL_MS = 12 * 60 * 60 * 1000;

export interface GldDailyHolding {
    date: string;
    tonnes_in_trust: number;
    nav_usd: number;
    total_net_assets_usd: number;
}

export interface GldFlowTrend {
    latest_date: string;
    latest_tonnes: number;
    change_1d_tonnes: number | null;
    change_5d_tonnes: number | null;
    change_mtd_tonnes: number | null;
    change_ytd_tonnes: number | null;
    direction_5d: 'inflow' | 'outflow' | 'flat';
    consecutive_inflow_days: number;
    consecutive_outflow_days: number;
    history_size: number;
}

interface ZipEntry {
    fileName: string;
    compressionMethod: number;
    compressedSize: number;
    localHeaderOffset: number;
}

let memoryCache: { fetchedAt: number; trend: GldFlowTrend } | null = null;

function roundTonne(value: number | null): number | null {
    return value === null ? null : Math.round(value * 10) / 10;
}

function columnIndex(cellRef: string): number {
    const letters = cellRef.match(/^[A-Z]+/)?.[0] ?? '';
    return [...letters].reduce((sum, char) => sum * 26 + char.charCodeAt(0) - 64, 0) - 1;
}

function decodeXml(value: string): string {
    return value
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'");
}

function parseZipEntries(buffer: Buffer): Map<string, Buffer> {
    let eocdOffset = -1;
    for (let offset = buffer.length - 22; offset >= 0; offset -= 1) {
        if (buffer.readUInt32LE(offset) === 0x06054b50) {
            eocdOffset = offset;
            break;
        }
    }
    if (eocdOffset < 0) {
        throw new Error('XLSX archive missing end-of-central-directory marker');
    }

    const entryCount = buffer.readUInt16LE(eocdOffset + 10);
    const centralDirectoryOffset = buffer.readUInt32LE(eocdOffset + 16);
    const entries: ZipEntry[] = [];
    let cursor = centralDirectoryOffset;

    for (let index = 0; index < entryCount; index += 1) {
        if (buffer.readUInt32LE(cursor) !== 0x02014b50) {
            throw new Error('XLSX central directory entry format unexpected');
        }

        const compressionMethod = buffer.readUInt16LE(cursor + 10);
        const compressedSize = buffer.readUInt32LE(cursor + 20);
        const fileNameLength = buffer.readUInt16LE(cursor + 28);
        const extraLength = buffer.readUInt16LE(cursor + 30);
        const commentLength = buffer.readUInt16LE(cursor + 32);
        const localHeaderOffset = buffer.readUInt32LE(cursor + 42);
        const fileName = buffer.toString('utf8', cursor + 46, cursor + 46 + fileNameLength);

        entries.push({ fileName, compressionMethod, compressedSize, localHeaderOffset });
        cursor += 46 + fileNameLength + extraLength + commentLength;
    }

    const files = new Map<string, Buffer>();
    for (const entry of entries) {
        const local = entry.localHeaderOffset;
        if (buffer.readUInt32LE(local) !== 0x04034b50) continue;
        const fileNameLength = buffer.readUInt16LE(local + 26);
        const extraLength = buffer.readUInt16LE(local + 28);
        const dataStart = local + 30 + fileNameLength + extraLength;
        const compressed = buffer.subarray(dataStart, dataStart + entry.compressedSize);
        const uncompressed =
            entry.compressionMethod === 8
                ? zlib.inflateRawSync(compressed)
                : entry.compressionMethod === 0
                    ? compressed
                    : null;
        if (uncompressed) {
            files.set(entry.fileName, uncompressed);
        }
    }

    return files;
}

function parseSharedStrings(xml: string): string[] {
    const strings: string[] = [];
    for (const match of xml.matchAll(/<si\b[\s\S]*?<\/si>/g)) {
        const text = [...match[0].matchAll(/<t[^>]*>([\s\S]*?)<\/t>/g)]
            .map((part) => decodeXml(part[1]))
            .join('');
        strings.push(text);
    }
    return strings;
}

function parseSheetRows(xml: string, sharedStrings: string[]): Array<Array<string | number | null>> {
    const rows: Array<Array<string | number | null>> = [];

    for (const rowMatch of xml.matchAll(/<row\b[^>]*>([\s\S]*?)<\/row>/g)) {
        const row: Array<string | number | null> = [];
        for (const cellMatch of rowMatch[1].matchAll(/<c\b([^>]*)>([\s\S]*?)<\/c>/g)) {
            const attrs = cellMatch[1];
            const body = cellMatch[2];
            const ref = attrs.match(/\br="([^"]+)"/)?.[1] ?? '';
            const type = attrs.match(/\bt="([^"]+)"/)?.[1] ?? '';
            const valueText = body.match(/<v>([\s\S]*?)<\/v>/)?.[1] ?? '';
            const inlineText = body.match(/<t[^>]*>([\s\S]*?)<\/t>/)?.[1] ?? '';
            const index = columnIndex(ref);
            let value: string | number | null = null;

            if (type === 's') {
                value = sharedStrings[Number(valueText)] ?? null;
            } else if (type === 'inlineStr') {
                value = decodeXml(inlineText);
            } else if (valueText) {
                const numeric = Number(valueText);
                value = Number.isFinite(numeric) ? numeric : decodeXml(valueText);
            }

            row[index] = value;
        }
        rows.push(row);
    }

    return rows;
}

function parseSpdrDate(value: string | number | null | undefined): string | null {
    if (typeof value === 'number') {
        const excelEpoch = Date.UTC(1899, 11, 30);
        return new Date(excelEpoch + value * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    }
    if (typeof value !== 'string') return null;

    const match = value.trim().match(/^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/);
    if (!match) return null;

    const monthMap: Record<string, number> = {
        jan: 0,
        feb: 1,
        mar: 2,
        apr: 3,
        may: 4,
        jun: 5,
        jul: 6,
        aug: 7,
        sep: 8,
        oct: 9,
        nov: 10,
        dec: 11
    };
    const month = monthMap[match[2].toLowerCase()];
    if (month === undefined) return null;

    return new Date(Date.UTC(Number(match[3]), month, Number(match[1]))).toISOString().slice(0, 10);
}

function toNumber(value: string | number | null | undefined): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    const numeric = Number(value.replace(/,/g, ''));
    return Number.isFinite(numeric) ? numeric : null;
}

async function fetchSpdrHistoricalCsv(): Promise<GldDailyHolding[]> {
    const response = await axios.get<ArrayBuffer>(SPDR_GLD_ARCHIVE_URL, {
        params: { product: 'gld', exchange: 'NYSE', lang: 'en' },
        headers: FETCH_HEADERS,
        timeout: 15000,
        responseType: 'arraybuffer'
    });

    const files = parseZipEntries(Buffer.from(response.data));
    const sharedStrings = files.has('xl/sharedStrings.xml')
        ? parseSharedStrings(files.get('xl/sharedStrings.xml')!.toString('utf8'))
        : [];

    const worksheetFiles = [...files.keys()].filter((fileName) => /^xl\/worksheets\/sheet\d+\.xml$/.test(fileName));
    for (const worksheetFile of worksheetFiles) {
        const rows = parseSheetRows(files.get(worksheetFile)!.toString('utf8'), sharedStrings);
        const headerIndex = rows.findIndex((row) => row.some((value) => value === 'Tonnes of Gold'));
        if (headerIndex < 0) continue;

        const header = rows[headerIndex].map((value) => String(value ?? '').trim());
        const dateIndex = header.indexOf('Date');
        const navIndex = header.indexOf('NAV/Share at 10:30am NYT');
        const tonnesIndex = header.indexOf('Tonnes of Gold');
        const assetsIndex = header.indexOf('Total Net Asset Value in the Trust');
        if (dateIndex < 0 || navIndex < 0 || tonnesIndex < 0 || assetsIndex < 0) {
            continue;
        }

        return rows
            .slice(headerIndex + 1)
            .map((row) => {
                const date = parseSpdrDate(row[dateIndex]);
                const nav = toNumber(row[navIndex]);
                const tonnes = toNumber(row[tonnesIndex]);
                const assets = toNumber(row[assetsIndex]);
                if (!date || nav === null || tonnes === null || assets === null || tonnes <= 0) {
                    return null;
                }
                return {
                    date,
                    nav_usd: nav,
                    tonnes_in_trust: tonnes,
                    total_net_assets_usd: assets
                };
            })
            .filter((row): row is GldDailyHolding => Boolean(row))
            .sort((a, b) => a.date.localeCompare(b.date))
            .slice(-260);
    }

    throw new Error('SPDR GLD archive missing Tonnes of Gold sheet');
}

function computeTrend(history: GldDailyHolding[]): GldFlowTrend {
    if (history.length === 0) {
        throw new Error('No GLD history data');
    }
    const latest = history[history.length - 1];

    const findHistoricalTonnes = (daysBack: number): number | null => {
        const target = history[history.length - 1 - daysBack];
        return target?.tonnes_in_trust ?? null;
    };

    const prior1d = findHistoricalTonnes(1);
    const prior5d = findHistoricalTonnes(5);
    const change1d = prior1d !== null ? latest.tonnes_in_trust - prior1d : null;
    const change5d = prior5d !== null ? latest.tonnes_in_trust - prior5d : null;

    const latestMonth = latest.date.slice(0, 7);
    const monthStart = history.find((holding) => holding.date.startsWith(latestMonth));
    const changeMtd = monthStart ? latest.tonnes_in_trust - monthStart.tonnes_in_trust : null;

    const latestYear = latest.date.slice(0, 4);
    const yearStart = history.find((holding) => holding.date.startsWith(latestYear));
    const changeYtd = yearStart ? latest.tonnes_in_trust - yearStart.tonnes_in_trust : null;

    let consecutiveInflow = 0;
    let consecutiveOutflow = 0;
    for (let index = history.length - 1; index > 0; index -= 1) {
        const diff = history[index].tonnes_in_trust - history[index - 1].tonnes_in_trust;
        if (diff > 0.01) {
            if (consecutiveOutflow > 0) break;
            consecutiveInflow += 1;
        } else if (diff < -0.01) {
            if (consecutiveInflow > 0) break;
            consecutiveOutflow += 1;
        } else {
            break;
        }
    }

    const direction5d: GldFlowTrend['direction_5d'] =
        change5d === null ? 'flat' : change5d > 0.5 ? 'inflow' : change5d < -0.5 ? 'outflow' : 'flat';

    return {
        latest_date: latest.date,
        latest_tonnes: latest.tonnes_in_trust,
        change_1d_tonnes: roundTonne(change1d),
        change_5d_tonnes: roundTonne(change5d),
        change_mtd_tonnes: roundTonne(changeMtd),
        change_ytd_tonnes: roundTonne(changeYtd),
        direction_5d: direction5d,
        consecutive_inflow_days: consecutiveInflow,
        consecutive_outflow_days: consecutiveOutflow,
        history_size: history.length
    };
}

/**
 * Returns SPDR Gold Trust tonnes-in-trust flow trend with a 12h cache. Failures
 * return null so gold narratives can fall back to news/context without blocking.
 */
export async function getGldFlowTrend(): Promise<GldFlowTrend | null> {
    if (memoryCache && Date.now() - memoryCache.fetchedAt < CACHE_TTL_MS) {
        return memoryCache.trend;
    }

    try {
        const history = await fetchSpdrHistoricalCsv();
        if (history.length < 2) {
            return null;
        }
        const trend = computeTrend(history);
        memoryCache = { fetchedAt: Date.now(), trend };
        return trend;
    } catch (error) {
        console.warn('[spdr-gold-flow-fetcher] failed:', error instanceof Error ? error.message : error);
        return null;
    }
}
