import assert from 'node:assert/strict';
import { CURATED_DESCRIPTIONS, getCompanyDescription, getDisplayDescription } from './company-description';

const originalKey = process.env.MASSIVE_API_KEY;
const originalFetch = global.fetch;

async function run(): Promise<void> {
    assert.ok(Object.keys(CURATED_DESCRIPTIONS).length >= 80);

    const curated = await getDisplayDescription('NVDA');
    assert.ok(curated.includes('NVIDIA'));
    assert.ok(curated.length >= 18 && curated.length <= 28);
    for (const term of ['标的', '龙头', '供应商', '平台', '厂商', '公司']) {
        assert.equal(curated.endsWith(term[0]) && !curated.endsWith(term), false);
    }

    process.env.MASSIVE_API_KEY = 'test-key';
    global.fetch = (async () => ({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => ({
            results: {
                name: 'Example Software Corporation',
                sic_description: 'SERVICES-PREPACKAGED SOFTWARE',
                type: 'CS',
                market_cap: 123,
                primary_exchange: 'NASDAQ'
            }
        })
    })) as unknown as typeof fetch;

    const massive = await getDisplayDescription('ZZZX');
    assert.equal(massive, 'Example Software 是软件公司');
    const desc = await getCompanyDescription('ZZZX');
    assert.equal(desc?.industry, 'SERVICES-PREPACKAGED SOFTWARE');

    global.fetch = (async () => {
        throw new Error('network down');
    }) as unknown as typeof fetch;

    const fallback = await getDisplayDescription('NOPE');
    assert.equal(fallback, 'NOPE 是美股核心标的');

    global.fetch = (async () => ({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => ({
            results: {
                name: 'Very Long Example Infrastructure Supplier Corporation',
                sic_description: 'ELECTRICAL WORK',
                type: 'CS',
                market_cap: 123,
                primary_exchange: 'NYSE'
            }
        })
    })) as unknown as typeof fetch;
    const truncated = await getDisplayDescription('LONG');
    assert.ok(truncated.length <= 29);
    for (const term of ['标的', '龙头', '供应商', '平台', '厂商', '公司']) {
        assert.equal(truncated.endsWith(term[0]) && !truncated.endsWith(term), false);
    }
}

run()
    .then(() => {
        if (originalKey === undefined) delete process.env.MASSIVE_API_KEY;
        else process.env.MASSIVE_API_KEY = originalKey;
        global.fetch = originalFetch;
        console.log('company-description tests passed');
    })
    .catch((error) => {
        if (originalKey === undefined) delete process.env.MASSIVE_API_KEY;
        else process.env.MASSIVE_API_KEY = originalKey;
        global.fetch = originalFetch;
        throw error;
    });
