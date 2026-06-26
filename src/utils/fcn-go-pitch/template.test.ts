import assert from 'node:assert/strict';
import type { PitchInputs } from './llm-stitcher';
import { buildDealStructureSentence, buildDeterministicPitch, buildMinimalPitch } from './template';

const basePitch: PitchInputs = {
    symbol: 'LITE',
    company_short_desc: 'Lumentum 是数据中心光通信器件核心供应商',
    display_description: 'Lumentum 是光通信器件供应商',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    lit_tags: {
        holding: ['backlog'],
        timing: ['momentum_intact']
    }
};

const dealSentence = buildDealStructureSentence(basePitch);
assert.ok(dealSentence.includes('以 $85（较现价低 15%）承接 LITE'));
assert.ok(dealSentence.includes('年化票息 12%-16%，期限 3 个月'));
assert.ok(dealSentence.includes('若股价未跌破 $85'));
assert.ok(!dealSentence.includes('承接LITE'));
assert.ok(!dealSentence.includes(','));
assert.ok(!dealSentence.includes(';'));

const litePitch = buildDeterministicPitch({
    ...basePitch,
    symbol: 'LITE',
    change_5d_pct: 6.5,
    pct_from_52w_high: -12.8,
    recent_news_titles: ['Lumentum to Join Nasdaq-100 Index Beginning May 18']
});
const tsmPitch = buildDeterministicPitch({
    ...basePitch,
    symbol: 'TSM',
    company_short_desc: '台积电是全球先进制程晶圆代工龙头',
    display_description: '台积电是先进制程代工龙头',
    change_5d_pct: -2.3,
    pct_from_52w_high: -9,
    recent_news_titles: ['TSMC Reports Monthly Revenue Growth']
});

assert.notEqual(litePitch, tsmPitch);
assert.ok(litePitch.includes('近 5 日 +6.5%'));
assert.ok(litePitch.includes('未来 3-6 个月'));
assert.equal(litePitch.includes('让您以'), false);
assert.equal(litePitch.includes('若股价未跌破'), false);
assert.ok(tsmPitch.includes('近 5 日 -2.3%'));
assert.ok(buildMinimalPitch({ ...basePitch, change_5d_pct: 1.2, pct_from_52w_high: -7 }).includes('近 5 日 +1.2%'));

const xomMinimal = buildMinimalPitch({
    ...basePitch,
    symbol: 'XOM',
    recent_news_titles: ['Brent Crude Is Up 85% Since January']
});
assert.equal(xomMinimal.includes('85%'), false);
assert.equal(xomMinimal.includes('近期消息'), false);

const unhMinimal = buildMinimalPitch({
    ...basePitch,
    symbol: 'UNH',
    recent_news_titles: ['Is UnitedHealth Group a Buy, Sell, or Hold in 2026?']
});
assert.equal(unhMinimal.includes('Buy, Sell, or Hold'), false);
assert.equal(unhMinimal.includes('近期消息'), false);

console.log('fcn-go-pitch template tests passed');
