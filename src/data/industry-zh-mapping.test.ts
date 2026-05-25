import assert from 'node:assert/strict';
import { mapIndustryToZh } from './industry-zh-mapping';

const CASES: Array<[string, string]> = [
    ['SEMICONDUCTORS & RELATED DEVICES', '半导体'],
    ['SERVICES-PREPACKAGED SOFTWARE', '软件'],
    ['INTERNET INFORMATION RETRIEVAL SERVICES', '互联网平台'],
    ['DIGITAL ADVERTISING', '数字广告'],
    ['CONSUMER ELECTRONICS', '消费电子'],
    ['COMMUNICATIONS EQUIPMENT', '通信设备'],
    ['COMPUTER & OFFICE EQUIPMENT', '企业硬件'],
    ['COMPUTER HARDWARE', '计算机硬件'],
    ['CYBERSECURITY SOFTWARE', '网络安全'],
    ['SERVICES-COMPUTER PROCESSING & DATA PREPARATION', '数据处理'],
    ['DATA PROCESSING AND HOSTING', '云计算'],
    ['SERVICES-BUSINESS SERVICES, NEC', '商业服务'],
    ['PHARMACEUTICAL PREPARATIONS', '制药'],
    ['BIOLOGICAL PRODUCTS', '生物科技'],
    ['SURGICAL & MEDICAL INSTRUMENTS', '医疗设备'],
    ['HEALTH SERVICES', '医疗服务'],
    ['MANAGED CARE', '医疗保险'],
    ['NATIONAL COMMERCIAL BANKS', '综合银行'],
    ['LIFE INSURANCE', '保险'],
    ['INVESTMENT ADVICE', '资产管理'],
    ['SECURITY BROKERS AND DEALERS', '资本市场'],
    ['CREDIT SERVICES', '信贷服务'],
    ['PAYMENT PROCESSING', '支付网络'],
    ['MOTOR VEHICLES & PASSENGER CAR BODIES', '汽车制造'],
    ['AIRCRAFT AND PARTS', '航空航天'],
    ['ELECTRICAL EQUIPMENT', '电气设备'],
    ['ENGINEERING SERVICES', '工程建筑'],
    ['CONSTRUCTION MACHINERY', '工业机械'],
    ['PETROLEUM REFINING', '综合油气'],
    ['CRUDE PETROLEUM & NATURAL GAS', '油气勘探'],
    ['OIL FIELD SERVICES', '油服设备'],
    ['SOLAR ENERGY', '可再生能源'],
    ['ELECTRIC SERVICES', '电力公用事业'],
    ['RESTAURANTS', '餐饮连锁'],
    ['APPAREL RETAIL', '服装零售'],
    ['VARIETY STORES', '折扣零售'],
    ['MOTION PICTURE STREAMING', '线上娱乐'],
    ['CABLE & OTHER PAY TELEVISION SERVICES', '媒体分发'],
    ['SERVICES-TO DWELLINGS & OTHER BUILDINGS', '住宿平台'],
    ['TELEPHONE COMMUNICATIONS', '电信服务'],
    ['INDUSTRIAL REIT', '工业 REIT'],
    ['OFFICE REIT', '商业 REIT'],
    ['APARTMENT REIT', '居住 REIT'],
    ['E-COMMERCE', '电商平台'],
    ['SOFT DRINKS', '饮料'],
    ['PACKAGED FOODS', '食品消费'],
    ['GOLD AND SILVER MINING', '黄金矿业']
];

for (const [input, expected] of CASES) {
    assert.equal(mapIndustryToZh(input), expected, input);
}

assert.equal(mapIndustryToZh('UNKNOWN SIC DESCRIPTION'), null);
assert.equal(mapIndustryToZh(null), null);

console.log('industry-zh-mapping tests passed');
