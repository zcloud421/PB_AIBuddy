import type { NewsItem } from '../data/news-fetcher';

export interface ClaimDetection {
    kind: 'date' | 'corporate_action' | 'earnings_term';
    rawSnippet: string;
    position: number;
}

export interface EventValidationResult {
    passed: boolean;
    unanchored: Array<{ kind: string; snippet: string }>;
}

const DATE_PATTERNS: RegExp[] = [
    /\d{4}年\d{1,2}月\d{1,2}日/g,
    /\d{4}[-/]\d{1,2}[-/]\d{1,2}/g,
    /周[一二三四五六日]/g,
    /(本|上|下|近|刚)(周|月|季)/g
];

const CORPORATE_ACTION_KEYWORDS = [
    '纳入', '剔除', '收购', '并购', '分拆', '回购', '增持', '减持',
    '获批', '批准', '通过', '裁决', '诉讼', '反垄断',
    '下调', '上调', '维持', '上修', '下修',
    '发布', '推出', '签约', '合作', '战略合作', '续约',
    '罢工', '召回', '停产', '关闭'
];

const EARNINGS_TERMS = [
    '营收', '收入', '净利润', '净利', 'EPS', '每股收益',
    '毛利率', '净利率', '运营利润率',
    '指引', '展望', 'guidance',
    '同比', 'YoY', '环比', 'QoQ',
    '超预期', '低于预期', 'beat', 'miss'
];

export function detectClaims(text: string): ClaimDetection[] {
    const claims: ClaimDetection[] = [];

    for (const pattern of DATE_PATTERNS) {
        let match: RegExpExecArray | null;
        pattern.lastIndex = 0;
        while ((match = pattern.exec(text)) !== null) {
            claims.push({ kind: 'date', rawSnippet: match[0], position: match.index });
        }
    }

    for (const keyword of CORPORATE_ACTION_KEYWORDS) {
        let index = text.indexOf(keyword);
        while (index !== -1) {
            claims.push({ kind: 'corporate_action', rawSnippet: keyword, position: index });
            index = text.indexOf(keyword, index + 1);
        }
    }

    for (const term of EARNINGS_TERMS) {
        let index = text.indexOf(term);
        while (index !== -1) {
            claims.push({ kind: 'earnings_term', rawSnippet: term, position: index });
            index = text.indexOf(term, index + 1);
        }
    }

    return claims.sort((left, right) => left.position - right.position);
}

export function hasHeadlineAnchor(claim: ClaimDetection, newsItems: Array<Pick<NewsItem, 'title'> & { published_at?: string }>): boolean {
    if (newsItems.length === 0) {
        return false;
    }

    for (const item of newsItems) {
        const title = item.title.toLowerCase();
        const snippet = claim.rawSnippet.toLowerCase();

        if (claim.kind === 'corporate_action') {
            const equivalents = corporateActionToEnglish(snippet);
            if (title.includes(snippet) || equivalents.some((equivalent) => title.includes(equivalent))) {
                return true;
            }
        }

        if (claim.kind === 'date') {
            if (!item.published_at) {
                continue;
            }
            const publishedDate = new Date(item.published_at);
            const daysSincePublish = (Date.now() - publishedDate.getTime()) / (1000 * 60 * 60 * 24);
            if (Number.isFinite(daysSincePublish) && daysSincePublish <= 14) {
                return true;
            }
        }

        if (claim.kind === 'earnings_term') {
            const earningsAnchors = ['earnings', 'quarter', 'q1', 'q2', 'q3', 'q4', 'revenue', 'eps', 'guidance', '营收', '财报', '季度'];
            if (earningsAnchors.some((anchor) => title.includes(anchor))) {
                return true;
            }
        }
    }

    return false;
}

export function validateEventAnchors(
    text: string,
    newsItems: Array<Pick<NewsItem, 'title'> & { published_at?: string }>
): EventValidationResult {
    const claims = detectClaims(text);
    const unanchored: EventValidationResult['unanchored'] = [];

    for (const claim of claims) {
        if (!hasHeadlineAnchor(claim, newsItems)) {
            unanchored.push({ kind: claim.kind, snippet: claim.rawSnippet });
        }
    }

    return {
        passed: unanchored.length === 0,
        unanchored
    };
}

function corporateActionToEnglish(zh: string): string[] {
    const map: Record<string, string[]> = {
        '纳入': ['add to', 'join', 'include', 'included'],
        '剔除': ['remove', 'removed', 'delete', 'deleted'],
        '收购': ['acquire', 'acquisition', 'buyout', 'merger'],
        '并购': ['merger', 'acquisition', 'm&a'],
        '分拆': ['spinoff', 'spin-off', 'split'],
        '回购': ['buyback', 'repurchase'],
        '获批': ['approve', 'approval', 'approved'],
        '批准': ['approve', 'approval', 'approved'],
        '上调': ['raise', 'raised', 'upgrade', 'boost'],
        '下调': ['cut', 'downgrade', 'reduce', 'lower'],
        '发布': ['launch', 'release', 'announce', 'unveil'],
        '推出': ['launch', 'release', 'unveil'],
        '合作': ['partner', 'partnership', 'collaboration'],
        '战略合作': ['partner', 'partnership', 'collaboration'],
        '诉讼': ['lawsuit', 'sue', 'litigation'],
        '召回': ['recall'],
        '罢工': ['strike']
    };
    return map[zh] ?? [];
}
