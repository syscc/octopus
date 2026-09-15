export type AutoGroupModelFilterMode = 'off' | 'blacklist' | 'whitelist';

export interface AutoGroupModelFilter {
    mode: AutoGroupModelFilterMode;
    keywords: string[];
}

export const AUTO_GROUP_FILTER_MAX_KEYWORDS = 100;
export const AUTO_GROUP_FILTER_MAX_KEYWORD_LENGTH = 200;
export const DEFAULT_AUTO_GROUP_MODEL_FILTER: AutoGroupModelFilter = { mode: 'off', keywords: [] };

function trimAutoGroupFilterKeyword(value: string): string {
    return value.replace(/^[\s\uFEFF\u0085]+|[\s\uFEFF\u0085]+$/gu, '');
}

function foldAutoGroupFilterAscii(value: string): string {
    return value.replace(/[A-Z]/g, (character) => character.toLowerCase());
}

export function normalizeAutoGroupFilterKeywords(keywords: readonly string[]): string[] {
    const seen = new Set<string>();
    return keywords.reduce<string[]>((result, keyword) => {
        const trimmed = trimAutoGroupFilterKeyword(keyword);
        const normalized = foldAutoGroupFilterAscii(trimmed);
        if (normalized && !seen.has(normalized)) {
            seen.add(normalized);
            result.push(trimmed);
        }
        return result;
    }, []);
}

export function parseAutoGroupFilterKeywordsInput(value: string): string[] {
    return normalizeAutoGroupFilterKeywords(value.split(/[\r\n,，]+/));
}

// An invalid saved policy must not silently turn into an unrestricted policy.
export function parseAutoGroupModelFilter(value: string | undefined): AutoGroupModelFilter | null {
    if (value === undefined) return { mode: 'off', keywords: [] };
    try {
        const parsed: unknown = JSON.parse(value);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
        const record = parsed as Record<string, unknown>;
        if (Object.keys(record).some((key) => key !== 'mode' && key !== 'keywords')) return null;
        if (record.mode !== 'off' && record.mode !== 'blacklist' && record.mode !== 'whitelist') return null;
        if (!Array.isArray(record.keywords) || record.keywords.some((keyword) => typeof keyword !== 'string')) return null;
        if ((record.keywords as string[]).some((keyword) => /[,，\r\n]/u.test(keyword))) return null;
        const keywords = normalizeAutoGroupFilterKeywords(record.keywords as string[]);
        if (keywords.length > AUTO_GROUP_FILTER_MAX_KEYWORDS ||
            keywords.some((keyword) => [...keyword].length > AUTO_GROUP_FILTER_MAX_KEYWORD_LENGTH)) return null;
        return { mode: record.mode, keywords };
    } catch {
        return null;
    }
}

export function filterAutoGroupCandidates<T extends { name: string; channel_name: string }>(
    candidates: readonly T[],
    filter: AutoGroupModelFilter | null,
): T[] {
    if (!filter) return [];
    if (filter.mode === 'off') return [...candidates];
    const keywords = normalizeAutoGroupFilterKeywords(filter.keywords).map(foldAutoGroupFilterAscii);
    return candidates.filter((candidate) => {
        const modelName = foldAutoGroupFilterAscii(candidate.name);
        const channelName = foldAutoGroupFilterAscii(candidate.channel_name);
        const matched = keywords.some((keyword) => modelName.includes(keyword) || channelName.includes(keyword));
        return filter.mode === 'whitelist' ? matched : !matched;
    });
}
