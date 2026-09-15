import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    filterAutoGroupCandidates,
    normalizeAutoGroupFilterKeywords,
    parseAutoGroupFilterKeywordsInput,
    parseAutoGroupModelFilter,
} from '../src/lib/group-model-filter.ts';

const candidates = [
    { id: 1, name: '仅供测试/model-alpha', channel_name: 'normal/default' },
    { id: 2, name: 'model-alpha', channel_name: '仅供测试/account/default' },
    { id: 3, name: 'model-alpha', channel_name: 'trusted/default' },
    { id: 4, name: 'MODEL-ALPHA-BATCH', channel_name: 'trusted/default' },
];

for (const [mode, keywords, ids] of [
    ['off', ['仅供测试'], [1, 2, 3, 4]],
    ['blacklist', ['仅供测试'], [3, 4]],
    ['whitelist', ['仅供测试'], [1, 2]],
    ['blacklist', ['仅供测试', ' batch '], [3]],
    ['whitelist', ['仅供测试', ' batch '], [1, 2, 4]],
    ['blacklist', [], [1, 2, 3, 4]],
    ['whitelist', [], []],
    ['blacklist', ['  ', ''], [1, 2, 3, 4]],
    ['whitelist', ['  ', ''], []],
    ['blacklist', ['normalmodel'], [1, 2, 3, 4]],
    ['blacklist', ['.*', '*/model'], [1, 2, 3, 4]],
]) {
    test(`${mode} matches literal keywords ${JSON.stringify(keywords)}`, () => {
        const result = filterAutoGroupCandidates(candidates, { mode, keywords });
        assert.deepEqual(result.map((item) => item.id), ids);
    });
}

test('unavailable or invalid rules do not automatically add candidates', () => {
    assert.deepEqual(filterAutoGroupCandidates(candidates, null), []);
});

test('filtering does not mutate manual picker candidates or existing selections', () => {
    const original = structuredClone(candidates);
    const selected = [candidates[0]];
    const eligible = filterAutoGroupCandidates(candidates, { mode: 'blacklist', keywords: ['仅供测试'] });
    assert.deepEqual(candidates, original);
    assert.equal(selected[0], candidates[0]);
    assert.deepEqual([...selected, ...eligible].map((item) => item.id), [1, 3, 4]);
    assert.notEqual(filterAutoGroupCandidates(candidates, { mode: 'off', keywords: [] }), candidates);
});

test('global rules only filter the candidates already matched by the group', () => {
    const matched = candidates.filter((item) => item.name === 'model-alpha');
    assert.deepEqual(
        filterAutoGroupCandidates(matched, { mode: 'whitelist', keywords: ['仅供测试', 'batch'] }).map((item) => item.id),
        [2],
    );
});

test('normalization supports multiple keyword separators, trimming and case-insensitive deduplication', () => {
    assert.deepEqual(parseAutoGroupFilterKeywordsInput('  alpha\nALPHA, Beta， 中文\r\n beta  ,\n'), ['alpha', 'Beta', '中文']);
    assert.deepEqual(normalizeAutoGroupFilterKeywords([' A ', 'a', ' ', '', ' B']), ['A', 'B']);
    const config = parseAutoGroupModelFilter('{"mode":"blacklist","keywords":[" A ","a"," "]}');
    assert.deepEqual(config, { mode: 'blacklist', keywords: ['A'] });
});

test('only an absent legacy setting defaults to off; malformed values fail closed', () => {
    assert.deepEqual(parseAutoGroupModelFilter(undefined), { mode: 'off', keywords: [] });
    for (const invalid of [
        '', 'null', '[]', '{}', 'false', 'bad json',
        '{"mode":"invalid","keywords":[]}', '{"mode":"OFF","keywords":[]}',
        '{"mode":"off"}', '{"keywords":[]}', '{"mode":"off","keywords":null}',
        '{"mode":"off","keywords":[null]}', '{"mode":"off","keywords":[42]}',
        '{"mode":"off","keywords":[true]}', '{"mode":"off","keywords":{}}',
        '{"mode":"off","keywords":["alpha,beta"]}', '{"mode":"off","keywords":["alpha，beta"]}',
        '{"mode":"off","keywords":["alpha\\nbeta"]}',
        '{"mode":"off","keywords":[],"extra":true}', '{"mode":"off","keywords":[]}{}',
    ]) {
        assert.equal(parseAutoGroupModelFilter(invalid), null, invalid);
    }
});

test('keyword limits count Unicode characters and apply after normalization', () => {
    for (const keywords of [Array.from({ length: 100 }, (_, index) => `word-${index}`), ['🌟'.repeat(200)], Array(101).fill('same')]) {
        assert.notEqual(parseAutoGroupModelFilter(JSON.stringify({ mode: 'blacklist', keywords })), null);
    }
    for (const keywords of [Array.from({ length: 101 }, (_, index) => `word-${index}`), ['🌟'.repeat(201)]]) {
        assert.equal(parseAutoGroupModelFilter(JSON.stringify({ mode: 'blacklist', keywords })), null);
    }
});

test('case folding is intentionally limited to ASCII letters', () => {
    const greek = [{ id: 5, name: 'ΟΣ', channel_name: 'trusted/default' }];
    const dottedI = [{ id: 6, name: 'İ', channel_name: 'trusted/default' }];
    assert.deepEqual(filterAutoGroupCandidates(greek, { mode: 'blacklist', keywords: ['οσ'] }), greek);
    assert.deepEqual(filterAutoGroupCandidates(dottedI, { mode: 'blacklist', keywords: ['i'] }), dottedI);
    assert.deepEqual(
        filterAutoGroupCandidates([{ id: 7, name: 'MODEL-ALPHA', channel_name: 'trusted/default' }], {
            mode: 'blacklist',
            keywords: ['model'],
        }).map((item) => item.id),
        [],
    );
});

test('keyword trimming treats the agreed BOM and control whitespace as boundaries', () => {
    assert.deepEqual(parseAutoGroupFilterKeywordsInput('\uFEFFgpt\uFEFF,\u0085model\u0085'), ['gpt', 'model']);
    assert.deepEqual(parseAutoGroupModelFilter('{"mode":"blacklist","keywords":["\uFEFF\u0085\uFEFF"]}'), {
        mode: 'blacklist',
        keywords: [],
    });
});
