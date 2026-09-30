import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { stripTypeScriptTypes } from 'node:module';
import { test } from 'node:test';

const source = await readFile(new URL('../src/api/error-i18n.ts', import.meta.url), 'utf8');
const importsRemoved = source.replace(/^import[\s\S]*?;\n/gm, '');
const testModule = `
const useSettingStore = { getState: () => ({ locale: 'en' }) };
const enLocale = { errors: { site_channel: { key_create_failed: 'translated generic error' } } };
const zhHansLocale = enLocale;
const zhHantLocale = enLocale;
${importsRemoved}
`;
const moduleSource = stripTypeScriptTypes(testModule, { mode: 'strip' }).replaceAll(
    'export function translateApiErrorCode',
    'function translateApiErrorCode',
);
const loadedModule = await import(`data:text/javascript,${encodeURIComponent(`${moduleSource}\nexport { translateApiErrorCode };`)}`);

test('refresh failure param takes precedence over generic API translation', () => {
    assert.equal(
        loadedModule.translateApiErrorCode('site_channel.key_create_failed', 'fallback', {
            sub2apiRefreshFailure: 'refresh token expired',
        }),
        'refresh token expired',
    );
});

test('ordinary API errors keep the existing translation and invalid refresh params do not override it', () => {
    assert.equal(loadedModule.translateApiErrorCode('site_channel.key_create_failed', 'fallback'), 'translated generic error');
    assert.equal(
        loadedModule.translateApiErrorCode('site_channel.key_create_failed', 'fallback', { sub2apiRefreshFailure: '' }),
        'translated generic error',
    );
    assert.equal(
        loadedModule.translateApiErrorCode('site_channel.key_create_failed', 'fallback', { sub2apiRefreshFailure: 42 }),
        'translated generic error',
    );
});
