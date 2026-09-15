'use client';

import { useMemo, useRef, useState } from 'react';
import { AlertTriangle, ShieldCheck, X } from 'lucide-react';
import { useTranslations } from 'next-intl';
import {
    useGlobalAutoGroupModelFilter,
    useSetGlobalAutoGroupModelFilter,
} from '@/api/endpoints/group-model-filter';
import { toast } from '@/components/common/Toast';
import { Button } from '@/components/ui/button';
import {
    MorphingDialogDescription,
    MorphingDialogTitle,
    useMorphingDialog,
} from '@/components/ui/morphing-dialog';
import {
    AUTO_GROUP_FILTER_MAX_KEYWORD_LENGTH,
    AUTO_GROUP_FILTER_MAX_KEYWORDS,
    parseAutoGroupFilterKeywordsInput,
    type AutoGroupModelFilter,
    type AutoGroupModelFilterMode,
} from '@/lib/group-model-filter';
import { cn } from '@/lib/utils';

const FILTER_MODES: readonly AutoGroupModelFilterMode[] = ['off', 'blacklist', 'whitelist'];

function GlobalModelFilterForm({
    initialFilter,
    disabled,
    onSavingChange,
}: {
    initialFilter: AutoGroupModelFilter | null;
    disabled: boolean;
    onSavingChange?: (saving: boolean) => void;
}) {
    const t = useTranslations('group.globalModelFilter');
    const { setIsOpen, uniqueId } = useMorphingDialog();
    const saveFilter = useSetGlobalAutoGroupModelFilter();
    const submitting = useRef(false);
    // Initialize once per opening; settings polling must not replace an unsaved draft.
    const [mode, setMode] = useState<AutoGroupModelFilterMode | null>(initialFilter?.mode ?? null);
    const [keywordsText, setKeywordsText] = useState(initialFilter?.keywords.join('\n') ?? '');
    const [initialInvalid] = useState(initialFilter === null);
    const keywords = useMemo(() => parseAutoGroupFilterKeywordsInput(keywordsText), [keywordsText]);
    const tooManyKeywords = keywords.length > AUTO_GROUP_FILTER_MAX_KEYWORDS;
    const keywordTooLong = keywords.some((keyword) => [...keyword].length > AUTO_GROUP_FILTER_MAX_KEYWORD_LENGTH);
    const invalidKeywords = tooManyKeywords || keywordTooLong;
    const cannotSave = disabled || saveFilter.isPending || mode === null || invalidKeywords;

    const handleSave = async () => {
        if (cannotSave || mode === null || submitting.current) return;
        submitting.current = true;
        onSavingChange?.(true);
        try {
            await saveFilter.mutateAsync({ mode, keywords });
            toast.success(t('saved'));
            onSavingChange?.(false);
            setIsOpen(false);
        } catch (error) {
            toast.error(t('saveFailed'), {
                description: error instanceof Error ? error.message : undefined,
            });
        } finally {
            submitting.current = false;
            onSavingChange?.(false);
        }
    };

    return (
        <form
            className="grid min-w-0 gap-4"
            onSubmit={(event) => {
                event.preventDefault();
                void handleSave();
            }}
        >
            {initialInvalid && (
                <p role="alert" className="rounded-xl border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                    {t('invalidConfig')} {t('repairHint')}
                </p>
            )}
            <fieldset disabled={disabled || saveFilter.isPending} className="min-w-0">
                <legend className="mb-2 text-sm font-medium">{t('modeLabel')}</legend>
                <div className="grid grid-cols-3 gap-2">
                    {FILTER_MODES.map((value) => (
                        <label
                            key={value}
                            className={cn(
                                'flex min-w-0 cursor-pointer items-center justify-center rounded-xl border px-2 py-2 text-center text-sm transition-colors focus-within:ring-2 focus-within:ring-ring',
                                mode === value ? 'border-primary bg-primary/10 text-primary' : 'border-border bg-background hover:bg-muted/60',
                                (disabled || saveFilter.isPending) && 'cursor-not-allowed opacity-50',
                            )}
                        >
                            <input
                                type="radio"
                                name={`global-model-filter-mode-${uniqueId}`}
                                value={value}
                                checked={mode === value}
                                onChange={() => setMode(value)}
                                className="sr-only"
                            />
                            {t(`mode.${value}`)}
                        </label>
                    ))}
                </div>
            </fieldset>
            {mode !== null && <p className="text-sm leading-5 text-muted-foreground">{t(`modeHint.${mode}`)}</p>}
            <label className="grid min-w-0 gap-2 text-sm font-medium">
                {t('keywordsLabel')}
                <textarea
                    value={keywordsText}
                    onChange={(event) => setKeywordsText(event.target.value)}
                    disabled={disabled || saveFilter.isPending}
                    aria-invalid={invalidKeywords}
                    placeholder={t('keywordsPlaceholder')}
                    rows={6}
                    spellCheck={false}
                    className="block min-h-32 w-full min-w-0 max-w-full resize-y rounded-xl border border-border bg-background px-3 py-2 text-base font-normal outline-none placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/50 disabled:opacity-50 sm:text-sm"
                />
                <span className="text-xs font-normal leading-5 text-muted-foreground">{t('keywordsHint')}</span>
            </label>
            {invalidKeywords && (
                <p role="alert" className="text-sm text-destructive">
                    {tooManyKeywords ? t('tooManyKeywords') : t('keywordTooLong')}
                </p>
            )}
            {mode === 'whitelist' && keywords.length === 0 && (
                <div role="alert" className="flex gap-2 rounded-xl border border-amber-500/40 bg-amber-500/10 p-3 text-sm leading-5 text-amber-700 dark:text-amber-300">
                    <AlertTriangle className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
                    <p>{t('whitelistEmptyWarning')}</p>
                </div>
            )}
            {saveFilter.isError && <p role="alert" className="text-sm text-destructive">{t('saveFailed')}</p>}
            <div className="flex flex-wrap justify-end gap-2 pt-1">
                <Button type="button" variant="outline" className="rounded-xl" disabled={saveFilter.isPending} onClick={() => setIsOpen(false)}>
                    {t('cancel')}
                </Button>
                <Button type="submit" className="rounded-xl" disabled={cannotSave}>
                    {saveFilter.isPending ? t('saving') : t('save')}
                </Button>
            </div>
        </form>
    );
}

export function GlobalModelFilterDialogContent({ onSavingChange }: { onSavingChange?: (saving: boolean) => void }) {
    const t = useTranslations('group.globalModelFilter');
    const { setIsOpen, uniqueId } = useMorphingDialog();
    const query = useGlobalAutoGroupModelFilter();

    return (
        <div className="flex min-h-0 min-w-0 flex-col overflow-hidden">
            <MorphingDialogTitle className="shrink-0">
                <header className="mb-3 flex items-center justify-between gap-3">
                    <h2 id={`motion-ui-morphing-dialog-title-${uniqueId}`} className="flex min-w-0 items-center gap-2 text-xl font-bold text-card-foreground">
                        <ShieldCheck className="size-5 shrink-0 text-primary" aria-hidden="true" />
                        {t('title')}
                    </h2>
                    <Button type="button" variant="ghost" size="icon" aria-label={t('close')} title={t('close')} onClick={() => setIsOpen(false)}>
                        <X className="size-4" />
                    </Button>
                </header>
            </MorphingDialogTitle>
            <MorphingDialogDescription disableLayoutAnimation className="min-h-0 min-w-0 overflow-y-auto">
                <div className="grid gap-3 pb-4 text-sm leading-6 text-muted-foreground" id={`motion-ui-morphing-dialog-description-${uniqueId}`}>
                    <p>{t('description')}</p>
                    <p>{t('matchDescription')}</p>
                    <p>{t('notice')}</p>
                </div>
                {query.isError && (
                    <div role="alert" className="mb-4 flex flex-wrap items-center justify-between gap-2 rounded-xl border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                        <p>{t('loadFailed')}</p>
                        <Button type="button" variant="outline" className="rounded-xl" disabled={query.isFetching} onClick={() => void query.refetch()}>
                            {query.isFetching ? t('loading') : t('retry')}
                        </Button>
                    </div>
                )}
                {query.data === undefined ? (
                    <div className="grid gap-4">
                        {!query.isError && <p role="status" className="py-6 text-center text-sm text-muted-foreground">{t('loading')}</p>}
                        <Button type="button" variant="outline" className="justify-self-end rounded-xl" onClick={() => setIsOpen(false)}>{t('cancel')}</Button>
                    </div>
                ) : (
                    <>
                        {!query.supported && (
                            <p role="alert" className="mb-4 rounded-xl border border-amber-500/40 bg-amber-500/10 p-3 text-sm leading-5 text-amber-700 dark:text-amber-300">
                                {t('unsupported')}
                            </p>
                        )}
                        <GlobalModelFilterForm
                            initialFilter={query.filter}
                            disabled={query.isError || !query.supported}
                            onSavingChange={onSavingChange}
                        />
                    </>
                )}
            </MorphingDialogDescription>
        </div>
    );
}
