'use client';

import { useTranslations } from 'next-intl';
import { CalendarCheck2, CalendarSync, DollarSign, Globe2, RefreshCw, type LucideIcon } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { SettingKey } from '@/api/endpoints/setting';
import { useLastSyncTime, useSyncChannel } from '@/api/endpoints/channel';
import { useLastUpdateTime, useUpdateModelPrice } from '@/api/endpoints/model';
import { useCheckinAllSites, useSiteLastCheckinTime, useSiteLastSyncTime, useSyncAllSites } from '@/api/endpoints/site';
import { toast } from '@/components/common/Toast';
import { useSettingStore } from '@/stores/setting';
import { translateSiteMessage } from '@/components/modules/site/site-message';
import { SettingCard, useSettingField, useSettingToggle } from './shared';
import { cn } from '@/lib/utils';

function getErrorMessage(error: unknown, fallback: string) {
    if (error instanceof Error && error.message.trim()) {
        return error.message;
    }
    if (error && typeof error === 'object' && 'message' in error) {
        const message = (error as { message?: unknown }).message;
        if (typeof message === 'string' && message.trim()) {
            return message;
        }
    }
    return fallback;
}

// 每行一个定时任务：自动执行间隔（小时）+ 手动触发，可选展示上次执行时间
function TaskRow({ icon: Icon, label, settingKey, last, running, runLabel, pendingLabel, onRun, toggle }: {
    icon: LucideIcon;
    label: string;
    settingKey: string;
    last?: string;
    running: boolean;
    runLabel: string;
    pendingLabel: string;
    onRun: () => void;
    toggle?: {
        label: string;
        enabled: boolean;
        onToggle: (checked: boolean) => void;
    };
}) {
    const t = useTranslations('setting');
    const field = useSettingField(settingKey);

    return (
        <div className="flex items-center justify-between gap-4">
            <div className="flex min-w-0 flex-col gap-1">
                <div className="flex items-center gap-3">
                    <Icon className="h-5 w-5 shrink-0 text-muted-foreground" />
                    <span className="text-sm font-medium">{label}</span>
                </div>
                {last !== undefined && (
                    <span className="ml-8 text-xs text-muted-foreground">
                        {t('syncTasks.last')}: {last}
                    </span>
                )}
            </div>
            <div className="flex shrink-0 items-center gap-2">
                {toggle ? (
                    <label className="flex items-center gap-2 text-xs text-muted-foreground" title={toggle.label}>
                        <span className="whitespace-nowrap">{toggle.label}</span>
                        <Switch checked={toggle.enabled} onCheckedChange={toggle.onToggle} aria-label={toggle.label} />
                    </label>
                ) : null}
                <Input
                    type="number"
                    min="0"
                    value={field.value}
                    onChange={(e) => field.setValue(e.target.value)}
                    onBlur={field.save}
                    placeholder={t('syncTasks.intervalPlaceholder')}
                    className="w-28 rounded-xl"
                />
                <Button variant="outline" size="sm" onClick={onRun} disabled={running} className="rounded-xl">
                    {running ? pendingLabel : runLabel}
                </Button>
            </div>
        </div>
    );
}

// 间隔 / Cron 二选一的紧凑分段按钮（视觉沿用 APIKeyExport 的 ToggleGroup，缩小以适配任务行）
function ModeToggleGroup({ value, options, onChange, title }: {
    value: 'interval' | 'cron';
    options: { value: 'interval' | 'cron'; label: string }[];
    onChange: (value: 'interval' | 'cron') => void;
    title?: string;
}) {
    return (
        <div title={title} className="inline-flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-muted/20 p-0.5">
            {options.map((opt) => (
                <button
                    key={opt.value}
                    type="button"
                    onClick={() => onChange(opt.value)}
                    aria-pressed={value === opt.value}
                    className={cn(
                        'h-7 whitespace-nowrap rounded-md px-2 text-xs transition-colors',
                        value === opt.value
                            ? 'bg-primary text-primary-foreground'
                            : 'text-muted-foreground hover:bg-muted/40 hover:text-foreground'
                    )}
                >
                    {opt.label}
                </button>
            ))}
        </div>
    );
}

// 站点全量签到：基准调度（间隔小时 / Cron）+ 手动触发。
// 随机签到账号不直接跟随基准触发，而是在基准触发后按账号随机窗口延迟执行。
function SiteCheckinTaskRow({ last, running, runLabel, pendingLabel, onRun }: {
    last: string;
    running: boolean;
    runLabel: string;
    pendingLabel: string;
    onRun: () => void;
}) {
    const t = useTranslations('setting');
    const modeField = useSettingField(SettingKey.SiteCheckinScheduleMode);
    const intervalField = useSettingField(SettingKey.SiteCheckinInterval);
    const cronField = useSettingField(SettingKey.SiteCheckinCron);
    const mode: 'interval' | 'cron' = modeField.value === 'cron' ? 'cron' : 'interval';

    const handleModeChange = (next: 'interval' | 'cron') => {
        if (next === mode) return;
        // 切换立即保存 site_checkin_schedule_mode；失败时 hook 内部回滚并提示
        modeField.commit(next);
    };

    const handleIntervalBlur = () => {
        const raw = intervalField.value.trim();
        if (raw === '') return;
        const hours = Number(raw);
        if (!Number.isInteger(hours) || hours < 1 || hours > 720) {
            toast.error(t('syncTasks.siteCheckin.intervalInvalid'));
            return;
        }
        intervalField.save();
    };

    const handleCronBlur = () => {
        const raw = cronField.value.trim();
        if (raw === '') return;
        if (raw.split(/\s+/).length !== 5) {
            toast.error(t('syncTasks.siteCheckin.cronInvalid'));
            return;
        }
        cronField.save();
    };

    return (
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between sm:gap-4">
            <div className="flex min-w-0 flex-col gap-1">
                <div className="flex items-center gap-3">
                    <CalendarCheck2 className="h-5 w-5 shrink-0 text-muted-foreground" />
                    <span className="text-sm font-medium">{t('syncTasks.siteCheckin.label')}</span>
                </div>
                <span className="ml-8 text-xs text-muted-foreground">
                    {t('syncTasks.last')}: {last}
                </span>
            </div>
            <div className="flex min-w-0 shrink-0 flex-nowrap items-center gap-2">
                <ModeToggleGroup
                    value={mode}
                    options={[
                        { value: 'interval', label: t('syncTasks.siteCheckin.modeInterval') },
                        { value: 'cron', label: t('syncTasks.siteCheckin.modeCron') },
                    ]}
                    onChange={handleModeChange}
                    title={t('syncTasks.siteCheckin.hint')}
                />
                {mode === 'interval' ? (
                    <Input
                        type="number"
                        min={1}
                        max={720}
                        value={intervalField.value}
                        onChange={(e) => intervalField.setValue(e.target.value)}
                        onBlur={handleIntervalBlur}
                        placeholder={t('syncTasks.intervalPlaceholder')}
                        className="w-24 rounded-xl sm:w-28"
                        aria-label={t('syncTasks.siteCheckin.modeInterval')}
                    />
                ) : (
                    <Input
                        type="text"
                        value={cronField.value}
                        onChange={(e) => cronField.setValue(e.target.value)}
                        onBlur={handleCronBlur}
                        placeholder={t('syncTasks.siteCheckin.cronPlaceholder')}
                        className="w-28 rounded-xl font-mono text-xs"
                        aria-label={t('syncTasks.siteCheckin.modeCron')}
                    />
                )}
                <Button variant="outline" size="sm" onClick={onRun} disabled={running} className="shrink-0 whitespace-nowrap rounded-xl">
                    {running ? pendingLabel : runLabel}
                </Button>
            </div>
        </div>
    );
}

export function SettingSyncTasks() {
    const t = useTranslations('setting');
    const tAll = useTranslations();
    const locale = useSettingStore((state) => state.locale);

    const syncChannel = useSyncChannel();
    const { data: lastSyncTime } = useLastSyncTime();
    const updatePrice = useUpdateModelPrice();
    const modelPriceSystemProxy = useSettingToggle(SettingKey.ModelPriceUseSystemProxy);
    const { data: lastUpdateTime } = useLastUpdateTime();
    const syncAllSites = useSyncAllSites();
    const checkinAllSites = useCheckinAllSites();
    const { data: lastSiteSyncTime } = useSiteLastSyncTime();
    const { data: lastSiteCheckinTime } = useSiteLastCheckinTime();

    const formatTime = (timeStr: string | undefined) => {
        if (!timeStr) return t('syncTasks.never');
        const date = new Date(timeStr);
        if (Number.isNaN(date.getTime())) return t('syncTasks.never');
        if (date.getFullYear() === 1) return t('syncTasks.never');
        return date.toLocaleString();
    };

    return (
        <SettingCard icon={CalendarSync} title={t('syncTasks.title')}>
            {/* 渠道同步 */}
            <TaskRow
                icon={RefreshCw}
                label={t('syncTasks.llmSync.label')}
                settingKey={SettingKey.SyncLLMInterval}
                last={formatTime(lastSyncTime)}
                running={syncChannel.isPending}
                runLabel={t('syncTasks.llmSync.button')}
                pendingLabel={t('syncTasks.llmSync.pending')}
                onRun={() => syncChannel.mutate(undefined, {
                    onSuccess: () => toast.success(t('syncTasks.llmSync.success')),
                    onError: () => toast.error(t('syncTasks.llmSync.failed')),
                })}
            />

            {/* 模型价格更新 */}
            <TaskRow
                icon={DollarSign}
                label={t('syncTasks.llmPrice.label')}
                settingKey={SettingKey.ModelInfoUpdateInterval}
                last={formatTime(lastUpdateTime)}
                running={updatePrice.isPending}
                runLabel={t('syncTasks.llmPrice.button')}
                pendingLabel={t('syncTasks.llmPrice.pending')}
                toggle={{
                    label: t('syncTasks.llmPrice.useSystemProxy'),
                    enabled: modelPriceSystemProxy.enabled,
                    onToggle: modelPriceSystemProxy.toggle,
                }}
                onRun={() => updatePrice.mutate(undefined, {
                    onSuccess: () => toast.success(t('syncTasks.llmPrice.success')),
                    onError: () => toast.error(t('syncTasks.llmPrice.failed')),
                })}
            />

            {/* 站点全量同步 */}
            <TaskRow
                icon={Globe2}
                label={t('syncTasks.siteSync.label')}
                settingKey={SettingKey.SiteSyncInterval}
                last={formatTime(lastSiteSyncTime)}
                running={syncAllSites.isPending}
                runLabel={t('syncTasks.siteSync.button')}
                pendingLabel={t('syncTasks.siteSync.pending')}
                onRun={() => syncAllSites.mutate(undefined, {
                    onSuccess: () => toast.success(t('syncTasks.siteSync.success')),
                    onError: (error) => toast.error(translateSiteMessage(locale, getErrorMessage(error, t('syncTasks.siteSync.failed')), tAll)),
                })}
            />

            {/* 站点全量签到 */}
            <SiteCheckinTaskRow
                last={formatTime(lastSiteCheckinTime)}
                running={checkinAllSites.isPending}
                runLabel={t('syncTasks.siteCheckin.button')}
                pendingLabel={t('syncTasks.siteCheckin.pending')}
                onRun={() => checkinAllSites.mutate(undefined, {
                    onSuccess: () => toast.success(t('syncTasks.siteCheckin.success')),
                    onError: (error) => toast.error(translateSiteMessage(locale, getErrorMessage(error, t('syncTasks.siteCheckin.failed')), tAll)),
                })}
            />
        </SettingCard>
    );
}
