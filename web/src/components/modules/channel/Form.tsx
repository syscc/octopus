import { ChannelType, type AutoGroupType, type Channel, type ChannelWSMode, type OpenAIProtocolCapability, type OpenAIProtocolMode, type OpenAIProtocolProbeResult, isOpenAIChannelType, useFetchModel, useProbeOpenAIProtocol } from '@/api/endpoints/channel';
import { ProxySelector } from '@/components/modules/proxy-pool/ProxySelector';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { toast } from '@/components/common/Toast';
import { useTranslations } from 'next-intl';
import { useEffect, useRef, useState } from 'react';
import { RefreshCw, X, Plus } from 'lucide-react';
import { cn } from '@/lib/utils';

const OPENAI_CHANNEL_TYPE_VALUE = 'openai';

/**
 * Displays a Chat/Responses capability and can optionally act as the editable
 * protocol selector used by the OpenAI channel form.
 */
export function ProtocolCapabilityIndicator({
    capability,
    endpoint,
    checked,
    editable = false,
    disabled = false,
    onCheckedChange,
}: {
    capability: OpenAIProtocolCapability;
    endpoint: 'OpenAI Chat' | 'OpenAI Responses';
    checked?: boolean;
    editable?: boolean;
    disabled?: boolean;
    onCheckedChange?: (checked: boolean) => void;
}) {
    const t = useTranslations('channel.form');
    const ref = useRef<HTMLInputElement>(null);
    const isChecked = checked ?? capability === 'supported';

    useEffect(() => {
        if (ref.current) {
            ref.current.indeterminate = !editable && capability === 'unknown';
        }
    }, [capability, editable]);

    const input = (
        <input
            ref={ref}
            type="checkbox"
            checked={isChecked}
            readOnly={!editable}
            disabled={disabled}
            aria-readonly={!editable}
            aria-label={`${endpoint} ${t(protocolCapabilityLabelKey(capability))}`}
            onChange={editable ? (event) => onCheckedChange?.(event.target.checked) : undefined}
            onClick={!editable ? (event) => event.preventDefault() : undefined}
            onKeyDown={!editable ? (event) => event.preventDefault() : undefined}
            className={cn(
                'size-4 shrink-0 rounded border-border bg-background accent-primary',
                editable ? 'cursor-pointer' : 'pointer-events-none cursor-default',
                disabled && 'cursor-not-allowed opacity-50',
            )}
        />
    );

    const content = (
        <>
            {input}
            <span className="font-medium text-card-foreground">{endpoint}</span>
            {t(protocolCapabilityLabelKey(capability))}
        </>
    );

    return editable ? (
        <label className="flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground">
            {content}
        </label>
    ) : (
        <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
            {content}
        </span>
    );
}

export type OpenAIProtocolProbeAction = (
    channelId: number,
    onProbed?: (result: OpenAIProtocolProbeResult) => void,
    options?: { notify?: boolean },
) => void;

/**
 * 显式重新探测已保存渠道的 OpenAI 协议能力，并统一处理 loading/成功/部分失败/失败提示。
 * 请求仅携带渠道 ID，探测使用服务端权威配置；
 * 提示只包含本地化 outcome，不透出响应中的密钥或上游原始错误。
 */
export function useOpenAIProtocolProbeAction() {
    const t = useTranslations('channel.form');
    const probe = useProbeOpenAIProtocol();

    const probeOpenAIProtocol: OpenAIProtocolProbeAction = (
        channelId,
        onProbed,
        options,
    ) => {
        if (!Number.isInteger(channelId) || channelId <= 0 || probe.isPending) return;
        const notify = options?.notify ?? true;
        probe.mutate(channelId, {
            onSuccess: (result) => {
                if (notify) {
                    const hasChatEndpoint = result.endpoints.some((endpoint) => endpoint.endpoint === 'chat');
                    const hasResponsesEndpoint = result.endpoints.some((endpoint) => endpoint.endpoint === 'responses');
                    const incomplete = result.endpoints.length !== 2 || !hasChatEndpoint || !hasResponsesEndpoint;
                    const failed = result.endpoints.filter((endpoint) => endpoint.outcome === 'failed');
                    if (result.skipped) {
                        // 后端明确跳过：这是有结论的正常状态，但不是一次成功的上游检查。
                        toast.info(t('protocolProbeSkipped'));
                    } else if (incomplete || failed.length > 0) {
                        const detail = failed
                            .map((endpoint) => `${endpoint.endpoint === 'chat' ? 'OpenAI Chat' : 'OpenAI Responses'}: ${t('protocolProbeEndpointFailed')}`);
                        if (incomplete && detail.length === 0) {
                            detail.push(t('protocolProbeEndpointFailed'));
                        }
                        toast.warning(t('protocolProbePartialFailed'), { description: detail.join('; ') });
                    } else {
                        toast.success(t('protocolProbeSuccess'));
                    }
                }
                onProbed?.(result);
            },
            onError: () => {
                // API hook 只记录安全错误；隐式探测不应打扰用户，显式探测才显示提示。
                if (notify) toast.error(t('protocolProbeFailed'));
            },
        });
    };

    return { probeOpenAIProtocol, isPending: probe.isPending };
}

// i18n keys under the `channel.form` namespace
export function protocolCapabilityLabelKey(capability: OpenAIProtocolCapability) {
    switch (capability) {
        case 'supported':
            return 'capabilitySupported';
        case 'unsupported':
            return 'capabilityUnsupported';
        default:
            return 'capabilityUnknown';
    }
}

// i18n key under the `channel.form` namespace
export function protocolModeLabelKey(mode: OpenAIProtocolMode) {
    switch (mode) {
        case 'chat_only':
            return 'protocolModeChatOnly';
        case 'responses_only':
            return 'protocolModeResponsesOnly';
        case 'both':
            return 'protocolModeBoth';
        case 'auto':
        default:
            return 'protocolModeAuto';
    }
}

export function protocolModeFromSelections(
    chatSelected: boolean,
    responsesSelected: boolean,
): OpenAIProtocolMode | null {
    // 两个都勾表示"允许两个协议，真实能力交给运行时探测"，也就是 auto。
    // 绝不能返回 both：both 是手动模式，EffectiveOpenAIProtocolCapability 会
    // 对两个协议都硬返回 supported，运行时学习的 SQL 也只认 auto，写成 both
    // 会让上游真实不支持的那个协议永远学不到，每次请求都白试一次。
    if (chatSelected && responsesSelected) return 'auto';
    if (chatSelected) return 'chat_only';
    if (responsesSelected) return 'responses_only';
    return null;
}


export interface ChannelKeyFormItem {
    id?: number;
    enabled: boolean;
    channel_key: string;
    status_code?: number;
    last_use_time_stamp?: number;
    total_cost?: number;
    remark?: string;
}

export interface ChannelFormData {
    name: string;
    type: ChannelType;
    base_urls: Channel['base_urls'];
    custom_header: Channel['custom_header'];
    ws_mode: ChannelWSMode;
    proxy_mode: Channel['proxy_mode'];
    proxy_config_id: number | null;
    param_override: string;
    keys: ChannelKeyFormItem[];
    model: string;
    custom_model: string;
    enabled: boolean;
    auto_sync: boolean;
    auto_group: AutoGroupType;
    match_regex: string;
    openai_protocol_mode: OpenAIProtocolMode;
    openai_chat_capability: OpenAIProtocolCapability;
    openai_responses_capability: OpenAIProtocolCapability;
}

export interface ChannelFormProps {
    formData: ChannelFormData;
    onFormDataChange: React.Dispatch<React.SetStateAction<ChannelFormData>>;
    onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
    isPending: boolean;
    submitText: string;
    pendingText: string;
    onCancel?: () => void;
    cancelText?: string;
    idPrefix?: string;
    persistedOpenAIProtocolMode?: OpenAIProtocolMode;
    /** 已保存渠道 ID；仅已保存渠道允许显式 re-probe，创建表单不开放探测入口 */
    persistedChannelId?: number | null;
    /** 父组件提供的共享探测动作；详情卡和编辑表单共用同一个 mutation */
    probeOpenAIProtocol?: OpenAIProtocolProbeAction;
    isOpenAIProtocolProbePending?: boolean;
}

import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
} from "@/components/ui/accordion";

export function ChannelForm({
    formData,
    onFormDataChange,
    onSubmit,
    isPending,
    submitText,
    pendingText,
    onCancel,
    cancelText,
    persistedOpenAIProtocolMode,
    persistedChannelId,
    probeOpenAIProtocol,
    isOpenAIProtocolProbePending = false,
    idPrefix = 'channel',
}: ChannelFormProps) {
    const t = useTranslations('channel.form');

    // Ensure the form always shows at least 1 row for base_urls / keys / custom_header.
    // This avoids "empty list" UI and also keeps URL + APIKEY layout consistent.
    useEffect(() => {
        if (!formData.base_urls || formData.base_urls.length === 0) {
            onFormDataChange({ ...formData, base_urls: [{ url: '', delay: 0 }] });
            return;
        }
        if (!formData.keys || formData.keys.length === 0) {
            onFormDataChange({ ...formData, keys: [{ enabled: true, channel_key: '' }] });
            return;
        }
        if (!formData.custom_header || formData.custom_header.length === 0) {
            onFormDataChange({ ...formData, custom_header: [{ header_key: '', header_value: '' }] });
        }
    }, [formData, onFormDataChange]);

    const autoModels = formData.model
        ? formData.model.split(',').map((m) => m.trim()).filter(Boolean)
        : [];
    const customModels = formData.custom_model
        ? formData.custom_model.split(',').map((m) => m.trim()).filter(Boolean)
        : [];
    const [inputValue, setInputValue] = useState('');
    const inputRef = useRef<HTMLInputElement>(null);
    const lastOpenAITypeRef = useRef<ChannelType | null>(
        isOpenAIChannelType(formData.type) ? formData.type : null,
    );

    const isOpenAIChannel = isOpenAIChannelType(formData.type);

    // Switching a persisted manual override back to auto resets learned
    // capabilities on the server, so reflect that pending reset before save.
    const returningToAuto =
        (formData.openai_protocol_mode ?? 'auto') === 'auto' &&
        (persistedOpenAIProtocolMode ?? formData.openai_protocol_mode ?? 'auto') !== 'auto';
    const previewChatCapability: OpenAIProtocolCapability = returningToAuto
        ? 'unknown'
        : (formData.openai_chat_capability ?? 'unknown');
    const previewResponsesCapability: OpenAIProtocolCapability = returningToAuto
        ? 'unknown'
        : (formData.openai_responses_capability ?? 'unknown');

    // Manual mode is represented by the selected protocol checkboxes. The
    // backend already persists the equivalent enum, so the capability columns
    // remain server-owned observations.
    const manualMode = formData.openai_protocol_mode ?? 'auto';
    // 勾选状态：手动模式按模式本身决定；auto 模式下两个协议都允许，但被运行时
    // 证伪（unsupported）的不勾——跟站点渠道页保持一致，界面上的勾就是
    // "网关实测支持、或尚未证伪"的意思。历史遗留的 both 也走这条分支。
    const selectedChatProtocol =
        manualMode === 'chat_only'
            ? true
            : manualMode === 'responses_only'
                ? false
                : previewChatCapability !== 'unsupported';
    const selectedResponsesProtocol =
        manualMode === 'responses_only'
            ? true
            : manualMode === 'chat_only'
                ? false
                : previewResponsesCapability !== 'unsupported';

    const handleProtocolSelection = (endpoint: 'chat' | 'responses', checked: boolean) => {
        const nextChat = endpoint === 'chat' ? checked : selectedChatProtocol;
        const nextResponses = endpoint === 'responses' ? checked : selectedResponsesProtocol;
        const nextMode = protocolModeFromSelections(nextChat, nextResponses);
        // 至少留一个协议。不弹 toast，静默忽略这次点击。
        if (!nextMode) return;
        onFormDataChange((previous) => ({
            ...previous,
            openai_protocol_mode: nextMode,
        }));
    };

    // Explicit re-probe is available only for a saved OpenAI channel in auto mode.
    const canProbeOpenAIProtocol =
        isOpenAIChannel &&
        !returningToAuto &&
        (formData.openai_protocol_mode ?? 'auto') === 'auto' &&
        typeof persistedChannelId === 'number' &&
        persistedChannelId > 0 &&
        probeOpenAIProtocol != null;

    const handleProbeOpenAIProtocol = () => {
        if (!canProbeOpenAIProtocol || persistedChannelId == null || !probeOpenAIProtocol) return;
        probeOpenAIProtocol(persistedChannelId, (result) => {
            // Update only the automatic preview; a manual choice made in flight remains authoritative.
            onFormDataChange((prev) => {
                if ((prev.openai_protocol_mode ?? 'auto') !== 'auto' || !isOpenAIChannelType(prev.type)) return prev;
                return {
                    ...prev,
                    openai_chat_capability: result.chat,
                    openai_responses_capability: result.responses,
                };
            });
        });
    };

    // Keep the last concrete OpenAI storage value while the user temporarily
    // inspects another provider. This lets an existing type=1 channel return
    // to OpenAI without being silently normalized to type=0.
    useEffect(() => {
        if (isOpenAIChannel) {
            lastOpenAITypeRef.current = formData.type;
        }
    }, [formData.type, isOpenAIChannel]);

    const fetchModel = useFetchModel();

    const effectiveKey =
        formData.keys.find((k) => k.enabled && k.channel_key.trim())?.channel_key.trim() || '';

    const updateModels = (nextAuto: string[], nextCustom: string[]) => {
        const model = nextAuto.join(',');
        const custom_model = nextCustom.join(',');
        if (formData.model === model && formData.custom_model === custom_model) return;
        onFormDataChange({ ...formData, model, custom_model });
    };

    const handleRefreshModels = async () => {
        if (!formData.base_urls?.[0]?.url || !effectiveKey) return;
        fetchModel.mutate(
            {
                type: formData.type,
                base_urls: formData.base_urls,
                keys: formData.keys
                    .filter((k) => k.channel_key.trim())
                    .map((k) => ({ enabled: k.enabled, channel_key: k.channel_key.trim() })),
                proxy_mode: formData.proxy_mode,
                proxy_config_id: formData.proxy_mode === 'pool' ? formData.proxy_config_id : null,
                match_regex: formData.match_regex.trim() || null,
                custom_header: formData.custom_header?.filter((h) => h.header_key.trim()) || [],
            },
            {
                onSuccess: (data) => {
                    if (data && data.length > 0) {
                        const nextAuto = Array.from(new Set([...autoModels, ...data].map((m) => m.trim()).filter(Boolean)));
                        updateModels(nextAuto, customModels);
                        toast.success(t('modelRefreshSuccess'));
                    } else {
                        toast.warning(t('modelRefreshEmpty'));
                    }
                },
                onError: (error) => {
                    const errorMessage = error instanceof Error ? error.message : String(error);
                    toast.error(t('modelRefreshFailed'), { description: errorMessage });
                },
            }
        );
    };

    const handleAddModel = (model: string) => {
        const trimmedModel = model.trim();
        if (trimmedModel && !customModels.includes(trimmedModel) && !autoModels.includes(trimmedModel)) {
            updateModels(autoModels, [...customModels, trimmedModel]);
        }
        setInputValue('');
    };

    const handleRemoveAutoModel = (model: string) => {
        updateModels(autoModels.filter(m => m !== model), customModels);
    };

    const handleRemoveCustomModel = (model: string) => {
        updateModels(autoModels, customModels.filter(m => m !== model));
    };

    const handleInputKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            if (inputValue.trim()) handleAddModel(inputValue);
        }
    };

    const handleAddKey = () => {
        onFormDataChange({
            ...formData,
            keys: [...formData.keys, { enabled: true, channel_key: '' }],
        });
    };

    const handleUpdateKey = (idx: number, patch: Partial<ChannelKeyFormItem>) => {
        const next = formData.keys.map((k, i) => (i === idx ? { ...k, ...patch } : k));
        onFormDataChange({ ...formData, keys: next });
    };

    const handleRemoveKey = (idx: number) => {
        const curr = formData.keys ?? [];
        if (curr.length <= 1) return;
        const next = curr.filter((_, i) => i !== idx);
        onFormDataChange({ ...formData, keys: next });
    };

    const handleAddBaseUrl = () => {
        onFormDataChange({
            ...formData,
            base_urls: [...(formData.base_urls ?? []), { url: '', delay: 0 }],
        });
    };

    const handleUpdateBaseUrl = (idx: number, patch: Partial<Channel['base_urls'][number]>) => {
        const next = (formData.base_urls ?? []).map((u, i) => (i === idx ? { ...u, ...patch } : u));
        onFormDataChange({ ...formData, base_urls: next });
    };

    const handleRemoveBaseUrl = (idx: number) => {
        const curr = formData.base_urls ?? [];
        if (curr.length <= 1) return;
        onFormDataChange({ ...formData, base_urls: curr.filter((_, i) => i !== idx) });
    };

    const handleAddHeader = () => {
        onFormDataChange({
            ...formData,
            custom_header: [...(formData.custom_header ?? []), { header_key: '', header_value: '' }],
        });
    };

    const handleUpdateHeader = (idx: number, patch: Partial<Channel['custom_header'][number]>) => {
        const next = (formData.custom_header ?? []).map((h, i) => (i === idx ? { ...h, ...patch } : h));
        onFormDataChange({ ...formData, custom_header: next });
    };

    const handleRemoveHeader = (idx: number) => {
        const curr = formData.custom_header ?? [];
        if (curr.length <= 1) return;
        onFormDataChange({ ...formData, custom_header: curr.filter((_, i) => i !== idx) });
    };

    return (
        <form onSubmit={onSubmit} className="space-y-4 px-1">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                    <label htmlFor={`${idPrefix}-name`} className="text-sm font-medium text-card-foreground">
                        {t('name')}
                    </label>
                    <Input
                        className='rounded-xl'
                        id={`${idPrefix}-name`}
                        type="text"
                        value={formData.name}
                        onChange={(event) => onFormDataChange({ ...formData, name: event.target.value })}
                        required
                    />
                </div>

                <div className="space-y-2">
                    <label htmlFor={`${idPrefix}-type`} className="text-sm font-medium text-card-foreground">
                        {t('type')}
                    </label>
                    <Select
                        value={isOpenAIChannel ? OPENAI_CHANNEL_TYPE_VALUE : String(formData.type)}
                        onValueChange={(value) => {
                            if (value === OPENAI_CHANNEL_TYPE_VALUE) {
                                const type = lastOpenAITypeRef.current ?? ChannelType.OpenAIChat;
                                onFormDataChange({ ...formData, type });
                                return;
                            }
                            if (isOpenAIChannel) {
                                lastOpenAITypeRef.current = formData.type;
                            }
                            onFormDataChange({ ...formData, type: Number(value) as ChannelType });
                        }}
                    >
                        <SelectTrigger id={`${idPrefix}-type`} className="rounded-xl w-full border border-border px-4 py-2 text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent className='rounded-xl'>
                            <SelectItem className='rounded-xl' value={OPENAI_CHANNEL_TYPE_VALUE}>{t('typeOpenAI')}</SelectItem>
                            <SelectItem className='rounded-xl' value={String(ChannelType.Anthropic)}>{t('typeAnthropic')}</SelectItem>
                            <SelectItem className='rounded-xl' value={String(ChannelType.Gemini)}>{t('typeGemini')}</SelectItem>
                            <SelectItem className='rounded-xl' value={String(ChannelType.Volcengine)}>{t('typeVolcengine')}</SelectItem>
                            <SelectItem className='rounded-xl' value={String(ChannelType.OpenAIEmbedding)}>{t('typeOpenAIEmbedding')}</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
            </div>

            {isOpenAIChannel ? (
                <div className="space-y-2">
                    <div className="flex items-center justify-between gap-2">
                        <label className="text-sm font-medium text-card-foreground">
                            {t('protocolMode')}
                        </label>
                        {canProbeOpenAIProtocol ? (
                            <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={handleProbeOpenAIProtocol}
                                disabled={isOpenAIProtocolProbePending}
                                className="h-6 px-2 text-xs text-muted-foreground/50 hover:text-muted-foreground hover:bg-transparent"
                            >
                                <RefreshCw className={cn('h-3 w-3 mr-1', isOpenAIProtocolProbePending && 'animate-spin')} />
                                {isOpenAIProtocolProbePending ? t('protocolProbing') : t('protocolReprobe')}
                            </Button>
                        ) : null}
                    </div>
                    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 rounded-xl border border-border/70 bg-muted/20 px-3 py-2">
                        <ProtocolCapabilityIndicator
                            capability={previewChatCapability}
                            endpoint="OpenAI Chat"
                            checked={selectedChatProtocol}
                            editable
                            onCheckedChange={(checked) => handleProtocolSelection('chat', checked)}
                        />
                        <ProtocolCapabilityIndicator
                            capability={previewResponsesCapability}
                            endpoint="OpenAI Responses"
                            checked={selectedResponsesProtocol}
                            editable
                            onCheckedChange={(checked) => handleProtocolSelection('responses', checked)}
                        />
                    </div>
                </div>
            ) : null}

            <div className="space-y-2">
                <div className="flex items-center justify-between">
                    <label className="text-sm font-medium text-card-foreground">
                        {t('baseUrls')} {formData.base_urls.length > 0 ? `(${formData.base_urls.length})` : ''}
                    </label>
                    <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        onClick={handleAddBaseUrl}
                        className="h-6 px-2 text-xs text-muted-foreground/70 hover:text-muted-foreground hover:bg-transparent"
                    >
                        <Plus className="h-3 w-3 mr-1" />
                        {t('add')}
                    </Button>
                </div>
                <div className="space-y-2">
                    {(formData.base_urls ?? []).map((u, idx) => (
                        <div key={`baseurl-${idx}`} className="flex items-center gap-2">
                            <Input
                                id={`${idPrefix}-base-${idx}`}
                                type="url"
                                value={u.url}
                                onChange={(e) => handleUpdateBaseUrl(idx, { url: e.target.value })}
                                placeholder={t('baseUrlUrl')}
                                required={idx === 0}
                                className="rounded-xl flex-1"
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() => handleRemoveBaseUrl(idx)}
                                disabled={(formData.base_urls ?? []).length <= 1}
                                className="h-8 w-8 p-0 rounded-xl text-muted-foreground hover:text-destructive disabled:opacity-40 hover:bg-transparent"
                                title="Remove"
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                </div>
            </div>

            <div className="space-y-2">
                <div className="flex items-center justify-between">
                    <label className="text-sm font-medium text-card-foreground">
                        {t('apiKey')} {formData.keys.length > 0 ? `(${formData.keys.length})` : ''}
                    </label>
                    <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        onClick={handleAddKey}
                        className="h-6 px-2 text-xs text-muted-foreground/70 hover:text-muted-foreground hover:bg-transparent"
                    >
                        <Plus className="h-3 w-3 mr-1" />
                        {t('add')}
                    </Button>
                </div>
                <div className="space-y-2">
                    {(formData.keys ?? []).map((k, idx) => (
                        <div key={k.id ?? `new-${idx}`} className="flex items-center gap-2">
                            <Input
                                type="text"
                                value={k.channel_key}
                                onChange={(e) => handleUpdateKey(idx, { channel_key: e.target.value })}
                                placeholder={t('apiKey')}
                                required={idx === 0}
                                className="rounded-xl flex-1"
                            />
                            <Input
                                type="text"
                                value={k.remark ?? ''}
                                onChange={(e) => handleUpdateKey(idx, { remark: e.target.value })}
                                placeholder={t('remark')}
                                className="rounded-xl w-32"
                            />
                            <Switch
                                checked={k.enabled}
                                onCheckedChange={(checked) => handleUpdateKey(idx, { enabled: checked })}
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() => handleRemoveKey(idx)}
                                disabled={(formData.keys ?? []).length <= 1}
                                className="h-8 w-8 p-0 rounded-xl text-muted-foreground hover:text-destructive hover:bg-transparent disabled:opacity-40"
                                title="Remove"
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                </div>
            </div>

            <div className="space-y-2">
                <div className="flex items-center justify-between">
                    <label className="text-sm font-medium text-card-foreground">{t('model')}</label>
                    <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        onClick={handleRefreshModels}
                        disabled={!formData.base_urls?.[0]?.url || !effectiveKey || fetchModel.isPending}
                        className="h-6 px-2 text-xs text-muted-foreground/50 hover:text-muted-foreground hover:bg-transparent"
                    >
                        <RefreshCw className={`h-3 w-3 mr-1 ${fetchModel.isPending ? 'animate-spin' : ''}`} />
                        {t('modelRefresh')}
                    </Button>
                </div>
                <input type="hidden" value={formData.model} required />

                <div className="relative">
                    <Input
                        ref={inputRef}
                        id={`${idPrefix}-model-custom`}
                        type="text"
                        value={inputValue}
                        onChange={(e) => setInputValue(e.target.value)}
                        onKeyDown={handleInputKeyDown}
                        placeholder={t('modelCustomPlaceholder')}
                        className="pr-10 rounded-xl"
                    />
                    {inputValue.trim() && !customModels.includes(inputValue.trim()) && !autoModels.includes(inputValue.trim()) && (
                        <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            onClick={() => handleAddModel(inputValue)}
                            className="absolute rounded-lg right-1 top-1/2 -translate-y-1/2 h-7 w-7 p-0 text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors"
                            title={t('modelAdd')}
                        >
                            <Plus className="size-4" />
                        </Button>
                    )}
                </div>

                <div className="space-y-2">
                    <div className="flex items-center justify-between">
                        <label className="text-xs font-medium text-card-foreground">
                            {t('modelSelected')} {(autoModels.length + customModels.length) > 0 && `(${autoModels.length + customModels.length})`}
                        </label>
                        {(autoModels.length + customModels.length) > 0 && (
                            <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() => {
                                    updateModels([], []);
                                }}
                                className="h-6 px-2 text-xs text-muted-foreground/50 hover:text-muted-foreground hover:bg-transparent"
                            >
                                {t('modelClearAll')}
                            </Button>
                        )}
                    </div>
                    <div className="rounded-xl border border-border bg-muted/30 p-2.5 max-h-40 min-h-12 overflow-y-auto">
                        {(autoModels.length + customModels.length) > 0 ? (
                            <div className="flex flex-wrap gap-1.5">
                                {autoModels.map((model) => (
                                    <Badge key={model} variant="secondary" className="bg-muted hover:bg-muted/80">
                                        {model}
                                        <button
                                            type="button"
                                            onClick={() => handleRemoveAutoModel(model)}
                                            className="ml-1 rounded-sm opacity-70 hover:opacity-100 focus:outline-none focus:ring-1 focus:ring-ring"
                                        >
                                            <X className="h-3 w-3" />
                                        </button>
                                    </Badge>
                                ))}
                                {customModels.map((model) => (
                                    <Badge key={model} className="bg-primary hover:bg-primary/90">
                                        {model}
                                        <button
                                            type="button"
                                            onClick={() => handleRemoveCustomModel(model)}
                                            className="ml-1 rounded-sm opacity-70 hover:opacity-100 focus:outline-none focus:ring-1 focus:ring-ring"
                                        >
                                            <X className="h-3 w-3" />
                                        </button>
                                    </Badge>
                                ))}
                            </div>
                        ) : (
                            <div className="flex items-center justify-center h-8 text-xs text-muted-foreground">
                                {t('modelNoSelected')}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            <div className="rounded-xl border bg-card p-4">
                <ProxySelector
                    value={{ proxy_mode: formData.proxy_mode, proxy_config_id: formData.proxy_config_id }}
                    onChange={(next) => onFormDataChange({
                        ...formData,
                        proxy_mode: next.proxy_mode as Channel['proxy_mode'],
                        proxy_config_id: next.proxy_config_id ?? null,
                    })}
                />
            </div>

            <Accordion type="single" collapsible className="w-full border rounded-xl bg-card">
                <AccordionItem value="advanced" className="border-none">
                    <AccordionTrigger className="text-sm font-medium text-card-foreground py-3 px-4 hover:no-underline hover:bg-muted/30 rounded-xl transition-colors">
                        {t('advanced')}
                    </AccordionTrigger>
                    <AccordionContent className="pt-4 px-4 pb-4 space-y-4 border-t">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            {isOpenAIChannel ? (
                                <div className="space-y-2">
                                    <label htmlFor={`${idPrefix}-ws-mode`} className="text-sm font-medium text-card-foreground">
                                        {t('wsMode')}
                                    </label>
                                    <Select
                                        value={formData.ws_mode ?? 'inherit'}
                                        onValueChange={(value) => onFormDataChange({ ...formData, ws_mode: value as ChannelWSMode })}
                                    >
                                        <SelectTrigger id={`${idPrefix}-ws-mode`} className="rounded-xl w-full border border-border px-4 py-2 text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                                            <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent className='rounded-xl'>
                                            <SelectItem className='rounded-xl' value="inherit">{t('wsModeInherit')}</SelectItem>
                                            <SelectItem className='rounded-xl' value="passthrough">{t('wsModePassthrough')}</SelectItem>
                                            <SelectItem className='rounded-xl' value="transform">{t('wsModeTransform')}</SelectItem>
                                            <SelectItem className='rounded-xl' value="off">{t('wsModeOff')}</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                            ) : null}

                        </div>

                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <label className="text-sm font-medium text-card-foreground">
                                    {t('customHeader')} {formData.custom_header.length > 0 ? `(${formData.custom_header.length})` : ''}
                                </label>
                                <Button
                                    type="button"
                                    variant="ghost"
                                    size="sm"
                                    onClick={handleAddHeader}
                                    className="h-6 px-2 text-xs text-muted-foreground/70 hover:text-muted-foreground hover:bg-transparent"
                                >
                                    <Plus className="h-3 w-3 mr-1" />
                                    {t('customHeaderAdd')}
                                </Button>
                            </div>
                            <div className="space-y-2">
                                {(formData.custom_header ?? []).map((h, idx) => (
                                    <div key={`hdr-${idx}`} className="flex items-center gap-2">
                                        <Input
                                            type="text"
                                            value={h.header_key}
                                            onChange={(e) => handleUpdateHeader(idx, { header_key: e.target.value })}
                                            placeholder={t('customHeaderKey')}
                                            className="rounded-xl flex-1"
                                        />
                                        <Input
                                            type="text"
                                            value={h.header_value}
                                            onChange={(e) => handleUpdateHeader(idx, { header_value: e.target.value })}
                                            placeholder={t('customHeaderValue')}
                                            className="rounded-xl flex-1"
                                        />
                                        <Button
                                            type="button"
                                            variant="ghost"
                                            size="sm"
                                            onClick={() => handleRemoveHeader(idx)}
                                            disabled={(formData.custom_header ?? []).length <= 1}
                                            className="h-8 w-8 p-0 rounded-xl text-muted-foreground hover:text-destructive hover:bg-transparent disabled:opacity-40"
                                            title="Remove"
                                        >
                                            <X className="h-4 w-4" />
                                        </Button>
                                    </div>
                                ))}
                            </div>
                        </div>

                        <div className="space-y-2">
                            <label htmlFor={`${idPrefix}-match-regex`} className="text-sm font-medium text-card-foreground">
                                {t('matchRegex')}
                            </label>
                            <Input
                                id={`${idPrefix}-match-regex`}
                                type="text"
                                value={formData.match_regex}
                                onChange={(e) => onFormDataChange({ ...formData, match_regex: e.target.value })}
                                placeholder={t('matchRegexPlaceholder')}
                                className="rounded-xl"
                            />
                        </div>

                        <div className="space-y-2">
                            <label htmlFor={`${idPrefix}-param-override`} className="text-sm font-medium text-card-foreground">
                                {t('paramOverride')}
                            </label>
                            <textarea
                                id={`${idPrefix}-param-override`}
                                value={formData.param_override}
                                onChange={(e) => onFormDataChange({ ...formData, param_override: e.target.value })}
                                placeholder={t('paramOverridePlaceholder')}
                                className="min-h-28 w-full rounded-xl border border-border bg-background px-3 py-2 text-sm text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                            />
                        </div>
                    </AccordionContent>
                </AccordionItem>
            </Accordion>

            <div className="flex flex-wrap items-center justify-between gap-4 p-4 rounded-xl bg-muted/20 border border-border/50">
                <label className="flex items-center gap-2 cursor-pointer">
                    <Switch
                        checked={formData.enabled}
                        onCheckedChange={(checked) => onFormDataChange({ ...formData, enabled: checked })}
                    />
                    <span className="text-sm font-medium text-card-foreground">{t('enabled')}</span>
                </label>
                <div className="flex items-center gap-6">
                    <label className="flex items-center gap-2 cursor-pointer">
                        <Switch
                            checked={formData.auto_sync}
                            onCheckedChange={(checked) => onFormDataChange({ ...formData, auto_sync: checked })}
                        />
                        <span className="text-sm text-card-foreground">{t('autoSync')}</span>
                    </label>
                </div>
            </div>

            <div className={`flex flex-col gap-3 pt-2 ${onCancel ? 'sm:flex-row' : ''}`}>
                {onCancel && cancelText && (
                    <Button
                        type="button"
                        variant="secondary"
                        onClick={onCancel}
                        className="w-full sm:flex-1 rounded-2xl h-12"
                    >
                        {cancelText}
                    </Button>
                )}
                <Button
                    type="submit"
                    disabled={isPending}
                    className="w-full sm:flex-1 rounded-2xl h-12"
                >
                    {isPending ? pendingText : submitText}
                </Button>
            </div>
        </form>
    );
}
