import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../client';
import { logger } from '@/lib/logger';
import { formatCount, formatMoney, formatTime } from '@/lib/utils';
import { StatsChannel, type StatsMetricsFormatted } from './stats';
import type { ProxyMode } from './proxy-pool';
/**
 * 渠道类型枚举
 */
export enum ChannelType {
    OpenAIChat = 0,
    OpenAIResponse = 1,
    Anthropic = 2,
    Gemini = 3,
    Volcengine = 4,
    OpenAIEmbedding = 5,
}

/**
 * 自动分组类型枚举
 */
export enum AutoGroupType {
    None = 0,   // 不自动分组
    Fuzzy = 1,  // 模糊匹配
    Exact = 2,  // 准确匹配
    Regex = 3,  // 正则匹配
}

export type ChannelWSMode = 'inherit' | 'off' | 'passthrough' | 'transform';

/**
 * OpenAI 协议能力手动覆盖模式（与后端 model.OpenAIProtocolMode 对齐）
 */
export type OpenAIProtocolMode = 'auto' | 'chat_only' | 'responses_only' | 'both';

/**
 * OpenAI 协议自动探测状态（与后端 model.OpenAIProtocolCapability 对齐）
 */
export type OpenAIProtocolCapability = 'unknown' | 'supported' | 'unsupported';

/**
 * 是否为 OpenAI 协议渠道（Chat / Responses）
 */
export function isOpenAIChannelType(type: ChannelType) {
    return type === ChannelType.OpenAIChat || type === ChannelType.OpenAIResponse;
}

/**
 * 计算渠道当前生效的 Chat/Responses 协议能力。
 * auto 模式返回后端探测结果（unknown 表示尚未探测）；
 * 手动覆盖模式返回该模式下的有效结果，而非可能过期的自动探测字段。
 */
export function effectiveOpenAIProtocolCapabilities(
    channel: Pick<Channel, 'type' | 'openai_protocol_mode' | 'openai_chat_capability' | 'openai_responses_capability'>,
): { chat: OpenAIProtocolCapability; responses: OpenAIProtocolCapability } {
    if (!isOpenAIChannelType(channel.type)) {
        return { chat: 'unsupported', responses: 'unsupported' };
    }
    switch (channel.openai_protocol_mode ?? 'auto') {
        case 'chat_only':
            return { chat: 'supported', responses: 'unsupported' };
        case 'responses_only':
            return { chat: 'unsupported', responses: 'supported' };
        case 'both':
            return { chat: 'supported', responses: 'supported' };
        case 'auto':
        default:
            return {
                chat: channel.openai_chat_capability ?? 'unknown',
                responses: channel.openai_responses_capability ?? 'unknown',
            };
    }
}

/**
 * 主动协议探测请求：仅携带已保存渠道 ID，探测使用服务端权威配置（密钥不出服务端）。
 */
export type ProbeOpenAIProtocolRequest = {
    id: number;
};

export type OpenAIProtocolProbeEndpoint = 'chat' | 'responses';

export type OpenAIProtocolProbeOutcome = 'probed' | 'skipped' | 'failed';

/**
 * 单个端点（chat / responses）的安全摘要结果；UI 只展示本地化 outcome，
 * status/message 仅作信息保留，不透出到界面，避免泄露上游原始错误。
 */
export type OpenAIProtocolProbeEndpointResult = {
    endpoint: OpenAIProtocolProbeEndpoint;
    /** 探测前已记录的能力 */
    current: OpenAIProtocolCapability;
    /** 本次探测观测到的能力 */
    observed: OpenAIProtocolCapability;
    /** 探测后记录/生效的能力 */
    capability: OpenAIProtocolCapability;
    outcome: OpenAIProtocolProbeOutcome;
    status: number | null;
    message: string | null;
};

export type OpenAIProtocolProbeResult = {
    channel_id: number;
    mode: OpenAIProtocolMode;
    chat: OpenAIProtocolCapability;
    responses: OpenAIProtocolCapability;
    skipped: boolean;
    endpoints: OpenAIProtocolProbeEndpointResult[];
};

// 后端 probe 响应结构仍在演进：字段全部按可选处理并做安全默认。
type OpenAIProtocolProbeServer = Partial<Omit<OpenAIProtocolProbeResult, 'endpoints'>> & {
    endpoints?: Array<Partial<OpenAIProtocolProbeEndpointResult> & { endpoint?: string | null }> | null;
};

function normalizeProbeCapability(value: unknown): OpenAIProtocolCapability {
    return value === 'supported' || value === 'unsupported' ? value : 'unknown';
}

function normalizeProbeOutcome(value: unknown): OpenAIProtocolProbeOutcome {
    // 未知/非法 outcome 保守归一为 failed，绝不静默当作成功。
    return value === 'probed' || value === 'skipped' || value === 'failed' ? value : 'failed';
}

function normalizeProbeMode(value: unknown): OpenAIProtocolMode {
    return value === 'auto' || value === 'chat_only' || value === 'responses_only' || value === 'both'
        ? value
        : 'auto';
}

function normalizeOpenAIProtocolProbeResult(
    data: OpenAIProtocolProbeServer | null | undefined,
    channelId: number,
): OpenAIProtocolProbeResult {
    const endpoints: OpenAIProtocolProbeEndpointResult[] = [];
    for (const raw of data?.endpoints ?? []) {
        const endpointName = raw?.endpoint;
        if (endpointName !== 'chat' && endpointName !== 'responses') continue;
        const observed = normalizeProbeCapability(raw.observed);
        endpoints.push({
            endpoint: endpointName,
            current: normalizeProbeCapability(raw.current),
            observed,
            capability: normalizeProbeCapability(raw.capability ?? observed),
            outcome: normalizeProbeOutcome(raw.outcome),
            status: typeof raw.status === 'number' ? raw.status : null,
            message: typeof raw.message === 'string' ? raw.message : null,
        });
    }
    const capabilityByEndpoint = (name: OpenAIProtocolProbeEndpoint) =>
        endpoints.find((item) => item.endpoint === name)?.capability;
    // skipped 只在后端明确给出 boolean，或后端未给出且所有 endpoint 均明确
    // skipped 时为 true；空/缺失 endpoints 不能由此推导出 skipped。
    const skipped =
        typeof data?.skipped === 'boolean'
            ? data.skipped
            : endpoints.length > 0 && endpoints.every((endpoint) => endpoint.outcome === 'skipped');
    return {
        channel_id: typeof data?.channel_id === 'number' ? data.channel_id : channelId,
        mode: normalizeProbeMode(data?.mode),
        chat: normalizeProbeCapability(data?.chat ?? capabilityByEndpoint('chat')),
        responses: normalizeProbeCapability(data?.responses ?? capabilityByEndpoint('responses')),
        skipped,
        endpoints,
    };
}

export type BaseUrl = {
    url: string;
    delay: number;
};

export type CustomHeader = {
    header_key: string;
    header_value: string;
};

export type ChannelKey = {
    id: number;
    channel_id: number;
    enabled: boolean;
    channel_key: string;
    status_code: number;
    last_use_time_stamp: number;
    total_cost: number;
    remark: string;
};

export type ManagedChannelSource = {
    site_id: number;
    site_account_id: number;
    site_user_group_id?: number | null;
    group_key: string;
};

/**
 * 渠道完整数据（与后端 model.Channel 对齐；数组字段在前端保证为 []）
 */
export type Channel = {
    id: number;
    name: string;
    type: ChannelType;
    enabled: boolean;
    base_urls: BaseUrl[];
    keys: ChannelKey[];
    model: string;
    custom_model: string;
    proxy_mode: Exclude<ProxyMode, 'inherit'>;
    proxy_config_id?: number | null;
    auto_sync: boolean;
    auto_group: AutoGroupType;
    custom_header: CustomHeader[];
    ws_mode: ChannelWSMode;
    openai_protocol_mode: OpenAIProtocolMode;
    openai_chat_capability: OpenAIProtocolCapability;
    openai_responses_capability: OpenAIProtocolCapability;
    param_override?: string | null;
    match_regex?: string | null;
    managed: boolean;
    managed_source?: ManagedChannelSource | null;
    stats: StatsChannel;
};

// Internal type: backend may return null for slice fields; normalize to [] in select()
type ChannelServer = Omit<Channel, 'base_urls' | 'custom_header' | 'keys' | 'openai_protocol_mode' | 'openai_chat_capability' | 'openai_responses_capability'> & {
    base_urls: BaseUrl[] | null;
    custom_header: CustomHeader[] | null;
    keys: ChannelKey[] | null;
    openai_protocol_mode?: OpenAIProtocolMode | null;
    openai_chat_capability?: OpenAIProtocolCapability | null;
    openai_responses_capability?: OpenAIProtocolCapability | null;
};

/**
 * 创建渠道请求：必填字段 + 可选字段
 */
export type CreateChannelRequest = {
    name: string;
    type: ChannelType;
    enabled?: boolean;
    base_urls: BaseUrl[];
    keys: Array<Pick<ChannelKey, 'enabled' | 'channel_key' | 'remark'>>;
    model: string;
    custom_model?: string;
    proxy_mode?: Exclude<ProxyMode, 'inherit'>;
    proxy_config_id?: number | null;
    auto_sync?: boolean;
    auto_group?: AutoGroupType;
    custom_header?: CustomHeader[];
    ws_mode?: ChannelWSMode;
    openai_protocol_mode?: OpenAIProtocolMode;
    param_override?: string | null;
    match_regex?: string | null;
};

/**
 * 更新渠道请求：id + 可选字段 + keys diff
 */
export type UpdateChannelRequest = {
    id: number;
    name?: string;
    type?: ChannelType;
    enabled?: boolean;
    base_urls?: BaseUrl[];
    model?: string;
    custom_model?: string;
    proxy_mode?: Exclude<ProxyMode, 'inherit'>;
    proxy_config_id?: number | null;
    auto_sync?: boolean;
    auto_group?: AutoGroupType;
    custom_header?: CustomHeader[];
    ws_mode?: ChannelWSMode;
    openai_protocol_mode?: OpenAIProtocolMode;
    param_override?: string | null;
    match_regex?: string | null;
    // keys diff
    keys_to_add?: Array<Pick<ChannelKey, 'enabled' | 'channel_key' | 'remark'>>;
    keys_to_update?: Array<{ id: number; enabled?: boolean; channel_key?: string; remark?: string }>;
    keys_to_delete?: number[];
};

export type FetchModelRequest = {
    type: ChannelType;
    base_urls: BaseUrl[];
    keys: Array<Pick<ChannelKey, 'enabled' | 'channel_key'>>;
    proxy_mode?: Exclude<ProxyMode, 'inherit'>;
    proxy_config_id?: number | null;
    match_regex?: string | null;
    custom_header?: CustomHeader[];
};

/**
 * 获取渠道列表 Hook
 * 
 * @example
 * const { data: channels, isLoading, error } = useChannelList();
 * 
 * if (isLoading) return <Loading />;
 * if (error) return <Error message={error.message} />;
 * 
 * channels?.forEach(channel => console.log(channel.raw.name));
 */
export function useChannelList() {
    return useQuery({
        queryKey: ['channels', 'list'],
        queryFn: async () => {
            return apiClient.get<ChannelServer[]>('/api/v1/channel/list');
        },
        select: (data) => data.map((item) => ({
            raw: ({
                ...item,
                managed: item.managed ?? false,
                managed_source: item.managed_source ?? null,
                base_urls: item.base_urls ?? [],
                custom_header: item.custom_header ?? [],
                ws_mode: item.ws_mode ?? 'inherit',
                openai_protocol_mode: item.openai_protocol_mode ?? 'auto',
                openai_chat_capability: item.openai_chat_capability ?? 'unknown',
                openai_responses_capability: item.openai_responses_capability ?? 'unknown',
                keys: item.keys ?? [],
                proxy_mode: item.proxy_mode ?? 'direct',
                proxy_config_id: item.proxy_config_id ?? null,
            }) satisfies Channel,
            formatted: {
                input_token: formatCount(item.stats.input_token),
                output_token: formatCount(item.stats.output_token),
                total_token: formatCount(item.stats.input_token + item.stats.output_token),
                input_cost: formatMoney(item.stats.input_cost),
                output_cost: formatMoney(item.stats.output_cost),
                total_cost: formatMoney(item.stats.input_cost + item.stats.output_cost),
                request_success: formatCount(item.stats.request_success),
                request_failed: formatCount(item.stats.request_failed),
                request_count: formatCount(item.stats.request_success + item.stats.request_failed),
                wait_time: formatTime(item.stats.wait_time),
            }
        })) as Array<{ raw: Channel; formatted: StatsMetricsFormatted }>,
        refetchInterval: 30000,
    });
}

/**
 * 创建渠道 Hook
 * 
 * @example
 * const createChannel = useCreateChannel();
 * 
 * createChannel.mutate({
 *   name: 'OpenAI',
 *   type: ChannelType.OpenAIChat,
 *   base_urls: [{ url: 'https://api.openai.com', delay: 0 }],
 *   keys: [{ enabled: true, channel_key: 'sk-xxx' }],
 *   model: 'gpt-4',
 * });
 */
export function useCreateChannel() {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (data: CreateChannelRequest) => {
            return apiClient.post<ChannelServer>('/api/v1/channel/create', data);
        },
        onSuccess: (data) => {
            logger.log('渠道创建成功:', data);
            queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            queryClient.invalidateQueries({ queryKey: ['models', 'list'] });
            queryClient.invalidateQueries({ queryKey: ['models', 'channel'] });
            queryClient.invalidateQueries({ queryKey: ['proxy-pool'] });
            queryClient.invalidateQueries({ queryKey: ['groups', 'list'] });
            // 后端在创建后自动探测 OpenAI 协议能力；延迟补一次刷新兜底异步探测结果。
            setTimeout(() => {
                queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            }, 2000);
        },
        onError: (error) => {
            logger.error('渠道创建失败:', error);
        },
    });
}

/**
 * 更新渠道 Hook
 * 
 * @example
 * const updateChannel = useUpdateChannel();
 * 
 * updateChannel.mutate({
 *   id: 1,
 *   name: 'OpenAI Updated',
 *   type: ChannelType.OpenAIChat,
 *   enabled: true,
 *   base_urls: [{ url: 'https://api.openai.com', delay: 0 }],
 *   keys_to_add: [{ enabled: true, channel_key: 'sk-xxx' }],
 *   model: 'gpt-4-turbo',
 *   proxy: false,
 * });
 */
export function useUpdateChannel() {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (data: UpdateChannelRequest) => {
            return apiClient.post<ChannelServer>('/api/v1/channel/update', data);
        },
        onSuccess: (data) => {
            logger.log('渠道更新成功:', data);
            queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            queryClient.invalidateQueries({ queryKey: ['models', 'channel'] });
            queryClient.invalidateQueries({ queryKey: ['proxy-pool'] });
            queryClient.invalidateQueries({ queryKey: ['groups', 'list'] });
            // 后端在更新后自动探测 OpenAI 协议能力；延迟补一次刷新兜底异步探测结果。
            setTimeout(() => {
                queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            }, 2000);
        },
        onError: (error) => {
            logger.error('渠道更新失败:', error);
        },
    });
}

/**
 * 删除渠道 Hook
 * 
 * @example
 * const deleteChannel = useDeleteChannel();
 * 
 * deleteChannel.mutate(1); // 删除 ID 为 1 的渠道
 */
export function useDeleteChannel() {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (id: number) => {
            return apiClient.delete<null>(`/api/v1/channel/delete/${id}`);
        },
        onSuccess: () => {
            logger.log('渠道删除成功');
            queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            queryClient.invalidateQueries({ queryKey: ['models', 'channel'] });
            queryClient.invalidateQueries({ queryKey: ['proxy-pool'] });
            queryClient.invalidateQueries({ queryKey: ['groups', 'list'] });
        },
        onError: (error) => {
            logger.error('渠道删除失败:', error);
        },
    });
}

/**
 * 启用/禁用渠道 Hook
 * 
 * @example
 * const enableChannel = useEnableChannel();
 * 
 * enableChannel.mutate({ id: 1, enabled: true }); // 启用 ID 为 1 的渠道
 * enableChannel.mutate({ id: 1, enabled: false }); // 禁用 ID 为 1 的渠道
 */
export function useEnableChannel() {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (data: { id: number; enabled: boolean }) => {
            return apiClient.post<null>('/api/v1/channel/enable', data);
        },
        onSuccess: () => {
            logger.log('渠道状态更新成功');
            queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
            queryClient.invalidateQueries({ queryKey: ['models', 'channel'] });
            queryClient.invalidateQueries({ queryKey: ['groups', 'list'] });
        },
        onError: (error) => {
            logger.error('渠道状态更新失败:', error);
        },
    });
}

/**
 * 获取渠道模型列表 Hook
 * 
 * @example
 * const fetchModel = useFetchModel();
 * 
 * fetchModel.mutate({
 *   type: ChannelType.OpenAIChat,
 *   base_urls: [{ url: 'https://api.openai.com', delay: 0 }],
 *   keys: [{ enabled: true, channel_key: 'sk-xxx' }],
 *   proxy: false,
 * });
 * 
 * // 在 onSuccess 中获取模型列表
 * fetchModel.data // ['gpt-4', 'gpt-3.5-turbo', ...]
 */
export function useFetchModel() {
    return useMutation({
        mutationFn: async (data: FetchModelRequest) => {
            return apiClient.post<string[]>('/api/v1/channel/fetch-model', data);
        },
        onSuccess: (data) => {
            logger.log('模型列表获取成功:', data);
        },
        onError: (error) => {
            logger.error('模型列表获取失败:', error);
        },
    });
}

/**
 * 主动探测已保存渠道的 OpenAI 协议能力 Hook。
 *
 * 请求仅携带渠道 ID（服务端用权威配置探测，密钥不回传）；
 * 成功后刷新渠道列表，让详情/表单读到最新探测结果。
 *
 * @example
 * const probe = useProbeOpenAIProtocol();
 * probe.mutate(1);
 */
export function useProbeOpenAIProtocol() {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (id: number) => {
            const data = await apiClient.post<OpenAIProtocolProbeServer | null>(
                '/api/v1/channel/probe-openai-protocol',
                { id },
            );
            return normalizeOpenAIProtocolProbeResult(data, id);
        },
        onSuccess: (data) => {
            logger.log('OpenAI 协议探测成功:', data.channel_id);
            queryClient.invalidateQueries({ queryKey: ['channels', 'list'] });
        },
        onError: (error) => {
            logger.error('OpenAI 协议探测失败:', error);
        },
    });
}

/**
 * 获取渠道最后同步时间 Hook
 * 
 * @example
 * const lastSyncTime = useLastSyncTime();
 * 
 * if (lastSyncTime) {
 *   console.log('最后同步时间:', new Date(lastSyncTime).toLocaleString());
 * }
 */
export function useLastSyncTime() {
    return useQuery({
        queryKey: ['channels', 'last-sync-time'],
        queryFn: async () => {
            return apiClient.get<string>('/api/v1/channel/last-sync-time');
        },
        refetchInterval: 30000,
    });
}
/**
 * 同步渠道 Hook
 * 
 * @example
 * const syncChannel = useSyncChannel();
 * 
 * syncChannel.mutate();
 */
export function useSyncChannel() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async () => {
            return apiClient.post<null>('/api/v1/channel/sync');
        },
        onSuccess: () => {
            logger.log('渠道同步成功');
            queryClient.invalidateQueries({ queryKey: ['channels', 'last-sync-time'] });
        },
        onError: (error) => {
            logger.error('渠道同步失败:', error);
        },
    });
}
