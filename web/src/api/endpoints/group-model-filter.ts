import { useMemo } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../client';
import { SettingKey, useSettingList, type Setting } from './setting';
import { parseAutoGroupModelFilter, type AutoGroupModelFilter } from '@/lib/group-model-filter';

export function useGlobalAutoGroupModelFilter() {
    const query = useSettingList();
    const saved = query.data?.find((setting) => setting.key === SettingKey.GlobalAutoGroupModelFilter);
    const filter = useMemo(() => parseAutoGroupModelFilter(saved?.value), [saved?.value]);
    return { ...query, filter, supported: saved !== undefined };
}

export function useSetGlobalAutoGroupModelFilter() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (filter: AutoGroupModelFilter) => apiClient.post<Setting>('/api/v1/setting/set', {
            key: SettingKey.GlobalAutoGroupModelFilter,
            value: JSON.stringify(filter),
        }),
        onSuccess: (saved) => {
            // Editors must see the saved policy immediately, not after the next poll.
            queryClient.setQueryData<Setting[]>(['settings', 'list'], (settings) => [
                ...(settings ?? []).filter((setting) => setting.key !== saved.key),
                saved,
            ]);
            return queryClient.invalidateQueries({ queryKey: ['settings', 'list'] });
        },
    });
}
