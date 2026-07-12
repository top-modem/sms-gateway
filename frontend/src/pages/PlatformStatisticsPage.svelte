<script>
  import { onMount } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';

  let { onBack = () => {} } = $props();

  let items = $state([]);
  let statistics = $state([]);
  let waitList = $state([]);
  let rejectionReasons = $state([]);
  let selectedRow = $state(null);
  let selectedItem = $state(null);
  let selectedItemSms = $state([]);
  let loading = $state(true);
  let detailLoading = $state(false);
  let error = $state('');

  async function fetchData() {
    loading = true;
    error = '';
    try {
      const [itemsRes, statsRes, waitListRes, reasonsRes] = await Promise.all([
        apiClient.getFirefoxPlatformItems(),
        apiClient.getFirefoxPlatformStatistics(),
        apiClient.getFirefoxWaitList(),
        apiClient.getFirefoxPlatformRejectionReasons(),
      ]);
      items = Array.isArray(itemsRes) ? itemsRes : (itemsRes?.data ?? []);
      statistics = Array.isArray(statsRes) ? statsRes : (statsRes?.data ?? []);
      waitList = Array.isArray(waitListRes?.data?.data) ? waitListRes.data.data : [];
      rejectionReasons = Array.isArray(reasonsRes) ? reasonsRes : (reasonsRes?.data ?? []);
    } catch (e) {
      error = e?.message ?? 'Failed to load platform statistics';
    } finally {
      loading = false;
    }
  }

  function toNumber(value) {
    return Number(value || 0);
  }

  function buildRowKey(itemId, iccid, phoneNum) {
    return `${itemId}|${iccid || phoneNum || 'unknown'}`;
  }

  function findItemByPhone(phoneNum) {
    return items.find((item) => item.phone_num === phoneNum) || null;
  }

  function buildCurrentRows() {
    return waitList
      .map((entry) => {
        const itemId = String(entry?.Item_ID ?? entry?.item_id ?? '').trim();
        const phoneNum = String(entry?.Phone_Num ?? entry?.phone_num ?? '').trim();
        const countryId = String(entry?.Country_ID ?? entry?.country_id ?? '').trim();
        const cachedItem = findItemByPhone(phoneNum);
        const stat =
          statistics.find((entry) =>
            entry.item_id === itemId &&
            (
              (entry.iccid && cachedItem?.iccid && entry.iccid === cachedItem.iccid) ||
              (entry.phone_num && phoneNum && entry.phone_num === phoneNum)
            )
          ) || null;

        return {
          key: buildRowKey(itemId, cachedItem?.iccid || stat?.iccid || '', phoneNum),
          item_id: itemId || 'unknown',
          country_id: countryId || cachedItem?.country_id || stat?.country_id || '',
          phone_num: phoneNum || cachedItem?.phone_num || stat?.phone_num || '',
          iccid: cachedItem?.iccid || stat?.iccid || '',
          total_sms: toNumber(stat?.total_sms),
          uploaded_sms: toNumber(stat?.uploaded_sms),
          failed_sms: toNumber(stat?.failed_sms),
          isLegacy: false,
        };
      })
      .filter((row) => row.item_id !== 'unknown' || row.phone_num || row.iccid)
      .sort((a, b) =>
        b.failed_sms - a.failed_sms ||
        b.total_sms - a.total_sms ||
        a.phone_num.localeCompare(b.phone_num)
      );
  }

  function buildLegacyRows() {
    const currentKeys = new Set(
      currentRows.map((row) => buildRowKey(row.item_id, row.iccid, row.phone_num))
    );

    return statistics
      .filter((stat) => !currentKeys.has(buildRowKey(stat.item_id, stat.iccid, stat.phone_num)))
      .map((stat) => ({
        key: buildRowKey(stat.item_id, stat.iccid, stat.phone_num),
        item_id: stat.item_id || 'unknown',
        country_id: stat.country_id || '',
        phone_num: stat.phone_num || '',
        iccid: stat.iccid || '',
        total_sms: toNumber(stat.total_sms),
        uploaded_sms: toNumber(stat.uploaded_sms),
        failed_sms: toNumber(stat.failed_sms),
        isLegacy: true,
      }))
      .sort((a, b) =>
        b.failed_sms - a.failed_sms ||
        b.total_sms - a.total_sms ||
        a.item_id.localeCompare(b.item_id)
      );
  }

  const currentRows = $derived(buildCurrentRows());
  const legacyRows = $derived(buildLegacyRows());
  const totalAttempts = $derived(statistics.reduce((sum, stat) => sum + toNumber(stat.total_sms), 0));
  const totalSuccess = $derived(statistics.reduce((sum, stat) => sum + toNumber(stat.uploaded_sms), 0));
  const totalFailed = $derived(statistics.reduce((sum, stat) => sum + toNumber(stat.failed_sms), 0));

  async function selectRow(row) {
    if (!row) {
      selectedRow = null;
      selectedItem = null;
      selectedItemSms = [];
      return;
    }

    detailLoading = true;
    try {
      const res = await apiClient.getFirefoxPlatformItemDetail(row.item_id, row.iccid || null);
      selectedRow = row;
      selectedItem = res?.data ?? null;
      selectedItemSms = res?.data?.sms_list ?? [];
    } catch (e) {
      console.error('Failed to load item detail:', e);
      selectedRow = row;
      selectedItem = null;
      selectedItemSms = [];
    } finally {
      detailLoading = false;
    }
  }

  function formatDate(ts) {
    if (!ts) return '-';
    return new Date(ts).toLocaleString();
  }

  function getUploadStateClass(uploaded) {
    return uploaded
      ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300'
      : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300';
  }

  function getSmsPhoneNumber(sms) {
    return selectedRow?.phone_num
      || selectedItem?.items?.find((item) => item.iccid === sms.sim_id || item.sim_id === sms.sim_id)?.phone_num
      || '-';
  }

  function getRowClass(row) {
    return [
      'cursor-pointer transition-colors',
      selectedRow?.key === row.key
        ? 'bg-blue-50 dark:bg-blue-900/20'
        : 'hover:bg-gray-50 dark:hover:bg-gray-700/40',
    ].join(' ');
  }

  onMount(() => {
    fetchData();
  });
</script>

<div class="flex flex-col h-dvh w-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
  <header class="shrink-0 z-10 border-b border-gray-200 dark:border-gray-700 bg-white/90 dark:bg-gray-800/90 backdrop-blur">
    <div class="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
      <div class="flex items-center gap-3">
        <button
          onclick={onBack}
          class="rounded-lg p-2 transition-colors hover:bg-gray-100 dark:hover:bg-gray-700"
          aria-label="Back"
        >
          <Icon icon="carbon:chevron-left" class="h-5 w-5" />
        </button>
        <div>
          <h1 class="text-lg font-semibold">Platform Statistics</h1>
          <p class="text-xs text-gray-500 dark:text-gray-400">
            Current platform items are shown first. Legacy or unmapped attempts are separated below.
          </p>
        </div>
      </div>
      <button
        onclick={fetchData}
        class="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-1.5 text-sm text-white transition-colors hover:bg-blue-700"
      >
        <Icon icon="carbon:renew" class="h-4 w-4" />
        Refresh
      </button>
    </div>
  </header>

  <div class="flex-1 overflow-auto">
    <div class="mx-auto max-w-7xl p-4">
      {#if loading}
      <div class="flex h-64 items-center justify-center text-gray-500">
        <Icon icon="carbon:loading" class="mr-2 h-8 w-8 animate-spin" />
        Loading...
      </div>
    {:else if error}
      <div class="rounded-lg border border-red-200 bg-red-50 p-4 text-red-700 dark:border-red-800 dark:bg-red-900/20 dark:text-red-300">
        {error}
      </div>
    {:else}
      <div class="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <div class="rounded-xl border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-800">
          <div class="text-sm text-gray-500 dark:text-gray-400">Current Items</div>
          <div class="text-2xl font-bold">{currentRows.length}</div>
        </div>
        <div class="rounded-xl border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-800">
          <div class="text-sm text-gray-500 dark:text-gray-400">SMS Attempts</div>
          <div class="text-2xl font-bold">{totalAttempts}</div>
        </div>
        <div class="rounded-xl border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-800">
          <div class="text-sm text-gray-500 dark:text-gray-400">Successful Uploads</div>
          <div class="text-2xl font-bold text-green-600">{totalSuccess}</div>
        </div>
        <div class="rounded-xl border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-800">
          <div class="text-sm text-gray-500 dark:text-gray-400">Failed Uploads</div>
          <div class="text-2xl font-bold text-red-600">{totalFailed}</div>
        </div>
      </div>

      <section class="mb-6 overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm dark:border-gray-700 dark:bg-gray-800">
        <div class="border-b border-gray-200 px-4 py-3 dark:border-gray-700">
          <div class="font-semibold">Platform Rejection Reason Summary</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">
            Quick view of why uploads were rejected.
          </div>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full text-sm">
            <thead class="bg-gray-50 dark:bg-gray-700/50">
              <tr>
                <th class="px-4 py-2 text-left font-medium">Reason</th>
                <th class="px-4 py-2 text-right font-medium">Count</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-100 dark:divide-gray-700">
              {#each rejectionReasons as reason}
                <tr>
                  <td class="px-4 py-2">{reason.reason}</td>
                  <td class="px-4 py-2 text-right font-medium">{reason.count}</td>
                </tr>
              {:else}
                <tr>
                  <td colspan="2" class="px-4 py-4 text-center text-gray-500 dark:text-gray-400">
                    No failed rejection data yet.
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>

      <div class="grid grid-cols-1 gap-6 xl:grid-cols-[1.1fr,0.9fr]">
        <div class="space-y-6">
          <section class="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm dark:border-gray-700 dark:bg-gray-800">
            <div class="border-b border-gray-200 px-4 py-3 dark:border-gray-700">
              <div class="font-semibold">Current Platform Items</div>
              <div class="text-xs text-gray-500 dark:text-gray-400">
                Live wait-list view from the platform, enriched with local SMS attempt statistics.
              </div>
            </div>
            <div class="overflow-x-auto">
              <table class="min-w-full text-sm">
                <thead class="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th class="px-4 py-2 text-left font-medium">Item ID</th>
                    <th class="px-4 py-2 text-left font-medium">Phone</th>
                    <th class="px-4 py-2 text-left font-medium">Country</th>
                    <th class="px-4 py-2 text-left font-medium">ICCID</th>
                    <th class="px-4 py-2 text-right font-medium">Attempts</th>
                    <th class="px-4 py-2 text-right font-medium text-green-700 dark:text-green-300">Success</th>
                    <th class="px-4 py-2 text-right font-medium text-red-700 dark:text-red-300">Failed</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gray-100 dark:divide-gray-700">
                  {#each currentRows as row (row.key)}
                    <tr class={getRowClass(row)} onclick={() => selectRow(row)}>
                      <td class="px-4 py-3 font-mono">{row.item_id}</td>
                      <td class="px-4 py-3">{row.phone_num || '-'}</td>
                      <td class="px-4 py-3">{row.country_id || '-'}</td>
                      <td class="px-4 py-3 font-mono text-xs text-gray-500 dark:text-gray-400">{row.iccid || '-'}</td>
                      <td class="px-4 py-3 text-right">{row.total_sms}</td>
                      <td class="px-4 py-3 text-right text-green-600">{row.uploaded_sms}</td>
                      <td class="px-4 py-3 text-right text-red-600">{row.failed_sms}</td>
                    </tr>
                  {:else}
                    <tr>
                      <td colspan="7" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">
                        No current platform items found.
                      </td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          </section>

          {#if legacyRows.length > 0}
            <section class="overflow-hidden rounded-xl border border-amber-200 bg-white shadow-sm dark:border-amber-800 dark:bg-gray-800">
              <div class="border-b border-amber-200 px-4 py-3 dark:border-amber-800">
                <div class="flex items-center gap-2 font-semibold text-amber-700 dark:text-amber-300">
                  <Icon icon="carbon:warning-alt" class="h-4 w-4" />
                  Legacy / Unmapped Attempts
                </div>
                <div class="text-xs text-amber-700/80 dark:text-amber-300/80">
                  These rows came from historical or incomplete mappings and are separated to avoid mixing them with current items.
                </div>
              </div>
              <div class="overflow-x-auto">
                <table class="min-w-full text-sm">
                  <thead class="bg-amber-50 dark:bg-amber-900/10">
                    <tr>
                      <th class="px-4 py-2 text-left font-medium">Item ID</th>
                      <th class="px-4 py-2 text-left font-medium">Phone</th>
                      <th class="px-4 py-2 text-left font-medium">ICCID</th>
                      <th class="px-4 py-2 text-right font-medium">Attempts</th>
                      <th class="px-4 py-2 text-right font-medium text-green-700 dark:text-green-300">Success</th>
                      <th class="px-4 py-2 text-right font-medium text-red-700 dark:text-red-300">Failed</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-amber-100 dark:divide-amber-900/20">
                    {#each legacyRows as row (row.key)}
                      <tr class={getRowClass(row)} onclick={() => selectRow(row)}>
                        <td class="px-4 py-3 font-mono">{row.item_id}</td>
                        <td class="px-4 py-3">{row.phone_num || '-'}</td>
                        <td class="px-4 py-3 font-mono text-xs text-gray-500 dark:text-gray-400">{row.iccid || '-'}</td>
                        <td class="px-4 py-3 text-right">{row.total_sms}</td>
                        <td class="px-4 py-3 text-right text-green-600">{row.uploaded_sms}</td>
                        <td class="px-4 py-3 text-right text-red-600">{row.failed_sms}</td>
                      </tr>
                    {/each}
                  </tbody>
                </table>
              </div>
            </section>
          {/if}
        </div>

        <section class="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm dark:border-gray-700 dark:bg-gray-800">
          <div class="border-b border-gray-200 px-4 py-3 dark:border-gray-700">
            {#if selectedRow}
              <div class="font-semibold">SMS Attempts for Item {selectedRow.item_id}</div>
              <div class="text-xs text-gray-500 dark:text-gray-400">
                Phone: {selectedRow.phone_num || '-'} | ICCID: {selectedRow.iccid || '-'} | Attempts: {selectedRow.total_sms} | Success: {selectedRow.uploaded_sms} | Failed: {selectedRow.failed_sms}
              </div>
            {:else}
              <div class="font-semibold">SMS Attempts</div>
              <div class="text-xs text-gray-500 dark:text-gray-400">
                Select a row from the left to inspect the exact messages and responses.
              </div>
            {/if}
          </div>

          {#if detailLoading}
            <div class="flex h-64 items-center justify-center text-gray-500">
              <Icon icon="carbon:loading" class="mr-2 h-6 w-6 animate-spin" />
              Loading detail...
            </div>
          {:else if selectedRow}
            <div class="overflow-x-auto">
              <table class="min-w-full text-sm">
                <thead class="bg-gray-50 dark:bg-gray-700/50">
                  <tr>
                    <th class="px-4 py-2 text-left font-medium">Time</th>
                    <th class="px-4 py-2 text-left font-medium">Phone</th>
                    <th class="px-4 py-2 text-left font-medium">Status</th>
                    <th class="px-4 py-2 text-left font-medium">Message</th>
                    <th class="px-4 py-2 text-left font-medium">Response</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gray-100 dark:divide-gray-700">
                  {#each selectedItemSms as sms (sms.id)}
                    <tr class="align-top">
                      <td class="whitespace-nowrap px-4 py-3 text-xs text-gray-500 dark:text-gray-400">
                        {formatDate(sms.platform_uploaded_at)}
                      </td>
                      <td class="px-4 py-3">{getSmsPhoneNumber(sms)}</td>
                      <td class="px-4 py-3">
                        <span class={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${getUploadStateClass(sms.uploaded_to_platform)}`}>
                          {sms.uploaded_to_platform ? 'Success' : 'Failed'}
                        </span>
                      </td>
                      <td class="max-w-sm px-4 py-3 break-words">{sms.message}</td>
                      <td class="max-w-sm px-4 py-3 break-words text-xs text-gray-500 dark:text-gray-400">
                        {sms.platform_response || '-'}
                      </td>
                    </tr>
                  {:else}
                    <tr>
                      <td colspan="5" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">
                        No SMS attempts found for this row.
                      </td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="flex h-64 items-center justify-center px-6 text-center text-gray-500 dark:text-gray-400">
              Select a current or legacy row to inspect its SMS attempts.
            </div>
          {/if}
        </section>
      </div>
    {/if}
  </div>
  </div>
</div>
