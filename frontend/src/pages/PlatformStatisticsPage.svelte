<script>
  import { onMount } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';

  let { onBack = () => {} } = $props();

  // ── State ──────────────────────────────────────────────────────────────────
  let items = $state([]);
  let statistics = $state([]);
  let selectedItem = $state(null);
  let selectedItemSms = $state([]);
  let loading = $state(true);
  let error = $state('');

  // ── Data fetching ──────────────────────────────────────────────────────────
  async function fetchData() {
    loading = true;
    error = '';
    try {
      const [itemsRes, statsRes] = await Promise.all([
        apiClient.getFirefoxPlatformItems(),
        apiClient.getFirefoxPlatformStatistics(),
      ]);
      items = Array.isArray(itemsRes) ? itemsRes : (itemsRes?.data ?? []);
      statistics = Array.isArray(statsRes) ? statsRes : (statsRes?.data ?? []);
    } catch (e) {
      error = e?.message ?? 'Failed to load platform statistics';
    } finally {
      loading = false;
    }
  }

  async function selectItem(itemId) {
    if (!itemId) {
      selectedItem = null;
      selectedItemSms = [];
      return;
    }
    try {
      const res = await apiClient.getFirefoxPlatformItemDetail(itemId);
      selectedItem = res;
      selectedItemSms = res?.sms_list ?? [];
    } catch (e) {
      console.error('Failed to load item detail:', e);
      selectedItem = null;
      selectedItemSms = [];
    }
  }

  function formatDate(ts) {
    if (!ts) return '-';
    const d = new Date(ts);
    return d.toLocaleString();
  }

  function getStat(itemId, phoneNum) {
    return statistics.find(s => s.item_id === itemId && s.phone_num === phoneNum) || null;
  }

  onMount(() => {
    fetchData();
  });
</script>

<div class="page-container min-h-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
  <!-- Header -->
  <div class="sticky top-0 z-10 bg-white/90 dark:bg-gray-800/90 backdrop-blur border-b border-gray-200 dark:border-gray-700 px-4 py-3 flex items-center justify-between">
    <div class="flex items-center gap-3">
      <button
        onclick={onBack}
        class="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
        aria-label="Back"
      >
        <Icon icon="carbon:arrow-left" class="w-5 h-5" />
      </button>
      <div>
        <h1 class="text-lg font-semibold">Platform Statistics</h1>
        <p class="text-xs text-gray-500 dark:text-gray-400">Track SMS uploaded to 火狐狸 platform</p>
      </div>
    </div>
    <button
      onclick={fetchData}
      class="flex items-center gap-2 px-3 py-1.5 text-sm rounded-lg bg-blue-600 hover:bg-blue-700 text-white transition-colors"
    >
      <Icon icon="carbon:renew" class="w-4 h-4" />
      Refresh
    </button>
  </div>

  <!-- Content -->
  <div class="p-4 max-w-7xl mx-auto">
    {#if loading}
      <div class="flex items-center justify-center h-64 text-gray-500">
        <Icon icon="carbon:loading" class="w-8 h-8 animate-spin mr-2" />
        Loading...
      </div>
    {:else if error}
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-300">
        {error}
      </div>
    {:else}
      <!-- Summary Cards -->
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-6">
        <div class="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm border border-gray-200 dark:border-gray-700">
          <div class="text-sm text-gray-500 dark:text-gray-400">Total Items</div>
          <div class="text-2xl font-bold">{items.length}</div>
        </div>
        <div class="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm border border-gray-200 dark:border-gray-700">
          <div class="text-sm text-gray-500 dark:text-gray-400">Total SMS Uploaded</div>
          <div class="text-2xl font-bold">
            {statistics.reduce((sum, s) => sum + Number(s.total_sms || 0), 0)}
          </div>
        </div>
        <div class="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm border border-gray-200 dark:border-gray-700">
          <div class="text-sm text-gray-500 dark:text-gray-400">Successful Uploads</div>
          <div class="text-2xl font-bold text-green-600">
            {statistics.reduce((sum, s) => sum + Number(s.uploaded_sms || 0), 0)}
          </div>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Items Table -->
        <div class="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div class="px-4 py-3 border-b border-gray-200 dark:border-gray-700 font-semibold">
            Platform Items
          </div>
          <div class="overflow-x-auto max-h-[calc(100vh-20rem)] overflow-y-auto">
            <table class="w-full text-sm">
              <thead class="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
                <tr>
                  <th class="text-left px-4 py-2 font-medium">Item ID</th>
                  <th class="text-left px-4 py-2 font-medium">Country</th>
                  <th class="text-left px-4 py-2 font-medium">Phone</th>
                  <th class="text-left px-4 py-2 font-medium">Total SMS</th>
                  <th class="text-left px-4 py-2 font-medium">Success</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-100 dark:divide-gray-700">
                {#each items as item (item.id)}
                  {@const stat = getStat(item.item_id, item.phone_num)}
                  <tr
                    class={["hover:bg-gray-50 dark:hover:bg-gray-700/50 cursor-pointer transition-colors", selectedItem?.item_id === item.item_id ? 'bg-blue-50 dark:bg-blue-900/20' : ''].join(' ')}
                    onclick={() => selectItem(item.item_id)}
                  >
                    <td class="px-4 py-2.5 font-mono">{item.item_id}</td>
                    <td class="px-4 py-2.5">{item.country_id}</td>
                    <td class="px-4 py-2.5">{item.phone_num}</td>
                    <td class="px-4 py-2.5">{stat?.total_sms ?? 0}</td>
                    <td class="px-4 py-2.5 text-green-600">{stat?.uploaded_sms ?? 0}</td>
                  </tr>
                {:else}
                  <tr>
                    <td colspan="5" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">
                      No platform items yet
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        </div>

        <!-- SMS Detail -->
        <div class="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div class="px-4 py-3 border-b border-gray-200 dark:border-gray-700 font-semibold">
            {#if selectedItem}
              SMS for Item {selectedItem.item_id}
            {:else}
              SMS Detail
            {/if}
          </div>
          <div class="overflow-x-auto max-h-[calc(100vh-20rem)] overflow-y-auto">
            {#if selectedItem}
              <table class="w-full text-sm">
                <thead class="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
                  <tr>
                    <th class="text-left px-4 py-2 font-medium">Phone</th>
                    <th class="text-left px-4 py-2 font-medium">Content</th>
                    <th class="text-left px-4 py-2 font-medium">Response</th>
                    <th class="text-left px-4 py-2 font-medium">Uploaded At</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gray-100 dark:divide-gray-700">
                  {#each selectedItemSms as sms (sms.id)}
                    <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <td class="px-4 py-2.5">{selectedItem.items?.find(i => i.phone_num)?.phone_num ?? '-'}</td>
                      <td class="px-4 py-2.5 max-w-xs truncate" title={sms.message}>{sms.message}</td>
                      <td class="px-4 py-2.5 text-xs">{sms.platform_response ?? '-'}</td>
                      <td class="px-4 py-2.5 whitespace-nowrap">{formatDate(sms.platform_uploaded_at)}</td>
                    </tr>
                  {:else}
                    <tr>
                      <td colspan="4" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">
                        No SMS uploaded for this item yet
                      </td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            {:else}
              <div class="p-8 text-center text-gray-500 dark:text-gray-400">
                Select an item from the left to view its uploaded SMS
              </div>
            {/if}
          </div>
        </div>
      </div>
    {/if}
  </div>
</div>

<style>
  .page-container {
    display: flex;
    flex-direction: column;
  }
</style>
