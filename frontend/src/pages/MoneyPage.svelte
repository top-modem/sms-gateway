<script>
  import { onMount, onDestroy } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();

  let rows = $state([]);
  let loading = $state(true);
  let error = $state('');
  let itemOptions = $state([]);
  let itemKeyword = $state('');
  let selectedItemId = $state('');
  let selectedItemName = $state('');
  let unitPriceInput = $state('');
  let selectedSuccessCount = $state(0);
  let selectedEarningLoading = $state(false);

  let pollTimer;

  function formatMoney(value) {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '0';
    return num.toFixed(2).replace(/\.00$/, '');
  }

  function parseUnitPrice(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return null;
    return n;
  }

  const selectedUnitPrice = $derived(parseUnitPrice(unitPriceInput));
  const selectedEarning = $derived(
    selectedUnitPrice == null ? 0 : selectedSuccessCount * selectedUnitPrice
  );

  async function fetchData() {
    try {
      error = '';
      const data = await apiClient.getFirefoxMoneyStats();
      rows = Array.isArray(data) ? data : (data?.data ?? []);
    } catch (e) {
      error = e?.message ?? $t('err_load_money_stats');
    } finally {
      loading = false;
    }
  }

  async function fetchItemOptions(keyword = '') {
    try {
      const data = await apiClient.getFirefoxMoneyItems(keyword, 300);
      itemOptions = Array.isArray(data) ? data : (data?.data ?? []);
    } catch (e) {
      console.warn('Failed to load money item options:', e);
      itemOptions = [];
    }
  }

  async function fetchSelectedItemEarning() {
    selectedSuccessCount = 0;
    if (!selectedItemId) return;

    selectedEarningLoading = true;
    try {
      const data = await apiClient.getFirefoxMoneyItemEarning(selectedItemId);
      const payload = data?.data ?? data;
      selectedSuccessCount = Number(payload?.success_count ?? 0);
      if (!Number.isFinite(selectedSuccessCount)) {
        selectedSuccessCount = 0;
      }
    } catch (e) {
      console.warn('Failed to load selected item earning stats:', e);
      selectedSuccessCount = 0;
    } finally {
      selectedEarningLoading = false;
    }
  }

  function onChooseItem(event) {
    const itemId = event.currentTarget?.value || '';
    selectedItemId = itemId;
    const item = itemOptions.find((opt) => opt.item_id === itemId);
    selectedItemName = item?.item_name || '';
    if (!unitPriceInput && item?.item_uprice != null) {
      unitPriceInput = String(item.item_uprice);
    }
    fetchSelectedItemEarning();
  }

  function onItemKeywordInput(event) {
    itemKeyword = event.currentTarget?.value || '';
    fetchItemOptions(itemKeyword);
  }

  onMount(async () => {
    await Promise.all([fetchData(), fetchItemOptions('')]);
    pollTimer = setInterval(fetchData, 5000);
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
  });
</script>

<div class="flex h-dvh w-screen flex-col bg-[#f2f2f2] text-gray-900 dark:bg-zinc-900 dark:text-gray-100">
  <header class="flex items-center justify-between border-b border-gray-200 bg-white px-3 py-2 shadow-sm dark:border-zinc-700 dark:bg-zinc-800">
    <div class="flex items-center gap-2.5">
      <button
        onclick={onBack}
        class="inline-flex items-center gap-1.5 h-8 px-2.5 rounded-full bg-blue-600 text-white shadow-sm shadow-blue-600/30 hover:bg-blue-700 transition"
      >
        <Icon icon="carbon:arrow-left" class="h-3.5 w-3.5" />
        <span class="text-xs font-semibold">{$t('btn_back')}</span>
      </button>
      <h1 class="text-sm font-semibold tracking-wide">{$t('money_page_title')}</h1>
    </div>

    <button
      onclick={fetchData}
      class="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-blue-700 transition"
    >
      <Icon icon="carbon:renew" class="h-3.5 w-3.5" />
      {$t('btn_refresh')}
    </button>
  </header>

  <div class="flex-1 overflow-auto p-0">
    {#if loading}
      <div class="flex h-48 items-center justify-center text-gray-500 dark:text-gray-400">
        <Icon icon="carbon:loading" class="mr-2 h-6 w-6 animate-spin" />
        {$t('loading')}
      </div>
    {:else if error}
      <div class="rounded-lg border border-red-300 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-800 dark:bg-red-900/20 dark:text-red-300">
        {error}
      </div>
    {:else}
      <section class="border-b border-gray-300 bg-white px-3 py-3 dark:border-zinc-700 dark:bg-zinc-800">
        <div class="max-w-xl overflow-hidden rounded border border-gray-300 dark:border-zinc-600">
          <div class="grid grid-cols-[88px,1fr] border-b border-gray-300 dark:border-zinc-600">
            <label for="money-item-select" class="flex items-center bg-gray-100 px-3 py-2 text-sm font-medium text-gray-700 dark:bg-zinc-700 dark:text-gray-200">
              {$t('money_item_label')}
            </label>
            <div class="p-2">
              <input
                type="text"
                class="mb-2 w-full rounded border border-gray-300 px-2 py-1.5 text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
                placeholder={$t('money_item_search_placeholder')}
                value={itemKeyword}
                oninput={onItemKeywordInput}
              />
              <select
                id="money-item-select"
                class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
                value={selectedItemId}
                onchange={onChooseItem}
              >
                <option value="">{$t('money_item_select_placeholder')}</option>
                {#each itemOptions as opt}
                  <option value={opt.item_id}>
                    {opt.item_id} | {opt.item_name}
                  </option>
                {/each}
              </select>
            </div>
          </div>
          <div class="grid grid-cols-[88px,1fr]">
            <label for="money-unit-price" class="flex items-center bg-gray-100 px-3 py-2 text-sm font-medium text-gray-700 dark:bg-zinc-700 dark:text-gray-200">
              {$t('money_unit_price_label')}
            </label>
            <div class="p-2">
              <input
                id="money-unit-price"
                type="number"
                min="0"
                step="0.01"
                class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
                placeholder={$t('money_unit_price_placeholder')}
                bind:value={unitPriceInput}
              />
            </div>
          </div>
        </div>

        {#if selectedItemId}
          <div class="mt-3 rounded border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-800 dark:border-blue-700 dark:bg-blue-900/25 dark:text-blue-200">
            <div class="font-semibold">
              {$t('money_selected_item_result_title')}
            </div>
            <div class="mt-1 text-xs">
              {$t('money_selected_item_result_item')}: {selectedItemId}{selectedItemName ? ` | ${selectedItemName}` : ''}
            </div>
            <div class="mt-1 text-xs">
              {$t('money_selected_item_result_upload_count')}: {selectedEarningLoading ? '...' : selectedSuccessCount}
            </div>
            <div class="mt-1 text-sm font-bold text-[#10a248] dark:text-green-300">
              {$t('money_selected_item_result_earning')}: {formatMoney(selectedEarning)}
            </div>
          </div>
        {/if}
      </section>

      <section class="overflow-hidden border-y border-gray-300 bg-white dark:border-zinc-700 dark:bg-zinc-800">
        <div class="overflow-x-auto">
          <table class="money-table min-w-full text-[13px] leading-none">
            <colgroup>
              <col class="col-port" />
              <col class="col-phone" />
              <col class="col-wait" />
              <col class="col-recv" />
              <col class="col-up" />
              <col class="col-fail" />
              <col class="col-profit" />
              <col class="col-items" />
            </colgroup>
            <thead class="bg-[#17add4] text-white">
              <tr>
                <th class="px-3 py-2.5 text-left font-semibold">{$t('money_col_port')}</th>
                <th class="px-3 py-2.5 text-left font-semibold">{$t('money_col_phone')}</th>
                <th class="px-3 py-2.5 text-center font-semibold">{$t('money_col_waiting')}</th>
                <th class="px-3 py-2.5 text-center font-semibold">{$t('money_col_received')}</th>
                <th class="px-3 py-2.5 text-center font-semibold">{$t('money_col_uploaded')}</th>
                <th class="px-3 py-2.5 text-center font-semibold">{$t('money_col_failed')}</th>
                <th class="px-3 py-2.5 text-center font-semibold">{$t('money_col_earning')}</th>
                <th class="px-3 py-2.5 text-left font-semibold">{$t('money_col_earning_items')}</th>
              </tr>
            </thead>
            <tbody>
              {#each rows as row, idx}
                <tr class={idx % 2 === 0 ? 'bg-white dark:bg-zinc-800' : 'bg-[#ececec] dark:bg-zinc-900'}>
                  <td class="whitespace-nowrap px-3 py-2.5 font-medium">{row.com_port || '—'}</td>
                  <td class="whitespace-nowrap px-3 py-2.5">{row.phone_number || '—'}</td>
                  <td class="px-3 py-2.5 text-center font-semibold text-[#245dff] dark:text-blue-300">{row.waiting_sms_count ?? 0}</td>
                  <td class="px-3 py-2.5 text-center font-medium">{row.received_sms_count ?? 0}</td>
                  <td class="px-3 py-2.5 text-center font-semibold text-[#245dff] dark:text-blue-300">{row.successful_uploaded_sms_count ?? 0}</td>
                  <td class="px-3 py-2.5 text-center font-semibold text-[#d63031] dark:text-red-300">{row.failed_sms_count ?? 0}</td>
                  <td class="px-3 py-2.5 text-center font-bold text-[#10a248] dark:text-green-300">{formatMoney(row.money_earning ?? 0)}</td>
                  <td class="px-3 py-2.5 text-[12px] leading-5">{row.earning_item_names || '—'}</td>
                </tr>
              {:else}
                <tr>
                  <td colspan="8" class="px-3 py-8 text-center text-gray-500 dark:text-gray-400">{$t('money_empty')}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}
  </div>
</div>

<style>
  .money-table thead tr {
    height: 36px;
  }

  .money-table tbody tr {
    height: 34px;
  }

  .money-table th,
  .money-table td {
    font-variant-numeric: tabular-nums;
  }

  .money-table .col-port {
    width: 90px;
  }

  .money-table .col-phone {
    width: 160px;
  }

  .money-table .col-wait,
  .money-table .col-recv,
  .money-table .col-up,
  .money-table .col-fail {
    width: 92px;
  }

  .money-table .col-profit {
    width: 110px;
  }

  .money-table .col-items {
    min-width: 420px;
  }
</style>
