<script>
  import { onMount, onDestroy } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();

  let rows = $state([]);
  let loading = $state(true);
  let refreshing = $state(false);
  let error = $state('');
  let itemOptions = $state([]);
  let itemSearchKeyword = $state('');
  let isItemDropdownOpen = $state(false);
  let itemBoxEl; // plain DOM ref for click-outside detection, not reactive state
  let itemOptionsRequestId = 0;
  let selectedItemId = $state('');
  let selectedItemName = $state('');
  let unitPriceInput = $state('');
  let selectedSuccessCount = $state(0);
  let selectedEarningLoading = $state(false);
  let priceSaving = $state(false);
  let priceSaveError = $state('');
  let priceSaved = $state(false);
  let priceSavedTimer;

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

  async function handleRefresh() {
    if (refreshing) return;
    refreshing = true;
    try {
      await fetchData();
    } finally {
      refreshing = false;
    }
  }

  async function fetchItemOptions(keyword = '') {
    const requestId = ++itemOptionsRequestId;
    try {
      const data = await apiClient.getFirefoxMoneyItems(keyword, 300);
      if (requestId !== itemOptionsRequestId) return; // a newer request superseded this one
      itemOptions = Array.isArray(data) ? data : (data?.data ?? []);
    } catch (e) {
      if (requestId !== itemOptionsRequestId) return;
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

  function selectItem(opt) {
    selectedItemId = opt.item_id;
    selectedItemName = opt.item_name || '';
    if (opt.item_uprice != null) {
      unitPriceInput = String(opt.item_uprice);
    }
    fetchSelectedItemEarning();
  }

  function onChooseItem(opt) {
    selectItem(opt);
    isItemDropdownOpen = false;
  }

  function toggleItemDropdown() {
    if (isItemDropdownOpen) {
      isItemDropdownOpen = false;
      return;
    }
    isItemDropdownOpen = true;
    itemSearchKeyword = '';
    fetchItemOptions('');
  }

  function onItemSearchInput(event) {
    itemSearchKeyword = event.currentTarget?.value || '';
    fetchItemOptions(itemSearchKeyword);
  }

  function onDocumentClick(event) {
    if (isItemDropdownOpen && itemBoxEl && !itemBoxEl.contains(event.target)) {
      isItemDropdownOpen = false;
    }
  }

  function onUnitPriceInput(event) {
    const raw = event.currentTarget?.value || '';
    const cleaned = raw.replace(/[^\d.]/g, '');
    const firstDotIndex = cleaned.indexOf('.');
    unitPriceInput = firstDotIndex === -1
      ? cleaned
      : cleaned.slice(0, firstDotIndex + 1) + cleaned.slice(firstDotIndex + 1).replace(/\./g, '');
    if (raw !== unitPriceInput) {
      event.currentTarget.value = unitPriceInput;
    }
  }

  async function saveUnitPrice() {
    if (!selectedItemId) return;
    const price = parseUnitPrice(unitPriceInput);
    if (price == null) return;

    priceSaving = true;
    priceSaveError = '';
    try {
      await apiClient.updateFirefoxMoneyItemPrice(selectedItemId, price);
      await Promise.all([fetchData(), fetchSelectedItemEarning()]);
      priceSaved = true;
      clearTimeout(priceSavedTimer);
      priceSavedTimer = setTimeout(() => {
        priceSaved = false;
      }, 2000);
    } catch (e) {
      priceSaveError = e?.message ?? $t('err_save_item_price');
    } finally {
      priceSaving = false;
    }
  }

  function onUnitPriceKeydown(event) {
    if (event.key === 'Enter') {
      event.preventDefault();
      saveUnitPrice();
    }
  }

  function focusOnMount(node) {
    node.focus();
  }

  onMount(async () => {
    const [, defaultOptions] = await Promise.all([fetchData(), apiClient.getFirefoxMoneyItems('', 300)]);
    const list = Array.isArray(defaultOptions) ? defaultOptions : (defaultOptions?.data ?? []);
    if (list.length > 0) {
      selectItem(list[0]);
    }
    pollTimer = setInterval(fetchData, 5000);
  });

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer);
    clearTimeout(priceSavedTimer);
  });
</script>

<svelte:window onclick={onDocumentClick} />


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
      onclick={handleRefresh}
      disabled={refreshing}
      class="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-2.5 py-1.5 text-[11px] font-semibold text-white hover:bg-blue-700 transition disabled:opacity-60"
    >
      <Icon icon="carbon:renew" class="h-3.5 w-3.5 {refreshing ? 'animate-spin' : ''}" />
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
        <div class="max-w-3xl rounded border border-gray-300 dark:border-zinc-600">
          <div class="flex flex-wrap items-stretch">
            <label for="money-item-input" class="flex items-center bg-gray-100 px-3 py-2 text-sm font-medium text-gray-700 dark:bg-zinc-700 dark:text-gray-200">
              {$t('money_item_label')}
            </label>
            <div class="relative min-w-[220px] flex-1 p-2" bind:this={itemBoxEl}>
              <button
                id="money-item-toggle"
                type="button"
                onclick={toggleItemDropdown}
                class="flex w-full items-center justify-between rounded border border-gray-300 px-2 py-1.5 text-left text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
              >
                <span class="truncate">
                  {selectedItemId ? `${selectedItemId} | ${selectedItemName}` : $t('money_item_no_selection')}
                </span>
                <Icon
                  icon="carbon:chevron-down"
                  class="h-4 w-4 shrink-0 text-gray-500 transition-transform dark:text-gray-400 {isItemDropdownOpen ? 'rotate-180' : ''}"
                />
              </button>
              {#if isItemDropdownOpen}
                <div class="absolute left-2 right-2 top-full z-20 mt-1 rounded border border-gray-300 bg-white shadow-lg dark:border-zinc-600 dark:bg-zinc-800">
                  <div class="border-b border-gray-200 p-2 dark:border-zinc-700">
                    <input
                      id="money-item-search"
                      type="text"
                      autocomplete="off"
                      use:focusOnMount
                      class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
                      placeholder={$t('money_item_search_placeholder')}
                      value={itemSearchKeyword}
                      oninput={onItemSearchInput}
                    />
                  </div>
                  <div class="max-h-56 overflow-y-auto">
                    {#each itemOptions as opt (opt.item_id)}
                      <button
                        type="button"
                        onclick={() => onChooseItem(opt)}
                        class="block w-full px-3 py-1.5 text-left text-sm transition
                               {selectedItemId === opt.item_id
                                 ? 'bg-blue-600 text-white'
                                 : 'text-gray-700 hover:bg-blue-50 dark:text-gray-200 dark:hover:bg-zinc-700'}"
                      >
                        {opt.item_id} | {opt.item_name}
                      </button>
                    {:else}
                      <div class="px-3 py-2 text-sm text-gray-500 dark:text-gray-400">
                        {$t('money_item_select_placeholder')}
                      </div>
                    {/each}
                  </div>
                </div>
              {/if}
            </div>
            <label for="money-unit-price" class="flex items-center border-l border-gray-300 bg-gray-100 px-3 py-2 text-sm font-medium text-gray-700 dark:border-zinc-600 dark:bg-zinc-700 dark:text-gray-200">
              {$t('money_unit_price_label')}
            </label>
            <div class="flex items-center gap-2 border-l border-gray-300 p-2 dark:border-zinc-600">
              <input
                id="money-unit-price"
                type="text"
                inputmode="decimal"
                class="w-24 rounded border border-gray-300 px-2 py-1.5 text-sm outline-none focus:border-blue-500 dark:border-zinc-600 dark:bg-zinc-900"
                placeholder={$t('money_unit_price_placeholder')}
                value={unitPriceInput}
                oninput={onUnitPriceInput}
                onkeydown={onUnitPriceKeydown}
              />
              <button
                type="button"
                onclick={saveUnitPrice}
                class="inline-flex items-center rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 transition"
              >
                {$t('btn_confirm')}
              </button>
            </div>
          </div>
        </div>

        {#if priceSaving}
          <div class="mt-1 text-xs text-gray-500 dark:text-gray-400">{$t('saving')}</div>
        {:else if priceSaveError}
          <div class="mt-1 text-xs text-red-600 dark:text-red-400">{priceSaveError}</div>
        {:else if priceSaved}
          <div class="mt-1 text-xs text-[#10a248] dark:text-green-300">{$t('money_price_saved')}</div>
        {/if}

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
