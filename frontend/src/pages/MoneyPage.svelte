<script>
  import { onMount, onDestroy } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();

  let rows = $state([]);
  let loading = $state(true);
  let error = $state('');

  let pollTimer;

  function formatMoney(value) {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '0';
    return num.toFixed(2).replace(/\.00$/, '');
  }

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

  onMount(async () => {
    await fetchData();
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
