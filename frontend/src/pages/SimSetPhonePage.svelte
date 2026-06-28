<script>
  import { onMount } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {}, simId = null } = $props();

  let phoneNumber = $state('');
  let loading = $state(false);
  let fetching = $state(true);
  let error = $state('');
  let success = $state('');
  let simInfo = $state(null);

  // Best-effort display info for the selected SIM
  const displayPort = $derived(simInfo?.com_port ?? '—');
  const displayIccid = $derived(simId ?? '—');
  const currentPhone = $derived(simInfo?.phone_number ?? '');

  onMount(async () => {
    if (!simId) {
      fetching = false;
      error = $t('err_no_sim_selected');
      return;
    }

    try {
      const response = await apiClient.getAllSimsInfo();
      const infos = Array.isArray(response) ? response : (response?.data ?? []);
      simInfo = infos.find(info => info.sim_id === simId) ?? null;
      if (simInfo?.phone_number) {
        phoneNumber = simInfo.phone_number;
      }
    } catch (e) {
      console.error('Failed to load SIM info:', e);
    } finally {
      fetching = false;
    }
  });

  function normalizePhone(value) {
    return value.replace(/[^\d+]/g, '');
  }

  async function handleSubmit(e) {
    e.preventDefault();
    error = '';
    success = '';

    if (!simId) {
      error = $t('err_no_sim_selected');
      return;
    }

    const normalized = normalizePhone(phoneNumber).trim();
    if (!normalized) {
      error = $t('err_invalid_phone');
      return;
    }

    loading = true;
    try {
      await apiClient.setSimPhoneNumber(simId, normalized);
      success = $t('msg_phone_set_success');
      phoneNumber = normalized;
      // Refresh local info so the current phone display updates immediately
      const response = await apiClient.getAllSimsInfo();
      const infos = Array.isArray(response) ? response : (response?.data ?? []);
      simInfo = infos.find(info => info.sim_id === simId) ?? simInfo;
    } catch (err) {
      error = err?.data?.error ?? err?.message ?? $t('err_phone_set_failed');
    } finally {
      loading = false;
    }
  }
</script>

<div class="flex flex-col h-dvh w-screen bg-gray-50 dark:bg-zinc-950 font-sans">
  <!-- Header -->
  <header class="flex items-center gap-3 px-4 py-3 border-b border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shadow-sm shrink-0">
    <button
      onclick={onBack}
      class="inline-flex items-center justify-center w-9 h-9 rounded-lg border border-gray-200 dark:border-zinc-700
             text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-zinc-800 transition"
      aria-label="Back"
    >
      <Icon icon="carbon:arrow-left" class="w-5 h-5" />
    </button>
    <div class="flex items-center gap-2">
      <Icon icon="carbon:phone" class="w-5 h-5 text-gray-500 dark:text-gray-400" />
      <h1 class="text-base font-semibold text-gray-800 dark:text-gray-100">{$t('set_phone_title')}</h1>
    </div>
  </header>

  <!-- Content -->
  <div class="flex-1 overflow-auto p-4 sm:p-8">
    {#if fetching}
      <div class="max-w-md mx-auto bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-800 shadow-sm p-6">
        <div class="animate-pulse space-y-4">
          <div class="h-4 bg-gray-200 dark:bg-zinc-700 rounded w-1/2"></div>
          <div class="h-3 bg-gray-200 dark:bg-zinc-700 rounded w-3/4"></div>
          <div class="h-10 bg-gray-200 dark:bg-zinc-700 rounded"></div>
        </div>
      </div>
    {:else}
      <div class="max-w-md mx-auto bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-800 shadow-sm p-6">
        <div class="mb-6">
          <h2 class="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-1">{$t('set_phone_subtitle')}</h2>
          <p class="text-xs text-gray-500 dark:text-gray-400">{$t('set_phone_desc')}</p>
        </div>

        <!-- SIM summary -->
        <div class="space-y-3 mb-6 text-sm">
          <div class="flex justify-between">
            <span class="text-gray-500 dark:text-gray-400">{$t('col_com_port')}</span>
            <span class="font-mono text-gray-800 dark:text-gray-200">{displayPort}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-500 dark:text-gray-400">{$t('col_iccid')}</span>
            <span class="font-mono text-gray-800 dark:text-gray-200">{displayIccid}</span>
          </div>
          {#if currentPhone}
            <div class="flex justify-between">
              <span class="text-gray-500 dark:text-gray-400">{$t('current_phone_label')}</span>
              <span class="font-mono text-gray-800 dark:text-gray-200">{currentPhone}</span>
            </div>
          {/if}
        </div>

        <form onsubmit={handleSubmit} class="space-y-4">
          <div>
            <label for="phone" class="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1.5">
              {$t('phone_number_label')}
            </label>
            <input
              id="phone"
              type="tel"
              bind:value={phoneNumber}
              placeholder={$t('enter_phone')}
              disabled={loading}
              class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-zinc-700
                     bg-white dark:bg-zinc-950 text-gray-900 dark:text-gray-100
                     placeholder-gray-400 dark:placeholder-gray-600
                     focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent
                     disabled:opacity-60 disabled:cursor-not-allowed text-sm"
            />
            <p class="mt-1.5 text-xs text-gray-500 dark:text-gray-400">{$t('phone_input_hint')}</p>
          </div>

          {#if error}
            <div class="flex items-start gap-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg p-3">
              <Icon icon="carbon:warning" class="w-4 h-4 shrink-0 mt-0.5" />
              <span>{error}</span>
            </div>
          {/if}

          {#if success}
            <div class="flex items-start gap-2 text-xs text-green-600 dark:text-green-400 bg-green-50 dark:bg-green-900/20 rounded-lg p-3">
              <Icon icon="carbon:checkmark" class="w-4 h-4 shrink-0 mt-0.5" />
              <span>{success}</span>
            </div>
          {/if}

          <div class="flex gap-3 pt-2">
            <button
              type="button"
              onclick={onBack}
              disabled={loading}
              class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-zinc-700
                     text-gray-700 dark:text-gray-300 text-sm font-medium
                     hover:bg-gray-50 dark:hover:bg-zinc-800 transition
                     disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {$t('cancel')}
            </button>
            <button
              type="submit"
              disabled={loading}
              class="flex-1 px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700
                     text-white text-sm font-medium transition
                     disabled:opacity-60 disabled:cursor-not-allowed
                     inline-flex items-center justify-center gap-2"
            >
              {#if loading}
                <Icon icon="carbon:loading" class="w-4 h-4 animate-spin" />
                {$t('saving')}
              {:else}
                {$t('btn_save_number')}
              {/if}
            </button>
          </div>
        </form>
      </div>
    {/if}
  </div>
</div>
