<script>
  import { onMount } from 'svelte';
  import Icon from '@iconify/svelte';
  import { apiClient } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();

  let ports = $state([]);
  let loading = $state(true);
  let error = $state('');
  let message = $state('');
  let busy = $state(false);

  // The port whose session panel is expanded / active.
  let activePort = $state(null);
  let chip = $state(null);
  let profiles = $state([]);

  // Download form
  let dl = $state({ smdp: '', matching_id: '', confirmation_code: '', imei: '', activation_code: '' });

  const sessionPort = $derived(ports.find((p) => p.session_active)?.com_port ?? null);

  /** FetchApi rejects with {status, data} (not an Error), so pull the message out of the response body. */
  function describeError(e) {
    return e?.data?.error ?? e?.message ?? (e?.status ? `HTTP ${e.status}` : String(e));
  }

  async function loadPorts() {
    loading = true;
    error = '';
    try {
      const res = await apiClient.esimListPorts();
      ports = res?.data ?? [];
    } catch (e) {
      error = describeError(e);
    } finally {
      loading = false;
    }
  }

  function flash(msg) {
    message = msg;
    setTimeout(() => (message = ''), 4000);
  }

  function fail(e) {
    error = $t('esim_op_failed', { msg: describeError(e) });
    setTimeout(() => (error = ''), 6000);
  }

  async function enter(com) {
    busy = true;
    error = '';
    try {
      await apiClient.esimEnter(com);
      activePort = com;
      await loadPorts();
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function exit(com) {
    busy = true;
    error = '';
    try {
      await apiClient.esimExit(com);
      if (activePort === com) {
        activePort = null;
        chip = null;
        profiles = [];
      }
      await loadPorts();
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function reset(com) {
    busy = true;
    try {
      await apiClient.esimReset(com);
      if (activePort === com) {
        activePort = null;
        chip = null;
        profiles = [];
      }
      await loadPorts();
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function refreshSession(com) {
    try {
      const [chipRes, profRes] = await Promise.allSettled([
        apiClient.esimChip(com),
        apiClient.esimProfiles(com),
      ]);
      chip = chipRes.status === 'fulfilled' ? (chipRes.value?.data ?? null) : null;
      const p = profRes.status === 'fulfilled' ? (profRes.value?.data ?? []) : [];
      profiles = Array.isArray(p) ? p : [];
    } catch (e) {
      fail(e);
    }
  }

  async function enableProfile(com, iccid) {
    busy = true;
    try {
      await apiClient.esimEnable(com, iccid, true);
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function disableProfile(com, iccid) {
    busy = true;
    try {
      await apiClient.esimDisable(com, iccid, true);
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function deleteProfile(com, iccid) {
    if (!confirm($t('esim_confirm_delete'))) return;
    busy = true;
    try {
      await apiClient.esimDelete(com, iccid);
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function nicknameProfile(com, iccid) {
    const name = prompt($t('esim_nickname_prompt'));
    if (name == null) return;
    busy = true;
    try {
      await apiClient.esimNickname(com, iccid, name);
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  async function download(com) {
    busy = true;
    try {
      const body = {};
      for (const [k, v] of Object.entries(dl)) if (v) body[k] = v;
      await apiClient.esimDownload(com, body);
      await refreshSession(com);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    } finally {
      busy = false;
    }
  }

  function iccidOf(p) {
    return p.iccid ?? p.ICCID ?? '';
  }
  function providerOf(p) {
    return p.serviceProviderName ?? p.provider ?? '';
  }
  function nameOf(p) {
    return p.profileNickname ?? p.profileName ?? p.nickname ?? '';
  }
  function stateOf(p) {
    const s = (p.profileState ?? p.state ?? '').toString().toLowerCase();
    return s.includes('enable') ? 'enabled' : 'disabled';
  }

  onMount(loadPorts);
</script>

<div class="h-dvh w-screen overflow-y-auto bg-slate-50 p-4 md:p-6">
  <div class="mx-auto max-w-5xl">
    <div class="mb-4 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <button class="rounded-lg p-2 hover:bg-slate-200" onclick={onBack} aria-label="back">
          <Icon icon="mdi:arrow-left" width="22" />
        </button>
        <div>
          <h1 class="text-xl font-semibold text-slate-800">{$t('esim_title')}</h1>
          <p class="text-sm text-slate-500">{$t('esim_subtitle')}</p>
        </div>
      </div>
      <button
        class="flex items-center gap-1 rounded-lg bg-slate-800 px-3 py-2 text-sm text-white hover:bg-slate-700 disabled:opacity-50"
        onclick={loadPorts}
        disabled={loading || busy}
      >
        <Icon icon="mdi:refresh" width="18" />
        {$t('esim_refresh')}
      </button>
    </div>

    <!-- Lock banner -->
    <div class="mb-4 rounded-lg border px-4 py-2 text-sm {sessionPort ? 'border-amber-300 bg-amber-50 text-amber-800' : 'border-slate-200 bg-white text-slate-500'}">
      {sessionPort ? $t('esim_lock_banner', { port: sessionPort }) : $t('esim_lock_free')}
    </div>

    {#if message}
      <div class="mb-3 rounded-lg bg-green-50 px-4 py-2 text-sm text-green-700">{message}</div>
    {/if}
    {#if error}
      <div class="mb-3 rounded-lg bg-red-50 px-4 py-2 text-sm text-red-700">{error}</div>
    {/if}

    {#if loading}
      <p class="text-slate-500">…</p>
    {:else if ports.length === 0}
      <p class="rounded-lg bg-white p-6 text-center text-slate-500">{$t('esim_no_ports')}</p>
    {:else}
      <div class="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <table class="w-full text-sm">
          <thead class="bg-slate-100 text-left text-slate-600">
            <tr>
              <th class="px-4 py-2">{$t('esim_col_port')}</th>
              <th class="px-4 py-2">{$t('esim_col_capable')}</th>
              <th class="px-4 py-2">{$t('esim_col_mode')}</th>
              <th class="px-4 py-2">{$t('esim_col_sim')}</th>
              <th class="px-4 py-2 text-right">{$t('esim_col_actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each ports as p (p.com_port)}
              <tr class="border-t border-slate-100">
                <td class="px-4 py-2 font-medium text-slate-800">{p.com_port}</td>
                <td class="px-4 py-2">{p.esim_capable ? $t('esim_capable_yes') : $t('esim_capable_no')}</td>
                <td class="px-4 py-2">
                  <span class="rounded-full px-2 py-0.5 text-xs {p.esim_mode ? 'bg-amber-100 text-amber-700' : 'bg-slate-100 text-slate-600'}">
                    {p.esim_mode ? $t('esim_mode_esim') : $t('esim_mode_normal')}
                  </span>
                </td>
                <td class="px-4 py-2 text-slate-500">{p.sim_id ?? '—'}</td>
                <td class="px-4 py-2 text-right">
                  {#if p.esim_capable}
                    {#if p.esim_mode}
                      <button class="rounded bg-slate-700 px-2 py-1 text-xs text-white hover:bg-slate-600 disabled:opacity-50" onclick={() => exit(p.com_port)} disabled={busy}>{$t('esim_exit')}</button>
                      <button class="ml-1 rounded bg-red-500 px-2 py-1 text-xs text-white hover:bg-red-400 disabled:opacity-50" onclick={() => reset(p.com_port)} disabled={busy}>{$t('esim_reset')}</button>
                    {:else}
                      <button class="rounded bg-indigo-600 px-2 py-1 text-xs text-white hover:bg-indigo-500 disabled:opacity-50" onclick={() => enter(p.com_port)} disabled={busy || (sessionPort && sessionPort !== p.com_port)}>{$t('esim_enter')}</button>
                    {/if}
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>

      <p class="mt-2 text-xs text-amber-600">{$t('esim_warn_sms_paused')}</p>

      <!-- Session panel -->
      {#if activePort && sessionPort === activePort}
        <div class="mt-6 rounded-lg border border-slate-200 bg-white p-4">
          <div class="mb-3 flex items-center justify-between">
            <h2 class="font-semibold text-slate-800">{activePort} — {$t('esim_profiles')}</h2>
            <button class="rounded bg-slate-100 px-2 py-1 text-xs hover:bg-slate-200" onclick={() => refreshSession(activePort)} disabled={busy}>{$t('esim_refresh')}</button>
          </div>

          {#if chip}
            <div class="mb-4 grid grid-cols-1 gap-1 rounded bg-slate-50 p-3 text-xs text-slate-600 md:grid-cols-2">
              <div><span class="font-medium">{$t('esim_eid')}:</span> {chip.eidValue ?? chip.eid ?? '—'}</div>
              <div><span class="font-medium">{$t('esim_default_smdp')}:</span> {chip?.EuiccConfiguredAddresses?.defaultDpAddress ?? chip?.default_smdp ?? '—'}</div>
            </div>
          {/if}

          {#if profiles.length === 0}
            <p class="text-sm text-slate-500">{$t('esim_no_profiles')}</p>
          {:else}
            <table class="w-full text-sm">
              <thead class="bg-slate-100 text-left text-slate-600">
                <tr>
                  <th class="px-3 py-2">{$t('esim_col_provider')}</th>
                  <th class="px-3 py-2">{$t('esim_col_name')}</th>
                  <th class="px-3 py-2">{$t('esim_col_iccid')}</th>
                  <th class="px-3 py-2">{$t('esim_col_state')}</th>
                  <th class="px-3 py-2 text-right">{$t('esim_col_actions')}</th>
                </tr>
              </thead>
              <tbody>
                {#each profiles as p (iccidOf(p))}
                  <tr class="border-t border-slate-100">
                    <td class="px-3 py-2">{providerOf(p)}</td>
                    <td class="px-3 py-2">{nameOf(p)}</td>
                    <td class="px-3 py-2 font-mono text-xs">{iccidOf(p)}</td>
                    <td class="px-3 py-2">
                      <span class="rounded-full px-2 py-0.5 text-xs {stateOf(p) === 'enabled' ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-600'}">
                        {stateOf(p) === 'enabled' ? $t('esim_state_enabled') : $t('esim_state_disabled')}
                      </span>
                    </td>
                    <td class="px-3 py-2 text-right whitespace-nowrap">
                      {#if stateOf(p) === 'enabled'}
                        <button class="rounded bg-slate-200 px-2 py-1 text-xs hover:bg-slate-300 disabled:opacity-50" onclick={() => disableProfile(activePort, iccidOf(p))} disabled={busy}>{$t('esim_disable')}</button>
                      {:else}
                        <button class="rounded bg-green-600 px-2 py-1 text-xs text-white hover:bg-green-500 disabled:opacity-50" onclick={() => enableProfile(activePort, iccidOf(p))} disabled={busy}>{$t('esim_enable')}</button>
                      {/if}
                      <button class="ml-1 rounded bg-slate-100 px-2 py-1 text-xs hover:bg-slate-200 disabled:opacity-50" onclick={() => nicknameProfile(activePort, iccidOf(p))} disabled={busy}>{$t('esim_nickname')}</button>
                      <button class="ml-1 rounded bg-red-500 px-2 py-1 text-xs text-white hover:bg-red-400 disabled:opacity-50" onclick={() => deleteProfile(activePort, iccidOf(p))} disabled={busy}>{$t('esim_delete')}</button>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          {/if}

          <!-- Download form -->
          <div class="mt-6 rounded border border-slate-200 p-3">
            <h3 class="mb-2 text-sm font-semibold text-slate-700">{$t('esim_download_title')}</h3>
            <div class="grid grid-cols-1 gap-2 md:grid-cols-2">
              <input class="rounded border border-slate-300 px-2 py-1 text-sm" placeholder={$t('esim_field_smdp')} bind:value={dl.smdp} />
              <input class="rounded border border-slate-300 px-2 py-1 text-sm" placeholder={$t('esim_field_matching')} bind:value={dl.matching_id} />
              <input class="rounded border border-slate-300 px-2 py-1 text-sm" placeholder={$t('esim_field_confirmation')} bind:value={dl.confirmation_code} />
              <input class="rounded border border-slate-300 px-2 py-1 text-sm" placeholder={$t('esim_field_imei')} bind:value={dl.imei} />
              <input class="rounded border border-slate-300 px-2 py-1 text-sm md:col-span-2" placeholder={$t('esim_field_activation')} bind:value={dl.activation_code} />
            </div>
            <div class="mt-2 flex gap-2">
              <button class="rounded bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-500 disabled:opacity-50" onclick={() => download(activePort)} disabled={busy}>{$t('esim_download_btn')}</button>
            </div>
          </div>
        </div>
      {/if}
    {/if}
  </div>
</div>
