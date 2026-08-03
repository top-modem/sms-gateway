<script>
  import { onMount, onDestroy } from 'svelte';
  import Icon from '@iconify/svelte';
  import { EventSourcePolyfill } from 'event-source-polyfill';
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

  // ── Batch selection / filtering ──────────────────────────────────────────
  let selected = $state(new Set());
  let filterText = $state('');
  let filterCapable = $state(false);
  let filterHasSim = $state(false);

  // Batch dialog + job state
  let batchMode = $state(null); // null | 'download' | 'delete' | 'activate' | 'deactivate'
  let sources = $state([]); // ActivationCode[]
  let pairing = $state({}); // com_port -> index into `sources`
  let pasteText = $state('');
  let batchOptions = $state({ auto_enable: true, replace_existing: true, stop_on_error: false });
  let job = $state(null); // JobSnapshot
  let batchBusy = $state(false);
  let sseConn = null;

  const sessionPort = $derived(ports.find((p) => p.session_active)?.com_port ?? null);

  const filteredPorts = $derived(
    ports.filter((p) => {
      if (filterCapable && !p.esim_capable) return false;
      if (filterHasSim && !p.sim_id) return false;
      const q = filterText.trim().toLowerCase();
      if (q) {
        const hay = `${p.com_port} ${p.sim_id ?? ''}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    })
  );

  const selectedList = $derived(
    [...selected].sort((a, b) => (comNum(a) ?? 0) - (comNum(b) ?? 0))
  );
  const allFilteredSelected = $derived(
    filteredPorts.filter((p) => p.esim_capable).length > 0 &&
      filteredPorts.filter((p) => p.esim_capable).every((p) => selected.has(p.com_port))
  );

  function comNum(com) {
    const m = String(com).match(/(\d+)/);
    return m ? parseInt(m[1], 10) : null;
  }

  /** FetchApi rejects with {status, data} (not an Error), so pull the message out of the response body. */
  function describeError(e) {
    return e?.data?.error ?? e?.data?.message ?? e?.message ?? (e?.status ? `HTTP ${e.status}` : String(e));
  }

  function unwrapPayload(res) {
    const body = res?.data;
    if (body && typeof body === 'object' && 'data' in body) {
      return body.data;
    }
    return body;
  }

  async function loadPorts() {
    loading = true;
    error = '';
    try {
      const res = await apiClient.esimListPorts();
      const payload = unwrapPayload(res);
      ports = Array.isArray(payload) ? payload : [];
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
      chip = chipRes.status === 'fulfilled' ? (unwrapPayload(chipRes.value) ?? null) : null;
      const p = profRes.status === 'fulfilled' ? (unwrapPayload(profRes.value) ?? []) : [];
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

  // ── Selection ────────────────────────────────────────────────────────────
  function toggleSelect(com) {
    const s = new Set(selected);
    s.has(com) ? s.delete(com) : s.add(com);
    selected = s;
  }
  function toggleSelectAll() {
    const s = new Set(selected);
    const capable = filteredPorts.filter((p) => p.esim_capable);
    if (allFilteredSelected) {
      capable.forEach((p) => s.delete(p.com_port));
    } else {
      capable.forEach((p) => s.add(p.com_port));
    }
    selected = s;
  }
  function clearSelection() {
    selected = new Set();
  }

  // ── Batch sources / pairing ──────────────────────────────────────────────
  async function loadSources() {
    batchBusy = true;
    try {
      const res = await apiClient.esimScanSources();
      sources = unwrapPayload(res) ?? [];
    } catch (e) {
      fail(e);
      sources = [];
    } finally {
      batchBusy = false;
    }
  }

  async function uploadSources(ev) {
    const files = ev.target?.files;
    if (!files || !files.length) return;
    batchBusy = true;
    try {
      const res = await apiClient.esimUploadSources(files);
      const codes = unwrapPayload(res) ?? [];
      sources = [...sources, ...codes];
      buildPairing();
    } catch (e) {
      fail(e);
    } finally {
      batchBusy = false;
      if (ev.target) ev.target.value = '';
    }
  }

  function addPasted() {
    const lines = pasteText.split(/\r?\n/);
    const added = [];
    for (const line of lines) {
      const m = line.match(/LPA:1\$([^$\s]+)\$([^$\s]+)(?:\$([^$\s]+))?/i);
      if (m) {
        added.push({
          raw_lpa: `LPA:1$${m[1]}$${m[2]}${m[3] ? '$' + m[3] : ''}`,
          smdp: m[1],
          matching_id: m[2],
          confirmation_code: m[3] || null,
          source: 'manual',
          port_hint: null,
          label: null,
        });
      }
    }
    sources = [...sources, ...added];
    pasteText = '';
    buildPairing();
  }

  /** Auto-pair codes to selected ports: filename hint first, then sequential. */
  function buildPairing() {
    const map = {};
    const used = new Set();
    for (const com of selectedList) {
      const n = comNum(com);
      const idx = sources.findIndex((c, i) => !used.has(i) && c.port_hint === n);
      if (idx >= 0) {
        map[com] = idx;
        used.add(idx);
      }
    }
    for (const com of selectedList) {
      if (map[com] != null) continue;
      const idx = sources.findIndex((c, i) => !used.has(i));
      if (idx >= 0) {
        map[com] = idx;
        used.add(idx);
      }
    }
    pairing = map;
  }
  function setPair(com, value) {
    pairing = { ...pairing, [com]: value === '' ? null : Number(value) };
  }
  function codeLabel(c) {
    if (!c) return '';
    return `${c.smdp} / ${c.matching_id}${c.label ? ' (' + c.label + ')' : ''}`;
  }

  // ── Batch dialog + run ───────────────────────────────────────────────────
  async function openBatch(op) {
    if (sessionPort) {
      fail({ data: { error: $t('esim_batch_blocked_session', { port: sessionPort }) } });
      return;
    }
    if (selected.size === 0) return;
    batchMode = op;
    job = null;
    if (op === 'download') {
      await loadSources();
      buildPairing();
    }
  }
  function closeBatch() {
    batchMode = null;
    pasteText = '';
  }

  async function startDownload() {
    const items = selectedList.map((com) => {
      const idx = pairing[com];
      const code = idx != null && sources[idx] ? sources[idx] : null;
      return { com_port: com, activation_code: code?.raw_lpa ?? null };
    });
    await runBatch('download', items);
  }

  async function startSimpleBatch(op) {
    if (op === 'delete' && !confirm($t('esim_batch_confirm_delete', { n: selected.size }))) return;
    if (op === 'deactivate' && !confirm($t('esim_batch_confirm_deactivate', { n: selected.size }))) return;
    const items = selectedList.map((com) => ({ com_port: com }));
    await runBatch(op, items);
  }

  async function runBatch(op, items) {
    batchBusy = true;
    try {
      const payload = { op, items, ...batchOptions };
      await apiClient.esimStartBatch(payload);
      connectBatchSse();
      await pollBatchOnce();
      flash($t('esim_batch_started'));
    } catch (e) {
      fail(e);
    } finally {
      batchBusy = false;
    }
  }

  function authHeader() {
    const a = sessionStorage.getItem('auth') || localStorage.getItem('auth');
    if (a) {
      try {
        const { token } = JSON.parse(a);
        if (token) return { Authorization: `Basic ${token}` };
      } catch (_) {}
    }
    return { Authorization: 'Basic YWRtaW46MTIzNDU2' };
  }

  function connectBatchSse() {
    if (sseConn) sseConn.close();
    sseConn = new EventSourcePolyfill('/api/esim/batch/events', {
      headers: authHeader(),
      heartbeatTimeout: 60000,
    });
    sseConn.addEventListener('batch', (e) => {
      try {
        const snap = JSON.parse(e.data);
        job = snap;
        if (snap && (snap.status === 'done' || snap.status === 'cancelled')) {
          loadPorts();
        }
      } catch (_) {}
    });
    sseConn.onerror = () => {
      if (sseConn) {
        sseConn.close();
        sseConn = null;
      }
    };
  }

  async function pollBatchOnce() {
    try {
      const res = await apiClient.esimGetBatch();
      const snap = unwrapPayload(res);
      if (snap) job = snap;
    } catch (_) {}
  }

  async function cancelBatch() {
    try {
      await apiClient.esimCancelBatch();
    } catch (e) {
      fail(e);
    }
  }

  const jobRunning = $derived(job?.status === 'running');
  function itemBadge(status) {
    switch (status) {
      case 'ok':
        return 'bg-green-100 text-green-700';
      case 'failed':
        return 'bg-red-100 text-red-700';
      case 'running':
        return 'bg-amber-100 text-amber-700';
      case 'skipped':
        return 'bg-slate-100 text-slate-500';
      default:
        return 'bg-slate-100 text-slate-500';
    }
  }

  onMount(loadPorts);
  onDestroy(() => {
    if (sseConn) sseConn.close();
  });
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

    <!-- Filter + batch toolbar -->
    {#if !loading && ports.length > 0}
      <div class="mb-3 flex flex-wrap items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2">
        <div class="relative">
          <Icon icon="mdi:magnify" width="16" class="absolute left-2 top-2 text-slate-400" />
          <input
            class="w-48 rounded border border-slate-300 py-1 pl-7 pr-2 text-sm"
            placeholder={$t('esim_filter_placeholder')}
            bind:value={filterText}
          />
        </div>
        <label class="flex items-center gap-1 text-xs text-slate-600">
          <input type="checkbox" bind:checked={filterCapable} /> {$t('esim_filter_capable')}
        </label>
        <label class="flex items-center gap-1 text-xs text-slate-600">
          <input type="checkbox" bind:checked={filterHasSim} /> {$t('esim_filter_hassim')}
        </label>
        <span class="ml-auto text-xs text-slate-500">{$t('esim_selected_count', { n: selected.size })}</span>
      </div>

      {#if selected.size > 0}
        <div class="mb-3 flex flex-wrap items-center gap-2 rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2">
          <span class="text-sm font-medium text-indigo-800">{$t('esim_batch_bar', { n: selected.size })}</span>
          <button class="rounded bg-indigo-600 px-3 py-1 text-xs text-white hover:bg-indigo-500 disabled:opacity-50" onclick={() => openBatch('download')} disabled={busy || batchBusy || !!sessionPort}>{$t('esim_batch_download')}</button>
          <button class="rounded bg-green-600 px-3 py-1 text-xs text-white hover:bg-green-500 disabled:opacity-50" onclick={() => startSimpleBatch('activate')} disabled={busy || batchBusy || !!sessionPort}>{$t('esim_batch_activate')}</button>
          <button class="rounded bg-slate-500 px-3 py-1 text-xs text-white hover:bg-slate-400 disabled:opacity-50" onclick={() => startSimpleBatch('deactivate')} disabled={busy || batchBusy || !!sessionPort}>{$t('esim_batch_deactivate')}</button>
          <button class="rounded bg-red-500 px-3 py-1 text-xs text-white hover:bg-red-400 disabled:opacity-50" onclick={() => startSimpleBatch('delete')} disabled={busy || batchBusy || !!sessionPort}>{$t('esim_batch_delete')}</button>
          <button class="ml-auto rounded bg-white px-3 py-1 text-xs text-slate-600 hover:bg-slate-100" onclick={clearSelection}>{$t('esim_batch_clear')}</button>
        </div>
      {/if}
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
              <th class="px-3 py-2 w-8">
                <input type="checkbox" checked={allFilteredSelected} onchange={toggleSelectAll} aria-label="select all" />
              </th>
              <th class="px-4 py-2">{$t('esim_col_port')}</th>
              <th class="px-4 py-2">{$t('esim_col_capable')}</th>
              <th class="px-4 py-2">{$t('esim_col_mode')}</th>
              <th class="px-4 py-2">{$t('esim_col_sim')}</th>
              <th class="px-4 py-2 text-right">{$t('esim_col_actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each filteredPorts as p (p.com_port)}
              <tr class="border-t border-slate-100 {selected.has(p.com_port) ? 'bg-indigo-50/50' : ''}">
                <td class="px-3 py-2">
                  <input
                    type="checkbox"
                    checked={selected.has(p.com_port)}
                    disabled={!p.esim_capable}
                    onchange={() => toggleSelect(p.com_port)}
                    aria-label={`select ${p.com_port}`}
                  />
                </td>
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

    <!-- Batch progress panel -->
    {#if job}
      <div class="mt-6 rounded-lg border border-slate-200 bg-white p-4">
        <div class="mb-3 flex items-center justify-between">
          <h2 class="font-semibold text-slate-800">
            {$t('esim_batch_progress')} —
            <span class="text-sm font-normal text-slate-500">{$t('esim_batch_op_' + job.op)}</span>
            <span class="ml-2 rounded-full px-2 py-0.5 text-xs {job.status === 'running' ? 'bg-amber-100 text-amber-700' : job.status === 'cancelled' ? 'bg-slate-100 text-slate-600' : 'bg-green-100 text-green-700'}">
              {$t('esim_batch_status_' + job.status)}
            </span>
          </h2>
          <div class="flex gap-2">
            {#if jobRunning}
              <button class="rounded bg-red-500 px-3 py-1 text-xs text-white hover:bg-red-400" onclick={cancelBatch}>{$t('esim_batch_cancel')}</button>
            {:else}
              <button class="rounded bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200" onclick={() => (job = null)}>{$t('esim_batch_close')}</button>
            {/if}
          </div>
        </div>
        <table class="w-full text-sm">
          <thead class="bg-slate-100 text-left text-slate-600">
            <tr>
              <th class="px-3 py-2">{$t('esim_col_port')}</th>
              <th class="px-3 py-2">{$t('esim_col_state')}</th>
              <th class="px-3 py-2">{$t('esim_col_iccid')}</th>
              <th class="px-3 py-2">{$t('esim_batch_message')}</th>
            </tr>
          </thead>
          <tbody>
            {#each job.items as it (it.com_port)}
              <tr class="border-t border-slate-100">
                <td class="px-3 py-2 font-medium text-slate-800">{it.com_port}</td>
                <td class="px-3 py-2">
                  <span class="rounded-full px-2 py-0.5 text-xs {itemBadge(it.status)}">{$t('esim_batch_item_' + it.status)}</span>
                </td>
                <td class="px-3 py-2 font-mono text-xs">{it.iccid ?? '—'}</td>
                <td class="px-3 py-2 text-xs text-slate-500">{it.message ?? ''}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</div>

<!-- Batch download dialog -->
{#if batchMode === 'download'}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
    <div class="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-lg bg-white p-5 shadow-xl">
      <div class="mb-3 flex items-center justify-between">
        <h2 class="text-lg font-semibold text-slate-800">{$t('esim_batch_download_title')}</h2>
        <button class="rounded p-1 hover:bg-slate-100" onclick={closeBatch} aria-label="close">
          <Icon icon="mdi:close" width="20" />
        </button>
      </div>

      <!-- Source ingestion -->
      <div class="mb-4 rounded border border-slate-200 p-3">
        <div class="mb-2 flex flex-wrap items-center gap-2">
          <button class="rounded bg-slate-700 px-3 py-1 text-xs text-white hover:bg-slate-600 disabled:opacity-50" onclick={loadSources} disabled={batchBusy}>{$t('esim_src_scan')}</button>
          <label class="cursor-pointer rounded bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200">
            {$t('esim_src_upload')}
            <input type="file" class="hidden" multiple accept="image/*,.txt,.csv" onchange={uploadSources} />
          </label>
          <span class="text-xs text-slate-500">{$t('esim_src_count', { n: sources.length })}</span>
        </div>
        <div class="flex gap-2">
          <textarea class="h-16 flex-1 rounded border border-slate-300 px-2 py-1 text-xs font-mono" placeholder={$t('esim_src_paste_placeholder')} bind:value={pasteText}></textarea>
          <button class="self-start rounded bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200" onclick={addPasted}>{$t('esim_src_add')}</button>
        </div>
      </div>

      <!-- Options -->
      <div class="mb-3 flex flex-wrap gap-4 text-xs text-slate-600">
        <label class="flex items-center gap-1"><input type="checkbox" bind:checked={batchOptions.auto_enable} /> {$t('esim_opt_auto_enable')}</label>
        <label class="flex items-center gap-1"><input type="checkbox" bind:checked={batchOptions.replace_existing} /> {$t('esim_opt_replace')}</label>
        <label class="flex items-center gap-1"><input type="checkbox" bind:checked={batchOptions.stop_on_error} /> {$t('esim_opt_stop_on_error')}</label>
      </div>

      <!-- Pairing table -->
      <table class="w-full text-sm">
        <thead class="bg-slate-100 text-left text-slate-600">
          <tr>
            <th class="px-3 py-2">{$t('esim_col_port')}</th>
            <th class="px-3 py-2">{$t('esim_batch_assigned_code')}</th>
          </tr>
        </thead>
        <tbody>
          {#each selectedList as com (com)}
            <tr class="border-t border-slate-100">
              <td class="px-3 py-2 font-medium text-slate-800">{com}</td>
              <td class="px-3 py-2">
                <select class="w-full rounded border border-slate-300 px-2 py-1 text-xs" value={pairing[com] ?? ''} onchange={(e) => setPair(com, e.target.value)}>
                  <option value="">{$t('esim_batch_no_code')}</option>
                  {#each sources as c, i (i)}
                    <option value={i}>{codeLabel(c)} — {c.source}</option>
                  {/each}
                </select>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>

      <div class="mt-4 flex justify-end gap-2">
        <button class="rounded bg-slate-100 px-4 py-2 text-sm hover:bg-slate-200" onclick={closeBatch}>{$t('esim_batch_cancel')}</button>
        <button class="rounded bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-500 disabled:opacity-50" onclick={() => { closeBatch(); startDownload(); }} disabled={batchBusy}>{$t('esim_batch_start_download')}</button>
      </div>
    </div>
  </div>
{/if}
