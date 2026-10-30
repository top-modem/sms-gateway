<script>
  import { onMount, onDestroy } from 'svelte';
  import Icon from '@iconify/svelte';
  import { EventSourcePolyfill } from 'event-source-polyfill';
  import { apiClient, buildBasicAuthHeader } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();

  let ports = $state([]);
  let loading = $state(true);
  let error = $state('');
  let message = $state('');
  let busy = $state(false);
  let portStatus = $state({});

  // Filters + selection
  let filterText = $state('');
  let filterEsimOnly = $state(true);
  let selected = $state(new Set());

  // Manage drawer
  let managePort = $state(null);
  let inventory = $state(null);
  let dl = $state({ activation_code: '', confirmation_code: '', phone_number: '', enable: true });

  // Batch
  let batchMode = $state(null); // null | 'download' | 'activate' | 'delete'
  let sources = $state([]);
  let pairing = $state({});
  let pasteText = $state('');
  let batchOptions = $state({ auto_enable: true, replace_existing: false, stop_on_error: false });
  let batchInventories = $state({}); // com -> inventory
  let batchTargets = $state({}); // com -> iccid
  let deleteAll = $state(false);
  let deleteConfirm = $state('');
  let job = $state(null);
  let batchBusy = $state(false);
  let sseConn = null;

  const sessionPort = $derived(ports.find((p) => p.session_active)?.com_port ?? null);
  const jobRunning = $derived(job?.status === 'running');
  const locked = $derived(busy || batchBusy || jobRunning);

  const filteredPorts = $derived(
    ports.filter((p) => {
      if (filterEsimOnly && !p.esim_capable) return false;
      const q = filterText.trim().toLowerCase();
      if (!q) return true;
      return `${p.com_port} ${p.sim_id ?? ''} ${p.eid ?? ''} ${p.active_iccid ?? ''} ${p.active_name ?? ''}`
        .toLowerCase()
        .includes(q);
    })
  );
  const selectableFiltered = $derived(filteredPorts.filter((p) => p.esim_capable));
  const allSelected = $derived(
    selectableFiltered.length > 0 && selectableFiltered.every((p) => selected.has(p.com_port))
  );
  const selectedList = $derived([...selected].sort((a, b) => comNum(a) - comNum(b)));
  const manageInfo = $derived(ports.find((p) => p.com_port === managePort) ?? null);
  const inSession = $derived(!!managePort && sessionPort === managePort);
  const profiles = $derived(inventory?.profiles ?? []);
  const maxProfiles = $derived(inventory?.max_profiles ?? 20);
  const atLimit = $derived(profiles.length >= maxProfiles);

  function comNum(com) {
    const m = String(com).match(/(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
  }

  /** FetchApi rejects with {status, data}; pull the message out of the body. */
  function describeError(e) {
    return e?.data?.error ?? e?.data?.message ?? e?.message ?? (e?.status ? `HTTP ${e.status}` : String(e));
  }

  function unwrap(res) {
    const body = res?.data;
    return body && typeof body === 'object' && 'data' in body ? body.data : body;
  }

  function flash(msg) {
    message = msg;
    setTimeout(() => (message = ''), 4000);
  }

  function fail(e) {
    error = $t('esim_op_failed', { msg: describeError(e) });
    setTimeout(() => (error = ''), 8000);
  }

  function setPortStatus(com, status, op, msg = '') {
    portStatus = { ...portStatus, [com]: { status, op, msg } };
  }

  async function loadPorts() {
    loading = ports.length === 0;
    try {
      const payload = unwrap(await apiClient.esimListPorts());
      ports = Array.isArray(payload) ? payload : [];
    } catch (e) {
      fail(e);
    } finally {
      loading = false;
    }
  }

  async function loadInventory(com) {
    const inv = unwrap(await apiClient.esimInventory(com));
    if (managePort === com) inventory = inv;
    return inv;
  }

  // ── Status column ────────────────────────────────────────────────────────
  function actionText(op) {
    if (!op) return '';
    const key = 'esim_status_action_' + op;
    const translated = $t(key);
    return translated === key ? op : translated;
  }

  function statusMeta(p) {
    const item = job?.items?.find((it) => it.com_port === p.com_port);
    if (item) {
      return { status: item.status, label: $t('esim_batch_item_' + item.status), detail: item.message ?? '' };
    }
    const local = portStatus[p.com_port];
    if (local) {
      const label = local.status === 'running' ? $t('esim_status_running') : local.status === 'ok' ? $t('esim_status_ok') : $t('esim_status_failed');
      return { status: local.status, label, detail: local.msg ? `${actionText(local.op)}: ${local.msg}` : actionText(local.op) };
    }
    if (p.session_active || p.esim_mode) {
      return { status: 'session', label: $t('esim_status_session'), detail: $t('esim_mode_esim') };
    }
    if (p.last_status) {
      const st = String(p.last_status).toLowerCase() === 'ok' ? 'ok' : 'failed';
      const detail = [actionText(p.last_op), st === 'failed' ? p.last_message : '', p.last_at].filter(Boolean).join(' | ');
      return { status: st, label: st === 'ok' ? $t('esim_status_ok') : $t('esim_status_failed'), detail };
    }
    return { status: 'idle', label: $t('esim_status_idle'), detail: '' };
  }

  function badge(status) {
    switch (status) {
      case 'ok':
        return 'bg-green-100 text-green-700';
      case 'failed':
        return 'bg-red-100 text-red-700';
      case 'running':
      case 'session':
        return 'bg-amber-100 text-amber-700';
      default:
        return 'bg-slate-100 text-slate-500';
    }
  }

  // ── Single-port operations ───────────────────────────────────────────────
  async function runOp(com, op, fn) {
    busy = true;
    setPortStatus(com, 'running', op);
    try {
      const result = await fn();
      setPortStatus(com, 'ok', op);
      flash($t('esim_op_ok'));
      return result;
    } catch (e) {
      setPortStatus(com, 'failed', op, describeError(e));
      fail(e);
      return undefined;
    } finally {
      busy = false;
      await loadPorts();
      if (managePort === com) await loadInventory(com).catch(() => {});
    }
  }

  async function ensureSession(com) {
    if (sessionPort === com) return;
    await apiClient.esimEnter(com);
    await loadPorts();
  }

  async function openManage(com) {
    managePort = com;
    inventory = null;
    dl = { activation_code: '', confirmation_code: '', phone_number: '', enable: true };
    try {
      const inv = await loadInventory(com);
      if (!inv?.profiles_refreshed_at) await readCard();
    } catch (e) {
      fail(e);
    }
  }

  async function closeManage() {
    const com = managePort;
    if (com && sessionPort === com) {
      await runOp(com, 'exit', () => apiClient.esimExit(com));
    }
    managePort = null;
    inventory = null;
  }

  async function readCard() {
    const com = managePort;
    if (!com) return;
    await runOp(com, 'refresh', async () => {
      if (sessionPort === com) {
        await apiClient.esimChip(com).catch(() => {});
        await apiClient.esimProfiles(com);
      } else {
        await apiClient.esimRefreshInventory(com);
      }
    });
  }

  async function exitSession(com) {
    await runOp(com, 'exit', () => apiClient.esimExit(com));
  }

  async function resetPort(com) {
    await runOp(com, 'reset', () => apiClient.esimReset(com));
  }

  async function enableProfile(p) {
    const com = managePort;
    await runOp(com, 'enable', async () => {
      await ensureSession(com);
      await apiClient.esimEnable(com, p.iccid, true);
    });
  }

  async function disableProfile(p) {
    const com = managePort;
    await runOp(com, 'disable', async () => {
      await ensureSession(com);
      await apiClient.esimDisable(com, p.iccid, true);
    });
  }

  async function renameProfile(p) {
    const name = prompt($t('esim_nickname_prompt'), p.nickname ?? '');
    if (name == null) return;
    const com = managePort;
    await runOp(com, 'nickname', async () => {
      await ensureSession(com);
      await apiClient.esimNickname(com, p.iccid, name.trim());
    });
  }

  async function deleteProfile(p) {
    if (!confirm($t('esim_confirm_delete_profile', { name: p.display_name, iccid: p.iccid }))) return;
    const com = managePort;
    await runOp(com, 'delete', async () => {
      await ensureSession(com);
      await apiClient.esimDelete(com, p.iccid);
    });
  }

  async function editPhone(p) {
    const phone = prompt($t('esim_phone_prompt', { iccid: p.iccid }), p.phone_number ?? '');
    if (phone == null) return;
    try {
      await apiClient.esimSetMsisdn(p.iccid, phone.trim());
      await loadInventory(managePort);
      flash($t('esim_op_ok'));
    } catch (e) {
      fail(e);
    }
  }

  async function downloadProfile() {
    const com = managePort;
    const body = { activation_code: dl.activation_code.trim(), enable: dl.enable };
    if (dl.confirmation_code.trim()) body.confirmation_code = dl.confirmation_code.trim();
    if (dl.phone_number.trim()) body.phone_number = dl.phone_number.trim();
    const res = await runOp(com, 'download', async () => {
      await ensureSession(com);
      return unwrap(await apiClient.esimDownload(com, body));
    });
    if (res) {
      dl = { activation_code: '', confirmation_code: '', phone_number: '', enable: dl.enable };
      if (res.enable_error) fail({ message: res.enable_error });
    }
  }

  // ── Selection ────────────────────────────────────────────────────────────
  function toggleSelect(com) {
    const s = new Set(selected);
    s.has(com) ? s.delete(com) : s.add(com);
    selected = s;
  }
  function toggleSelectAll() {
    const s = new Set(selected);
    for (const p of selectableFiltered) allSelected ? s.delete(p.com_port) : s.add(p.com_port);
    selected = s;
  }

  // ── Batch: activation sources ────────────────────────────────────────────
  async function loadSources() {
    batchBusy = true;
    try {
      sources = unwrap(await apiClient.esimScanSources()) ?? [];
      buildPairing();
    } catch (e) {
      fail(e);
    } finally {
      batchBusy = false;
    }
  }

  async function uploadSources(ev) {
    const files = ev.target?.files;
    if (!files?.length) return;
    batchBusy = true;
    try {
      sources = [...sources, ...(unwrap(await apiClient.esimUploadSources(files)) ?? [])];
      buildPairing();
    } catch (e) {
      fail(e);
    } finally {
      batchBusy = false;
      ev.target.value = '';
    }
  }

  function addPasted() {
    for (const line of pasteText.split(/\r?\n/)) {
      const m = line.match(/LPA:1\$([^$\s]+)\$([^$\s]+)(?:\$([^$\s]+))?/i);
      if (m) {
        sources = [
          ...sources,
          { raw_lpa: m[0], smdp: m[1], matching_id: m[2], confirmation_code: m[3] || null, source: 'manual', port_hint: null, label: null },
        ];
      }
    }
    pasteText = '';
    buildPairing();
  }

  /** Pair codes to selected ports: filename hint first, then in order. */
  function buildPairing() {
    const map = {};
    const used = new Set();
    for (const com of selectedList) {
      const idx = sources.findIndex((c, i) => !used.has(i) && c.port_hint === comNum(com));
      if (idx >= 0) {
        map[com] = { code: idx, phone: phoneFromLabel(sources[idx].label) };
        used.add(idx);
      }
    }
    for (const com of selectedList) {
      if (map[com]) continue;
      const idx = sources.findIndex((_, i) => !used.has(i));
      map[com] = { code: idx >= 0 ? idx : null, phone: idx >= 0 ? phoneFromLabel(sources[idx].label) : '' };
      if (idx >= 0) used.add(idx);
    }
    pairing = map;
  }

  function phoneFromLabel(label) {
    return label ? String(label).replace(/\D/g, '').replace(/^0+/, '') : '';
  }

  function codeLabel(c) {
    return `${c.smdp} / ${c.matching_id}${c.label ? ' (' + c.label + ')' : ''}`;
  }

  // ── Batch: dialogs and run ───────────────────────────────────────────────
  async function openBatch(op) {
    if (sessionPort) {
      fail({ message: $t('esim_batch_blocked_session', { port: sessionPort }) });
      return;
    }
    batchMode = op;
    if (op === 'download') {
      sources = [];
      await loadSources();
    } else {
      deleteAll = false;
      deleteConfirm = '';
      const invs = {};
      const targets = {};
      await Promise.all(
        selectedList.map(async (com) => {
          try {
            invs[com] = unwrap(await apiClient.esimInventory(com));
          } catch (_) {
            invs[com] = null;
          }
          const list = invs[com]?.profiles ?? [];
          targets[com] = op === 'activate' && list.length === 1 ? list[0].iccid : '';
        })
      );
      batchInventories = invs;
      batchTargets = targets;
    }
  }

  function closeBatch() {
    batchMode = null;
    pasteText = '';
  }

  async function startDownload() {
    const items = selectedList.map((com) => {
      const pair = pairing[com];
      const code = pair?.code != null ? sources[pair.code] : null;
      return { com_port: com, activation_code: code?.raw_lpa ?? null, phone_number: pair?.phone || null };
    });
    closeBatch();
    await runBatch('download', items, batchOptions);
  }

  async function startProfileBatch() {
    const op = batchMode;
    if (op === 'delete' && deleteAll) {
      if (deleteConfirm.trim() !== 'DELETE') return;
      closeBatch();
      await runBatch('delete', selectedList.map((com) => ({ com_port: com })), { all_profiles: true });
      return;
    }
    const items = selectedList
      .filter((com) => batchTargets[com])
      .map((com) => ({ com_port: com, iccid: batchTargets[com] }));
    if (items.length === 0) return;
    if (op === 'delete' && !confirm($t('esim_batch_confirm_delete', { n: items.length }))) return;
    closeBatch();
    await runBatch(op, items, {});
  }

  async function startSimpleBatch(op) {
    if (op === 'deactivate' && !confirm($t('esim_batch_confirm_deactivate', { n: selected.size }))) return;
    await runBatch(op, selectedList.map((com) => ({ com_port: com })), {});
  }

  async function runBatch(op, items, options) {
    batchBusy = true;
    try {
      await apiClient.esimStartBatch({ op, items, stop_on_error: batchOptions.stop_on_error, ...options });
      connectBatchSse();
      const snap = unwrap(await apiClient.esimGetBatch());
      if (snap) job = snap;
      flash($t('esim_batch_started'));
    } catch (e) {
      fail(e);
    } finally {
      batchBusy = false;
    }
  }

  function connectBatchSse() {
    if (sseConn) sseConn.close();
    sseConn = new EventSourcePolyfill('/api/esim/batch/events', { headers: buildBasicAuthHeader(), heartbeatTimeout: 120000 });
    sseConn.addEventListener('batch', (e) => {
      try {
        job = JSON.parse(e.data);
        if (job.status !== 'running') loadPorts();
      } catch (_) {}
    });
    sseConn.onerror = () => {
      sseConn?.close();
      sseConn = null;
    };
  }

  async function cancelBatch() {
    try {
      await apiClient.esimCancelBatch();
    } catch (e) {
      fail(e);
    }
  }

  onMount(async () => {
    await loadPorts();
    try {
      const snap = unwrap(await apiClient.esimGetBatch());
      if (snap) {
        job = snap;
        if (snap.status === 'running') connectBatchSse();
      }
    } catch (_) {}
  });
  onDestroy(() => sseConn?.close());
</script>

<div class="flex flex-col h-dvh w-screen bg-gray-50 dark:bg-zinc-950 text-sm font-sans">
  <header class="flex items-center justify-between gap-3 px-4 py-3 bg-white dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-800 shadow-sm shrink-0">
    <div class="flex items-center gap-3">
      <button
        onclick={onBack}
        class="inline-flex items-center gap-1.5 h-9 px-2.5 rounded-full bg-blue-600 text-white shadow-sm shadow-blue-600/30 hover:bg-blue-700 transition"
        aria-label="Back"
      >
        <Icon icon="carbon:arrow-left" class="w-4 h-4" />
        <span class="text-xs font-semibold">{$t('btn_back')}</span>
      </button>
      <div class="flex items-center gap-2">
        <Icon icon="carbon:sim-card" class="w-5 h-5 text-gray-500 dark:text-gray-400" />
        <h1 class="text-base font-semibold text-gray-800 dark:text-gray-100">{$t('esim_title')}</h1>
      </div>
    </div>
    <button
      class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border border-gray-200 dark:border-zinc-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-zinc-800 transition disabled:opacity-50"
      onclick={loadPorts}
      disabled={busy}
    >
      <Icon icon="carbon:renew" class="w-4 h-4" />
      {$t('esim_refresh')}
    </button>
  </header>

  <div class="flex-1 overflow-auto p-4 sm:p-6">
    <p class="mb-3 text-sm text-gray-500">{$t('esim_subtitle')}</p>

    <div class="mb-3 flex items-center justify-between gap-3 rounded-lg border px-4 py-2 {sessionPort ? 'border-amber-300 bg-amber-50 text-amber-800' : 'border-gray-200 bg-white text-gray-500'}">
      <span>{sessionPort ? $t('esim_lock_banner', { port: sessionPort }) : $t('esim_lock_free')}</span>
      {#if sessionPort}
        <button class="rounded bg-slate-700 px-2 py-1 text-xs text-white hover:bg-slate-600 disabled:opacity-50" onclick={() => exitSession(sessionPort)} disabled={busy}>
          {$t('esim_exit')}
        </button>
      {/if}
    </div>

    {#if message}
      <div class="mb-3 rounded-lg bg-green-50 px-4 py-2 text-green-700">{message}</div>
    {/if}
    {#if error}
      <div class="mb-3 rounded-lg bg-red-50 px-4 py-2 text-red-700">{error}</div>
    {/if}

    <!-- Filters -->
    <div class="mb-3 flex flex-wrap items-center gap-3 rounded-lg border border-slate-200 bg-white px-3 py-2">
      <div class="relative">
        <Icon icon="mdi:magnify" width="16" class="absolute left-2 top-2 text-slate-400" />
        <input class="w-56 rounded border border-slate-300 py-1 pl-7 pr-2" placeholder={$t('esim_filter_placeholder')} bind:value={filterText} />
      </div>
      <label class="flex items-center gap-1 text-xs text-slate-600">
        <input type="checkbox" bind:checked={filterEsimOnly} /> {$t('esim_filter_capable')}
      </label>
      <span class="ml-auto text-xs text-slate-500">{$t('esim_selected_count', { n: selected.size })}</span>
    </div>

    <!-- Batch toolbar -->
    {#if selected.size > 0}
      <div class="mb-3 flex flex-wrap items-center gap-2 rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2">
        <span class="font-medium text-indigo-800">{$t('esim_batch_bar', { n: selected.size })}</span>
        <button class="rounded bg-slate-700 px-3 py-1 text-xs text-white hover:bg-slate-600 disabled:opacity-50" onclick={() => runBatch('refresh', selectedList.map((com) => ({ com_port: com })), {})} disabled={locked || !!sessionPort}>{$t('esim_batch_refresh')}</button>
        <button class="rounded bg-indigo-600 px-3 py-1 text-xs text-white hover:bg-indigo-500 disabled:opacity-50" onclick={() => openBatch('download')} disabled={locked || !!sessionPort}>{$t('esim_batch_download')}</button>
        <button class="rounded bg-green-600 px-3 py-1 text-xs text-white hover:bg-green-500 disabled:opacity-50" onclick={() => openBatch('activate')} disabled={locked || !!sessionPort}>{$t('esim_batch_activate')}</button>
        <button class="rounded bg-slate-500 px-3 py-1 text-xs text-white hover:bg-slate-400 disabled:opacity-50" onclick={() => startSimpleBatch('deactivate')} disabled={locked || !!sessionPort}>{$t('esim_batch_deactivate')}</button>
        <button class="rounded bg-red-500 px-3 py-1 text-xs text-white hover:bg-red-400 disabled:opacity-50" onclick={() => openBatch('delete')} disabled={locked || !!sessionPort}>{$t('esim_batch_delete')}</button>
        <button class="ml-auto rounded bg-white px-3 py-1 text-xs text-slate-600 hover:bg-slate-100" onclick={() => (selected = new Set())}>{$t('esim_batch_clear')}</button>
      </div>
    {/if}

    {#if loading}
      <p class="text-slate-500">…</p>
    {:else if filteredPorts.length === 0}
      <p class="rounded-lg bg-white p-6 text-center text-slate-500">{$t('esim_no_ports')}</p>
    {:else}
      <div class="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table class="w-full">
          <thead class="bg-slate-100 text-left text-slate-600">
            <tr>
              <th class="w-8 px-3 py-2"><input type="checkbox" checked={allSelected} onchange={toggleSelectAll} aria-label="select all" /></th>
              <th class="px-3 py-2">{$t('esim_col_port')}</th>
              <th class="px-3 py-2">{$t('esim_col_mode')}</th>
              <th class="px-3 py-2">{$t('esim_col_eid')}</th>
              <th class="px-3 py-2">{$t('esim_col_profiles')}</th>
              <th class="px-3 py-2">{$t('esim_col_active')}</th>
              <th class="px-3 py-2">{$t('esim_col_status')}</th>
              <th class="px-3 py-2">{$t('esim_col_actions')}</th>
            </tr>
          </thead>
          <tbody>
            {#each filteredPorts as p (p.com_port)}
              {@const st = statusMeta(p)}
              <tr class="border-t border-slate-100 {selected.has(p.com_port) ? 'bg-indigo-50/50' : ''} {managePort === p.com_port ? 'ring-1 ring-inset ring-indigo-300' : ''}">
                <td class="px-3 py-2">
                  <input type="checkbox" checked={selected.has(p.com_port)} disabled={!p.esim_capable} onchange={() => toggleSelect(p.com_port)} aria-label={`select ${p.com_port}`} />
                </td>
                <td class="px-3 py-2 font-medium text-slate-800">{p.com_port}</td>
                <td class="px-3 py-2">
                  {#if !p.esim_capable}
                    <span class="text-xs text-slate-400">{p.machine_type}</span>
                  {:else}
                    <span class="rounded-full px-2 py-0.5 text-xs {p.esim_mode ? 'bg-amber-100 text-amber-700' : 'bg-slate-100 text-slate-600'}">
                      {p.esim_mode ? $t('esim_mode_esim') : $t('esim_mode_normal')}
                    </span>
                  {/if}
                </td>
                <td class="px-3 py-2 font-mono text-xs text-slate-500" title={p.eid ?? ''}>{p.eid ? '…' + p.eid.slice(-8) : '—'}</td>
                <td class="px-3 py-2 text-slate-600">{p.esim_capable && p.profiles_refreshed_at ? `${p.profile_count} / 20` : '—'}</td>
                <td class="px-3 py-2">
                  {#if p.active_iccid}
                    <div class="max-w-[200px] truncate text-slate-800" title={p.active_name}>{p.active_name}</div>
                    <div class="font-mono text-[11px] text-slate-400">{p.active_iccid}</div>
                  {:else if p.esim_capable && p.profiles_refreshed_at}
                    <span class="text-xs text-slate-400">{$t('esim_no_active')}</span>
                  {:else}
                    <span class="text-slate-400">—</span>
                  {/if}
                </td>
                <td class="px-3 py-2">
                  <span class="rounded-full px-2 py-0.5 text-xs {badge(st.status)}">{st.label}</span>
                  {#if st.detail}
                    <div class="mt-0.5 max-w-[240px] truncate text-[11px] text-slate-500" title={st.detail}>{st.detail}</div>
                  {/if}
                </td>
                <td class="px-3 py-2 whitespace-nowrap">
                  <button class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-100 disabled:opacity-50" onclick={() => openManage(p.com_port)} disabled={!p.esim_capable || locked}>
                    {$t('esim_manage')}
                  </button>
                  <button class="ml-1 rounded bg-slate-500 px-2 py-1 text-xs text-white hover:bg-slate-400 disabled:opacity-50" onclick={() => resetPort(p.com_port)} disabled={!p.esim_capable || locked}>
                    {$t('esim_status_action_reset')}
                  </button>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      <p class="mt-2 text-xs text-amber-600">{$t('esim_warn_sms_paused')}</p>
    {/if}

    <!-- Batch progress -->
    {#if job}
      <div class="mt-6 rounded-lg border border-slate-200 bg-white p-4">
        <div class="mb-3 flex items-center justify-between">
          <h2 class="font-semibold text-slate-800">
            {$t('esim_batch_progress')} — <span class="font-normal text-slate-500">{$t('esim_batch_op_' + job.op)}</span>
            <span class="ml-2 rounded-full px-2 py-0.5 text-xs {jobRunning ? 'bg-amber-100 text-amber-700' : job.status === 'cancelled' ? 'bg-slate-100 text-slate-600' : 'bg-green-100 text-green-700'}">
              {$t('esim_batch_status_' + job.status)}
            </span>
          </h2>
          {#if jobRunning}
            <button class="rounded bg-red-500 px-3 py-1 text-xs text-white hover:bg-red-400" onclick={cancelBatch}>{$t('esim_batch_cancel')}</button>
          {:else}
            <button class="rounded bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200" onclick={() => (job = null)}>{$t('esim_batch_close')}</button>
          {/if}
        </div>
        <table class="w-full">
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
                <td class="px-3 py-2 font-medium">{it.com_port}</td>
                <td class="px-3 py-2"><span class="rounded-full px-2 py-0.5 text-xs {badge(it.status)}">{$t('esim_batch_item_' + it.status)}</span></td>
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

<!-- Manage drawer -->
{#if managePort}
  <div class="fixed inset-0 z-40 flex justify-end bg-black/30">
    <div class="flex h-full w-full max-w-3xl flex-col bg-white shadow-xl">
      <div class="flex items-center justify-between border-b border-slate-200 px-5 py-3">
        <div>
          <h2 class="text-base font-semibold text-slate-800">{$t('esim_drawer_title', { port: managePort })}</h2>
          <div class="mt-0.5 text-xs text-slate-500">
            {$t('esim_eid')}: <span class="font-mono">{inventory?.eid ?? manageInfo?.eid ?? '—'}</span>
          </div>
        </div>
        <div class="flex items-center gap-2">
          {#if inSession}
            <span class="rounded-full bg-amber-100 px-2 py-0.5 text-xs text-amber-700">{$t('esim_mode_esim')}</span>
          {/if}
          <button class="inline-flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-100 disabled:opacity-50" onclick={readCard} disabled={busy}>
            <Icon icon="carbon:renew" class="h-4 w-4" />{$t('esim_read_card')}
          </button>
          <button class="rounded bg-slate-700 px-3 py-1 text-xs text-white hover:bg-slate-600 disabled:opacity-50" onclick={closeManage} disabled={busy}>
            {inSession ? $t('esim_close_exit') : $t('esim_batch_close')}
          </button>
        </div>
      </div>

      <div class="flex-1 overflow-y-auto px-5 py-4">
        <div class="mb-3 text-xs text-slate-500">
          {inventory?.profiles_refreshed_at ? $t('esim_last_read', { time: inventory.profiles_refreshed_at }) : $t('esim_never_read')}
          · {profiles.length} / {maxProfiles}
        </div>
        <p class="mb-2 text-xs text-slate-500">{$t('esim_one_active_hint')}</p>
        {#if inSession}
          <p class="mb-3 rounded bg-amber-50 px-3 py-1.5 text-xs text-amber-700">{$t('esim_apply_hint')}</p>
        {/if}

        <div class="mb-5 overflow-hidden rounded-lg border border-slate-200">
          <table class="w-full">
            <thead class="bg-slate-50 text-left text-xs text-slate-600">
              <tr>
                <th class="px-3 py-2">{$t('esim_col_name')}</th>
                <th class="px-3 py-2">{$t('esim_col_iccid')}</th>
                <th class="px-3 py-2">{$t('esim_col_state')}</th>
                <th class="px-3 py-2">{$t('esim_col_phone')}</th>
                <th class="px-3 py-2">{$t('esim_col_actions')}</th>
              </tr>
            </thead>
            <tbody>
              {#each profiles as p (p.iccid)}
                <tr class="border-t border-slate-100 {p.state === 'enabled' ? 'bg-green-50/50' : ''}">
                  <td class="px-3 py-2">
                    <div class="font-medium text-slate-800">{p.display_name}</div>
                    {#if p.service_provider_name && p.service_provider_name !== p.display_name}
                      <div class="text-[11px] text-slate-500">{p.service_provider_name}</div>
                    {/if}
                  </td>
                  <td class="px-3 py-2 font-mono text-xs">{p.iccid}</td>
                  <td class="px-3 py-2">
                    <span class="rounded-full px-2 py-0.5 text-xs {p.state === 'enabled' ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-500'}">
                      {p.state === 'enabled' ? $t('esim_state_enabled') : $t('esim_state_disabled')}
                    </span>
                  </td>
                  <td class="px-3 py-2 text-xs">{p.phone_number ?? '—'}</td>
                  <td class="px-3 py-2 whitespace-nowrap text-xs">
                    {#if p.state === 'enabled'}
                      <button class="rounded bg-slate-500 px-2 py-1 text-white hover:bg-slate-400 disabled:opacity-50" onclick={() => disableProfile(p)} disabled={busy}>{$t('esim_disable')}</button>
                    {:else}
                      <button class="rounded bg-green-600 px-2 py-1 text-white hover:bg-green-500 disabled:opacity-50" onclick={() => enableProfile(p)} disabled={busy}>{$t('esim_enable')}</button>
                    {/if}
                    <button class="rounded border border-slate-300 px-2 py-1 hover:bg-slate-100 disabled:opacity-50" onclick={() => renameProfile(p)} disabled={busy}>{$t('esim_rename')}</button>
                    <button class="rounded border border-slate-300 px-2 py-1 hover:bg-slate-100 disabled:opacity-50" onclick={() => editPhone(p)} disabled={busy}>{$t('esim_set_phone')}</button>
                    <button class="rounded bg-red-500 px-2 py-1 text-white hover:bg-red-400 disabled:opacity-50" onclick={() => deleteProfile(p)} disabled={busy}>{$t('esim_delete')}</button>
                  </td>
                </tr>
              {:else}
                <tr><td colspan="5" class="px-3 py-6 text-center text-slate-400">{$t('esim_no_profiles')}</td></tr>
              {/each}
            </tbody>
          </table>
        </div>

        <div class="rounded-lg border border-slate-200 p-4">
          <h3 class="mb-3 font-semibold text-slate-800">{$t('esim_add_profile')}</h3>
          {#if atLimit}
            <p class="mb-3 rounded bg-red-50 px-3 py-1.5 text-xs text-red-700">{$t('esim_limit_reached', { max: maxProfiles })}</p>
          {/if}
          <label class="mb-1 block text-xs text-slate-600" for="dl-code">{$t('esim_field_activation')}</label>
          <input id="dl-code" class="mb-3 w-full rounded border border-slate-300 px-2 py-1.5 font-mono text-xs" placeholder="LPA:1$smdp.example.com$MATCHING-ID" bind:value={dl.activation_code} />
          <div class="mb-3 grid grid-cols-2 gap-3">
            <div>
              <label class="mb-1 block text-xs text-slate-600" for="dl-conf">{$t('esim_field_confirmation')}</label>
              <input id="dl-conf" type="password" class="w-full rounded border border-slate-300 px-2 py-1.5 text-xs" bind:value={dl.confirmation_code} />
            </div>
            <div>
              <label class="mb-1 block text-xs text-slate-600" for="dl-phone">{$t('esim_field_phone')}</label>
              <input id="dl-phone" type="tel" class="w-full rounded border border-slate-300 px-2 py-1.5 text-xs" bind:value={dl.phone_number} />
            </div>
          </div>
          <div class="flex items-center justify-between">
            <label class="flex items-center gap-1 text-xs text-slate-600">
              <input type="checkbox" bind:checked={dl.enable} /> {$t('esim_enable_after_download')}
            </label>
            <button class="rounded bg-indigo-600 px-4 py-1.5 text-xs text-white hover:bg-indigo-500 disabled:opacity-50" onclick={downloadProfile} disabled={busy || atLimit || !dl.activation_code.trim()}>
              {$t('esim_download_btn')}
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
{/if}

<!-- Batch download dialog -->
{#if batchMode === 'download'}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
    <div class="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-lg bg-white p-5 shadow-xl">
      <div class="mb-3 flex items-center justify-between">
        <h2 class="text-lg font-semibold text-slate-800">{$t('esim_batch_download_title')}</h2>
        <button class="rounded p-1 hover:bg-slate-100" onclick={closeBatch} aria-label="close"><Icon icon="mdi:close" width="20" /></button>
      </div>

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
          <textarea class="h-16 flex-1 rounded border border-slate-300 px-2 py-1 font-mono text-xs" placeholder={$t('esim_src_paste_placeholder')} bind:value={pasteText}></textarea>
          <button class="self-start rounded bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200" onclick={addPasted}>{$t('esim_src_add')}</button>
        </div>
      </div>

      <div class="mb-3 flex flex-wrap gap-4 text-xs text-slate-600">
        <label class="flex items-center gap-1"><input type="checkbox" bind:checked={batchOptions.auto_enable} /> {$t('esim_opt_auto_enable')}</label>
        <label class="flex items-center gap-1 text-red-600"><input type="checkbox" bind:checked={batchOptions.replace_existing} /> {$t('esim_opt_replace_warn')}</label>
        <label class="flex items-center gap-1"><input type="checkbox" bind:checked={batchOptions.stop_on_error} /> {$t('esim_opt_stop_on_error')}</label>
      </div>

      <table class="w-full">
        <thead class="bg-slate-100 text-left text-slate-600">
          <tr>
            <th class="px-3 py-2">{$t('esim_col_port')}</th>
            <th class="px-3 py-2">{$t('esim_batch_assigned_code')}</th>
            <th class="px-3 py-2">{$t('esim_col_phone')}</th>
          </tr>
        </thead>
        <tbody>
          {#each selectedList as com (com)}
            <tr class="border-t border-slate-100">
              <td class="px-3 py-2 font-medium">{com}</td>
              <td class="px-3 py-2">
                <select
                  class="w-full rounded border border-slate-300 px-2 py-1 text-xs"
                  value={pairing[com]?.code ?? ''}
                  onchange={(e) => (pairing = { ...pairing, [com]: { ...pairing[com], code: e.target.value === '' ? null : Number(e.target.value) } })}
                >
                  <option value="">{$t('esim_batch_no_code')}</option>
                  {#each sources as c, i (i)}
                    <option value={i}>{codeLabel(c)} — {c.source}</option>
                  {/each}
                </select>
              </td>
              <td class="px-3 py-2">
                <input
                  class="w-36 rounded border border-slate-300 px-2 py-1 text-xs"
                  value={pairing[com]?.phone ?? ''}
                  oninput={(e) => (pairing = { ...pairing, [com]: { ...pairing[com], phone: e.target.value } })}
                />
              </td>
            </tr>
          {/each}
        </tbody>
      </table>

      <div class="mt-4 flex justify-end gap-2">
        <button class="rounded bg-slate-100 px-4 py-2 hover:bg-slate-200" onclick={closeBatch}>{$t('esim_batch_cancel')}</button>
        <button class="rounded bg-indigo-600 px-4 py-2 text-white hover:bg-indigo-500 disabled:opacity-50" onclick={startDownload} disabled={batchBusy}>{$t('esim_batch_start_download')}</button>
      </div>
    </div>
  </div>
{/if}

<!-- Batch enable / delete dialog -->
{#if batchMode === 'activate' || batchMode === 'delete'}
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
    <div class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white p-5 shadow-xl">
      <div class="mb-3 flex items-center justify-between">
        <h2 class="text-lg font-semibold text-slate-800">{batchMode === 'activate' ? $t('esim_batch_enable_title') : $t('esim_batch_delete_title')}</h2>
        <button class="rounded p-1 hover:bg-slate-100" onclick={closeBatch} aria-label="close"><Icon icon="mdi:close" width="20" /></button>
      </div>

      {#if batchMode === 'delete'}
        <label class="mb-2 flex items-center gap-1 text-xs text-red-600">
          <input type="checkbox" bind:checked={deleteAll} /> {$t('esim_batch_delete_all')}
        </label>
        {#if deleteAll}
          <input class="mb-3 w-48 rounded border border-red-300 px-2 py-1 text-xs" placeholder={$t('esim_batch_type_delete')} bind:value={deleteConfirm} />
        {/if}
      {/if}

      {#if !(batchMode === 'delete' && deleteAll)}
        <table class="w-full">
          <thead class="bg-slate-100 text-left text-slate-600">
            <tr>
              <th class="px-3 py-2">{$t('esim_col_port')}</th>
              <th class="px-3 py-2">{$t('esim_col_name')}</th>
            </tr>
          </thead>
          <tbody>
            {#each selectedList as com (com)}
              {@const list = batchInventories[com]?.profiles ?? []}
              <tr class="border-t border-slate-100">
                <td class="px-3 py-2 font-medium">{com}</td>
                <td class="px-3 py-2">
                  {#if list.length === 0}
                    <span class="text-xs text-slate-400">{$t('esim_batch_no_cache')}</span>
                  {:else}
                    <select class="w-full rounded border border-slate-300 px-2 py-1 text-xs" bind:value={batchTargets[com]}>
                      <option value="">{$t('esim_batch_choose_profile')}</option>
                      {#each list as p (p.iccid)}
                        <option value={p.iccid}>{p.display_name} · {p.iccid}{p.state === 'enabled' ? ' ✓' : ''}</option>
                      {/each}
                    </select>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}

      <div class="mt-4 flex justify-end gap-2">
        <button class="rounded bg-slate-100 px-4 py-2 hover:bg-slate-200" onclick={closeBatch}>{$t('esim_batch_cancel')}</button>
        <button
          class="rounded px-4 py-2 text-white disabled:opacity-50 {batchMode === 'delete' ? 'bg-red-500 hover:bg-red-400' : 'bg-green-600 hover:bg-green-500'}"
          onclick={startProfileBatch}
          disabled={batchBusy || (batchMode === 'delete' && deleteAll ? deleteConfirm.trim() !== 'DELETE' : !selectedList.some((com) => batchTargets[com]))}
        >
          {$t('esim_batch_run')}
        </button>
      </div>
    </div>
  </div>
{/if}
