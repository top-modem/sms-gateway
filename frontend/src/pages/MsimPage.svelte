<script>
  import { onMount, onDestroy, tick } from 'svelte';
  import Icon from '@iconify/svelte';
  import { EventSourcePolyfill } from 'event-source-polyfill';
  import { apiClient, buildBasicAuthHeader } from '../js/api.js';
  import { t } from '../js/i18n.js';

  let { onBack = () => {} } = $props();
  let ports = $state([]);
  let selectedPort = $state(null);
  let inventory = $state(null);
  let loading = $state(true);
  let busy = $state(false);
  let error = $state('');
  let message = $state('');
  let filter = $state('');
  let scanSlots = $state(null);
  let returnSlot = $state('');
  let phoneSlot = $state(null);
  let phoneNumber = $state('');
  let drawer = $state(null);
  let returnFocus = null;
  let stream = null;
  let streamId = null;
  let timer = null;
  let pending = false;
  let destroyed = false;

  const filtered = $derived(ports.filter(port => `${port.com_port} ${port.slots.map(slot => `${slot.iccid ?? ''} ${slot.phone_number ?? ''}`).join(' ')}`.toLowerCase().includes(filter.toLowerCase())));
  const operation = $derived(inventory?.operation ?? null);
  const running = $derived(operation?.status === 'running');
  const locked = $derived(busy || running);
  const hasRunning = $derived(ports.some(port => port.operation?.status === 'running'));

  function unwrap(response) { return response?.data?.data ?? response?.data; }
  function describe(failure) { return failure?.data?.error ?? failure?.message ?? String(failure); }
  function timestamp(value) { return value ? new Date(value.includes('T') ? value : value.replace(' ', 'T') + 'Z').toLocaleString() : '-'; }
  function slotLabel(slot) { return $t('msim_slot', { slot }); }
  function stageLabel(value) { return $t('msim_stage_' + value); }
  function clearStream() { stream?.close(); stream = null; streamId = null; }

  function connectStream() {
    if (!running || !operation?.id) { clearStream(); return; }
    if (streamId === operation.id) return;
    clearStream();
    streamId = operation.id;
    const connection = new EventSourcePolyfill(`/api/msim/operations/${operation.id}/events`, {
      headers: buildBasicAuthHeader(), heartbeatTimeout: 45000,
    });
    stream = connection;
    connection.addEventListener('operation', event => {
      if (stream !== connection || !inventory) return;
      try {
        const snapshot = JSON.parse(event.data);
        inventory = { ...inventory, operation: snapshot };
        if (snapshot.status !== 'running') { clearStream(); refresh(); }
      } catch (_) {}
    });
    connection.onerror = () => { if (stream === connection) refresh(); };
  }

  async function refresh() {
    if (pending || destroyed) return;
    pending = true;
    const portAtStart = selectedPort;
    try {
      const rows = unwrap(await apiClient.msimPorts()) ?? [];
      if (destroyed) return;
      ports = rows;
      if (portAtStart && selectedPort === portAtStart) inventory = rows.find(row => row.com_port === portAtStart) ?? null;
      connectStream();
    } catch (failure) { if (!destroyed) error = describe(failure); }
    finally { pending = false; loading = false; }
  }

  async function open(port) {
    returnFocus = document.activeElement;
    clearStream();
    selectedPort = port.com_port;
    inventory = port;
    scanSlots = null;
    phoneSlot = null;
    error = '';
    message = '';
    await tick();
    drawer?.querySelector('button:not([disabled])')?.focus();
    await refresh();
  }

  function close() {
    clearStream();
    selectedPort = null;
    inventory = null;
    scanSlots = null;
    phoneSlot = null;
    returnFocus?.focus();
  }

  function dialogKeydown(event) {
    if (!selectedPort || !drawer) return;
    if (event.key === 'Escape') { event.preventDefault(); close(); return; }
    if (event.key !== 'Tab') return;
    const controls = [...drawer.querySelectorAll('button:not([disabled]),input:not([disabled]),select:not([disabled]),summary')]
      .filter(control => control.getClientRects().length > 0);
    const first = controls[0];
    const last = controls.at(-1);
    if (!drawer.contains(document.activeElement) || event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      (event.shiftKey ? last : first)?.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first?.focus();
    }
  }

  function prepareScan(slots) {
    scanSlots = slots;
    returnSlot = inventory.selection_verified ? String(inventory.verified_slot) : '';
    phoneSlot = null;
    error = '';
  }

  async function startScan() {
    if (!returnSlot || locked) return;
    busy = true;
    error = '';
    try {
      const snapshot = unwrap(await apiClient.msimScan(selectedPort, {
        slots: scanSlots, return_slot: Number(returnSlot), request_id: crypto.randomUUID(),
      }));
      inventory = { ...inventory, operation: snapshot };
      scanSlots = null;
      connectStream();
      await refresh();
    } catch (failure) { error = describe(failure); }
    finally { busy = false; }
  }

  async function selectSlot(slot) {
    if (!confirm($t('msim_switch_confirm', { slot: slot.slot, iccid: slot.iccid ?? '-' }))) return;
    busy = true;
    error = '';
    try {
      const snapshot = unwrap(await apiClient.msimSelect(selectedPort, slot.slot, {
        expected_iccid: slot.iccid, request_id: crypto.randomUUID(),
      }));
      inventory = { ...inventory, operation: snapshot };
      connectStream();
      await refresh();
    } catch (failure) { error = describe(failure); }
    finally { busy = false; }
  }

  async function savePhone(event) {
    event.preventDefault();
    busy = true;
    error = '';
    try {
      inventory = unwrap(await apiClient.msimPhone(selectedPort, phoneSlot.slot, {
        expected_iccid: phoneSlot.iccid, phone_number: phoneNumber,
      }));
      phoneSlot = null;
      message = $t('msg_esim_phone_set_success');
      await refresh();
    } catch (failure) { error = describe(failure); }
    finally { busy = false; }
  }

  async function cancel() {
    busy = true;
    try { await apiClient.msimCancel(operation.id); await refresh(); }
    catch (failure) { error = describe(failure); }
    finally { busy = false; }
  }

  onMount(() => {
    refresh();
    timer = setInterval(refresh, 2000);
  });
  onDestroy(() => { destroyed = true; clearInterval(timer); clearStream(); });
</script>

<svelte:window onkeydown={dialogKeydown} />

<div class="flex h-dvh w-screen flex-col bg-gray-50 text-sm text-gray-800 dark:bg-zinc-950 dark:text-gray-100">
  <header class="flex shrink-0 items-center justify-between gap-3 border-b border-gray-200 bg-white px-4 py-3 dark:border-zinc-800 dark:bg-zinc-900">
    <div class="flex min-w-0 items-center gap-3">
      <button onclick={onBack} class="inline-flex h-9 items-center gap-1 rounded bg-blue-600 px-3 text-white">
        <Icon icon="carbon:arrow-left" class="h-4 w-4" />{$t('btn_back')}
      </button>
      <h1 class="text-base font-semibold">{$t('msim_title')}</h1>
    </div>
    <button onclick={refresh} title={$t('esim_refresh')} aria-label={$t('esim_refresh')} class="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded border border-gray-200 dark:border-zinc-700">
      <Icon icon="carbon:renew" class="h-4 w-4" />
    </button>
  </header>
  <main class="flex-1 overflow-auto p-4 sm:p-6">
    {#if error && !selectedPort}<p role="alert" class="mb-3 rounded bg-red-50 p-3 text-red-700">{error}</p>{/if}
    <div class="mb-4 flex items-center gap-2">
      <Icon icon="carbon:search" class="h-4 w-4 text-gray-500" />
      <input bind:value={filter} aria-label={$t('msim_filter')} placeholder={$t('msim_filter')} class="h-9 min-w-0 w-72 rounded border border-gray-300 bg-white px-3 dark:border-zinc-700 dark:bg-zinc-900" />
    </div>
    {#if loading}<p>{$t('msim_loading')}</p>
    {:else if filtered.length === 0}<p class="py-8 text-center text-gray-500">{$t('msim_no_ports')}</p>
    {:else}
      <div class="overflow-x-auto rounded-lg border border-gray-200 bg-white dark:border-zinc-800 dark:bg-zinc-900">
        <table class="w-full text-left">
          <thead class="bg-gray-100 text-gray-600 dark:bg-zinc-800 dark:text-gray-300"><tr>
            <th class="px-4 py-3">{$t('esim_col_port')}</th><th class="px-4 py-3">{$t('msim_selected')}</th>
            <th class="px-4 py-3">{$t('phone_number_label')}</th><th class="px-4 py-3">{$t('msim_identified')}</th>
            <th class="px-4 py-3">{$t('msim_last_attempt')}</th>
            <th class="px-4 py-3">{$t('esim_col_status')}</th><th class="px-4 py-3">{$t('esim_col_actions')}</th>
          </tr></thead>
          <tbody>{#each filtered as port (port.com_port)}
            <tr class="border-t border-gray-100 dark:border-zinc-800">
              <td class="px-4 py-3 font-semibold">{port.com_port}</td>
              <td class="whitespace-nowrap px-4 py-3">{port.active_slot ? slotLabel(port.active_slot) : '-'}
                {#if !port.selection_verified}<div class="text-xs text-gray-500">{$t('msim_last_known')}</div>{/if}</td>
              <td class="px-4 py-3 font-mono">{port.slots.find(slot => slot.slot === port.active_slot)?.phone_number ?? '-'}</td>
              <td class="whitespace-nowrap px-4 py-3">{port.slots.filter(slot => slot.iccid && slot.state === 'present').length} / 4</td>
              <td class="whitespace-nowrap px-4 py-3 text-xs">{timestamp(port.slots.map(slot => slot.last_checked_at).filter(Boolean).sort().at(-1))}</td>
              <td class="px-4 py-3">{port.operation ? stageLabel(port.operation.stage) : $t('esim_status_idle')}</td>
              <td class="px-4 py-3"><button onclick={() => open(port)} class="inline-flex items-center gap-1 rounded border border-gray-300 px-3 py-1.5 dark:border-zinc-600"><Icon icon="carbon:settings" class="h-4 w-4" />{$t('esim_manage')}</button></td>
            </tr>
          {/each}</tbody>
        </table>
      </div>
    {/if}
  </main>
</div>

{#if selectedPort && inventory}
  <div class="fixed inset-0 z-40 bg-black/30" role="presentation" onclick={close}></div>
  <section bind:this={drawer} role="dialog" aria-modal="true" aria-label={`${selectedPort} ${$t('msim_title')}`} class="fixed inset-y-0 right-0 z-50 flex w-full flex-col bg-white shadow-xl dark:bg-zinc-900 lg:max-w-5xl">
    <header class="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-gray-200 p-4 dark:border-zinc-800">
      <div class="min-w-0"><h2 class="text-base font-semibold">{selectedPort} {$t('msim_title')}</h2>
        <p class="mt-1 text-xs text-gray-500">{$t(inventory.selection_verified ? 'msim_selected' : 'msim_last_known')}: {inventory.active_slot ? slotLabel(inventory.active_slot) : '-'}</p>
      </div>
      <div class="ml-auto flex shrink-0 gap-2">
        <button onclick={() => prepareScan([1, 2, 3, 4])} disabled={locked || hasRunning} class="inline-flex h-9 items-center gap-1.5 rounded border border-gray-300 px-3 disabled:opacity-40 dark:border-zinc-600"><Icon icon="carbon:renew" class="h-4 w-4" />{$t('msim_read_all')}</button>
        <button onclick={close} title={$t('esim_batch_close')} aria-label={$t('esim_batch_close')} class="inline-flex h-9 w-9 items-center justify-center rounded bg-gray-100 dark:bg-zinc-800"><Icon icon="carbon:close" class="h-4 w-4" /></button>
      </div>
    </header>
    <div class="flex-1 overflow-auto p-4 sm:p-6">
      {#if error}<p role="alert" class="mb-4 break-words rounded bg-red-50 p-3 text-red-700">{error}</p>{/if}
      {#if message}<p role="status" class="mb-4 rounded bg-green-50 p-3 text-green-700">{message}</p>{/if}
      {#if scanSlots}
        <form onsubmit={event => { event.preventDefault(); startScan(); }} class="mb-4 border-y border-amber-200 bg-amber-50 p-4 text-amber-900">
          <h3 class="font-semibold">{scanSlots.length === 4 ? $t('msim_read_all') : $t('msim_read_slot')}</h3>
          <p class="mt-2 text-xs">{$t('msim_scan_warning')}</p>
          <label class="mt-3 flex flex-wrap items-center gap-2">{$t('msim_return_slot')}
            <select bind:value={returnSlot} required class="h-9 rounded border border-amber-300 bg-white px-2 text-gray-800">
              <option value="">{$t('msim_choose_slot')}</option>
              {#each [1, 2, 3, 4] as slot}<option value={String(slot)}>{slotLabel(slot)}</option>{/each}
            </select>
          </label>
          <div class="mt-3 flex gap-2"><button type="submit" disabled={locked || !returnSlot} class="inline-flex items-center gap-1 rounded bg-blue-600 px-3 py-2 text-white disabled:opacity-40"><Icon icon="carbon:play" class="h-4 w-4" />{$t('msim_start_read')}</button>
            <button type="button" onclick={() => scanSlots = null} class="rounded border border-amber-300 px-3 py-2">{$t('cancel')}</button></div>
        </form>
      {/if}
      {#if operation}
        <div role="status" class="mb-4 border-l-4 p-3 {operation.status === 'failed' || operation.status === 'interrupted' ? 'border-red-400 bg-red-50 text-red-800' : running ? 'border-amber-400 bg-amber-50 text-amber-900' : 'border-green-400 bg-green-50 text-green-800'}">
          <div class="flex flex-wrap items-center justify-between gap-2"><strong>{stageLabel(operation.stage)}{operation.current_slot ? `: ${slotLabel(operation.current_slot)}` : ''}</strong>
            {#if running}<button onclick={cancel} disabled={busy} class="inline-flex items-center gap-1 rounded border border-current px-2 py-1 text-xs disabled:opacity-40"><Icon icon="carbon:stop" class="h-3 w-3" />{$t('cancel')}</button>{/if}</div>
          {#if operation.return_slot}<p class="mt-1 text-xs">{$t('msim_return_slot')}: {slotLabel(operation.return_slot)} / {$t('msim_restore_' + operation.restore_status)}</p>{/if}
          {#if operation.error}<p class="mt-2 break-words text-xs">{operation.error}</p>{/if}
        </div>
      {/if}
      <div class="hidden grid-cols-[72px_minmax(0,1.4fr)_110px_minmax(0,1fr)_190px] gap-3 border-b border-gray-200 px-3 py-2 text-xs font-semibold text-gray-500 dark:border-zinc-700 md:grid">
        <span>{$t('msim_slot_heading')}</span><span>ICCID</span><span>{$t('esim_col_status')}</span><span>{$t('phone_number_label')}</span><span>{$t('esim_col_actions')}</span>
      </div>
      {#each inventory.slots as slot (slot.slot)}
        {@const current = inventory.selection_verified && inventory.verified_slot === slot.slot}
        {@const reading = running && operation.current_slot === slot.slot}
        {@const item = operation?.items?.find(item => item.slot === slot.slot)}
        <article class="border-b border-gray-200 px-3 py-4 dark:border-zinc-700 {current ? 'bg-green-50 dark:bg-green-950/20' : reading ? 'bg-amber-50 dark:bg-amber-950/20' : ''}" data-slot={slot.slot}>
          <div class="grid grid-cols-1 items-start gap-3 md:grid-cols-[72px_minmax(0,1.4fr)_110px_minmax(0,1fr)_190px]">
            <h3 class="font-semibold">{slotLabel(slot.slot)}</h3>
            <div class="flex min-w-0 items-start gap-1"><span class="min-w-0 break-all font-mono text-xs leading-5">{slot.iccid ?? '-'}</span>
              {#if slot.iccid}<button title={$t('msim_copy_iccid')} aria-label={$t('msim_copy_iccid')} onclick={() => navigator.clipboard.writeText(slot.iccid).catch(failure => error = describe(failure))} class="flex h-6 w-6 shrink-0 items-center justify-center rounded hover:bg-gray-100"><Icon icon="carbon:copy" class="h-3.5 w-3.5" /></button>{/if}</div>
            <div><span class="inline-block rounded px-2 py-0.5 text-xs {current ? 'bg-green-100 text-green-800' : slot.state === 'unreadable' ? 'bg-red-100 text-red-700' : 'bg-gray-100 text-gray-600'}">{reading ? stageLabel(operation.stage) : current ? $t('msim_current') : $t('msim_state_' + (slot.state === 'present' ? 'cached' : slot.state))}</span></div>
            <div class="min-w-0 break-all font-mono">{slot.phone_number || $t('msim_unknown_number')}</div>
            <div class="flex flex-wrap items-center gap-1.5">
              {#if current}<span class="px-2 py-1 text-xs text-green-700">{$t('msim_current')}</span>
              {:else}<button onclick={() => selectSlot(slot)} disabled={locked || hasRunning || slot.state !== 'present' || !slot.iccid} class="inline-flex h-8 items-center gap-1 rounded bg-green-600 px-2 text-xs text-white disabled:opacity-40"><Icon icon="carbon:checkmark" class="h-3.5 w-3.5" />{$t('msim_use')}</button>{/if}
              <button onclick={() => { phoneSlot = slot; phoneNumber = slot.phone_number ?? ''; scanSlots = null; message = ''; }} disabled={locked || !slot.iccid} class="inline-flex h-8 items-center gap-1 rounded border border-gray-300 px-2 text-xs disabled:opacity-40 dark:border-zinc-600"><Icon icon="carbon:edit" class="h-3.5 w-3.5" />{$t('msim_number')}</button>
              <button onclick={() => prepareScan([slot.slot])} disabled={locked || hasRunning} title={$t('msim_read_slot')} aria-label={`${$t('msim_read_slot')} ${slot.slot}`} class="inline-flex h-8 w-8 items-center justify-center rounded border border-gray-300 disabled:opacity-40 dark:border-zinc-600"><Icon icon="carbon:renew" class="h-3.5 w-3.5" /></button>
            </div>
          </div>
          <details class="mt-3 text-xs text-gray-500"><summary class="cursor-pointer">{$t('msim_details')}</summary>
            <dl class="mt-2 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
              <dt>IMSI</dt><dd class="break-all font-mono">{slot.imsi ?? '-'}</dd>
              <dt>{$t('msim_last_verified')}</dt><dd>{timestamp(slot.identity_verified_at)}</dd>
              <dt>{$t('msim_last_attempt')}</dt><dd>{timestamp(slot.last_checked_at)}</dd>
              <dt>{$t('msim_number_source')}</dt><dd>{$t('msim_source_' + (slot.phone_number_source ?? 'unknown'))}</dd>
            </dl>
          </details>
          {#if slot.last_error || item?.message && item.message !== 'empty'}<p class="mt-2 break-words text-xs text-red-600">{slot.last_error ?? item.message}</p>{/if}
          {#if phoneSlot?.slot === slot.slot}
            <form onsubmit={savePhone} class="mt-3 flex flex-wrap items-end gap-2 border-t border-gray-200 pt-3">
              <label class="flex min-w-0 flex-1 flex-col gap-1 text-xs">{$t('msim_local_number')}
                <input bind:value={phoneNumber} aria-label={$t('phone_number_label')} type="tel" pattern="[+]?[0-9]*" class="h-9 w-full min-w-0 rounded border border-gray-300 bg-white px-2 text-sm text-gray-900 dark:border-zinc-600" />
              </label>
              <button type="submit" disabled={locked} class="inline-flex h-9 items-center gap-1 rounded bg-blue-600 px-3 text-xs text-white disabled:opacity-40"><Icon icon="carbon:save" class="h-4 w-4" />{$t('btn_save_esim_number')}</button>
              <button type="button" onclick={() => phoneSlot = null} class="h-9 rounded border border-gray-300 px-3 text-xs">{$t('cancel')}</button>
            </form>
          {/if}
        </article>
      {/each}
    </div>
  </section>
{/if}