<script>
  import { onMount } from 'svelte';
  import Icon from '@iconify/svelte';
  import { callLog, callActions, incomingCall, activeCall, callSseConnected } from '../../stores/calls.js';
  import { simCards } from '../../stores/simcards.js';
  import { formatDate } from '../../js/dateFormat.js';
  import { apiClient, buildBasicAuthHeader } from '../../js/api.js';
  import { t } from '../../js/i18n.js';

  let { filterSimId = null } = $props();

  let makeCallPhone = $state('');
  let makeCallSimId = $state('');
  let making = $state(false);
  let makeError = $state('');
  let voiceSettings = $state({ enabled: false, com_port: 'COM2', prompt_seconds: 10, code_seconds: 10, otp_length: 6, deadline_seconds: 60 });
  // COM port of the SIM this page was opened for; voice settings then apply to that port only.
  let selectedPort = $state('');
  let portEnabled = $state(false);
  let aliyun = $state({ app_key: '', region: 'cn-shanghai', sample_rate: 16000, access_key_id: '', has_token: false, has_access_key_secret: false, ready: false });
  let aliyunToken = $state('');
  let aliyunSecret = $state('');
  let aliyunBusy = $state(false);
  let aliyunMessage = $state('');
  let aliyunError = $state('');
  let voiceReady = $state(false);
  let voiceBusy = $state(true);
  let voiceError = $state('');
  let voiceSaved = $state(false);

  let sims = $derived($simCards);
  let log = $derived($callLog);
  let filteredLog = $derived(filterSimId ? log.filter(call => call.sim_id === filterSimId) : log);
  let incoming = $derived($incomingCall);
  let active = $derived($activeCall);

  onMount(() => {
    callActions.refreshLog();
    loadVoiceSettings();
    if (filterSimId) {
      makeCallSimId = filterSimId;
      return;
    }
    if (sims.length > 0 && !makeCallSimId) {
      makeCallSimId = sims[0].id;
    }
  });

  $effect(() => {
    if (filterSimId) {
      makeCallSimId = filterSimId;
    }
  });

  $effect(() => {
    if (sims.length > 0 && !makeCallSimId) {
      makeCallSimId = sims[0].id;
    }
  });

  async function handleMakeCall() {
    if (!makeCallPhone.trim() || !makeCallSimId) return;
    making = true;
    makeError = '';
    try {
      await callActions.make(makeCallSimId, makeCallPhone.trim());
    } catch (e) {
      makeError = e?.data?.message ?? 'Call failed';
    } finally {
      making = false;
    }
  }

  function voicePorts(settings) {
    return String(settings.com_port ?? '').split(',').map((port) => port.trim()).filter(Boolean);
  }

  async function loadVoiceSettings() {
    try {
      if (filterSimId) {
        const info = await apiClient.getAllSimsInfo();
        const rows = Array.isArray(info?.data) ? info.data : (Array.isArray(info) ? info : []);
        selectedPort = rows.find((row) => row.sim_id === filterSimId)?.com_port ?? '';
      }
      const response = await apiClient.getVoiceOtpSettings();
      voiceSettings = response.data.settings;
      portEnabled = !!selectedPort && voiceSettings.enabled && voicePorts(voiceSettings).includes(selectedPort);
      voiceReady = response.data.ready === true;
      aliyun = (await apiClient.getAliyunAsrSettings()).data;
    } catch (error) {
      voiceError = error?.data?.error ?? error?.message ?? $t('voice_error');
    } finally {
      voiceBusy = false;
    }
  }

  async function saveAliyun() {
    aliyunBusy = true;
    aliyunError = '';
    aliyunMessage = '';
    try {
      const payload = { app_key: aliyun.app_key, region: aliyun.region, sample_rate: Number(aliyun.sample_rate), access_key_id: aliyun.access_key_id };
      if (aliyunToken.trim()) payload.token = aliyunToken.trim();
      if (aliyunSecret.trim()) payload.access_key_secret = aliyunSecret.trim();
      aliyun = (await apiClient.setAliyunAsrSettings(payload)).data;
      voiceReady = aliyun.ready === true;
      aliyunToken = '';
      aliyunSecret = '';
      aliyunMessage = $t('voice_saved');
    } catch (error) {
      aliyunError = error?.data?.error ?? error?.message ?? $t('voice_error');
    } finally {
      aliyunBusy = false;
    }
  }

  async function testAliyun() {
    aliyunBusy = true;
    aliyunError = '';
    aliyunMessage = '';
    try {
      await apiClient.testAliyunAsr();
      aliyunMessage = $t('aliyun_test_ok');
    } catch (error) {
      aliyunError = error?.data?.error ?? error?.message ?? $t('voice_error');
    } finally {
      aliyunBusy = false;
    }
  }

  async function saveVoiceSettings() {
    voiceBusy = true;
    voiceError = '';
    voiceSaved = false;
    try {
      const payload = { ...voiceSettings };
      if (selectedPort) {
        // Only add/remove this port; a disabled config has no active ports to keep.
        const others = voiceSettings.enabled ? voicePorts(voiceSettings).filter((port) => port !== selectedPort) : [];
        const ports = portEnabled ? [...others, selectedPort] : others;
        payload.enabled = ports.length > 0;
        payload.com_port = ports.length > 0 ? ports.join(',') : selectedPort;
      }
      const response = await apiClient.setVoiceOtpSettings(payload);
      voiceSettings = response.data.settings;
      if (selectedPort) portEnabled = voiceSettings.enabled && voicePorts(voiceSettings).includes(selectedPort);
      voiceReady = response.data.ready === true;
      voiceSaved = true;
    } catch (error) {
      voiceError = error?.data?.error ?? error?.message ?? $t('voice_error');
    } finally {
      voiceBusy = false;
    }
  }

  function voiceProgress(value) {
    try { return value ? JSON.parse(value) : null; }
    catch { return null; }
  }

  function voiceRunning(progress) {
    return progress && !['completed', 'failed', 'cancelled', 'interrupted'].includes(progress.state);
  }

  async function cancelVoice(callId) {
    try {
      await apiClient.cancelVoiceOtp(callId);
      await callActions.refreshLog();
    } catch (error) {
      voiceError = error?.data?.error ?? error?.message ?? $t('voice_error');
    }
  }

  async function downloadVoice(callId, stage) {
    try {
      const response = await fetch(`/api/calls/${encodeURIComponent(callId)}/recording?stage=${stage}`, { headers: buildBasicAuthHeader() });
      if (!response.ok) throw new Error($t('voice_error'));
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement('a');
      link.href = url;
      link.download = `${callId}-${stage}.amr`;
      document.body.append(link);
      link.click();
      link.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (error) {
      voiceError = error?.message ?? $t('voice_error');
    }
  }

  function statusIcon(status) {
    switch (status) {
      case 'ended': return 'carbon:phone-off';
      case 'missed': return 'carbon:phone-missed';
      case 'ringing': return 'carbon:phone-incoming';
      case 'active': return 'carbon:phone-voice';
      default: return 'carbon:phone';
    }
  }

  function statusColor(status) {
    switch (status) {
      case 'ended': return 'text-zinc-400';
      case 'missed': return 'text-red-400';
      case 'ringing': return 'text-yellow-400';
      case 'active': return 'text-green-400';
      default: return 'text-zinc-400';
    }
  }

  function directionIcon(direction) {
    return direction === 'inbound' ? 'carbon:arrow-down' : 'carbon:arrow-up';
  }

  function durationLabel(started, ended) {
    if (!ended) return '';
    const s = Math.round((new Date(ended) - new Date(started)) / 1000);
    if (s < 60) return `${s}s`;
    return `${Math.floor(s / 60)}m ${s % 60}s`;
  }
</script>

<div class="flex flex-col h-full bg-white dark:bg-zinc-900 overflow-hidden">
  <!-- Header -->
  <div class="px-4 py-3 border-b border-gray-200/70 dark:border-zinc-800 shrink-0">
    <h2 class="text-base font-semibold text-gray-900 dark:text-white">{$t('call_log_title')}</h2>
    {#if filterSimId}
      <p class="text-xs text-blue-600 dark:text-blue-400 mt-0.5">SIM: {filterSimId.slice(0, 12)}…</p>
    {/if}
    <p class="text-xs text-gray-500 dark:text-zinc-500 mt-0.5">
      SSE: <span class={$callSseConnected ? 'text-green-500' : 'text-red-400'}>
        {$callSseConnected ? $t('sse_connected') : $t('sse_disconnected')}
      </span>
    </p>
  </div>

  <form
    aria-label={$t('voice_local_title')}
    class="px-4 py-3 border-b border-gray-200/70 dark:border-zinc-800 shrink-0"
    onsubmit={(event) => { event.preventDefault(); saveVoiceSettings(); }}
  >
    <div class="flex items-center justify-between gap-3 mb-3">
      <div class="flex items-center gap-3 min-w-0 flex-wrap">
        <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-100">{$t('voice_local_title')}</h3>
        <span class="text-xs {voiceReady ? 'text-green-600 dark:text-green-400' : 'text-amber-600 dark:text-amber-400'}">
          {voiceReady ? $t('voice_ready') : $t('voice_unavailable')}
        </span>
        <label class="inline-flex items-center gap-2 text-sm text-gray-700 dark:text-gray-200">
          {#if selectedPort}
            <input type="checkbox" bind:checked={portEnabled} disabled={voiceBusy || (!voiceReady && !portEnabled)} class="accent-blue-600" />
            {$t('voice_enable')} {selectedPort}
          {:else}
            <input type="checkbox" bind:checked={voiceSettings.enabled} disabled={voiceBusy || (!voiceReady && !voiceSettings.enabled)} class="accent-blue-600" />
            {$t('voice_enable')}
          {/if}
        </label>
      </div>
      <button type="submit" disabled={voiceBusy} aria-label={$t('voice_save')} title={$t('voice_save')}
        class="w-9 h-9 shrink-0 rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40 flex items-center justify-center">
        <Icon icon={voiceBusy ? 'carbon:renew' : 'carbon:save'} class="w-4 h-4" />
      </button>
    </div>
    <fieldset disabled={voiceBusy} class="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-3 min-w-0">
      <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0" title={$t('voice_port_hint')}>
        {$t('voice_port')}
        {#if selectedPort}
          <input type="text" readonly value={selectedPort} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-gray-50 dark:bg-zinc-900 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        {:else}
          <input type="text" required pattern={'\\s*COM[1-9][0-9]{0,2}(\\s*,\\s*COM[1-9][0-9]{0,2}){0,31}\\s*'} placeholder="COM2,COM5" bind:value={voiceSettings.com_port} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        {/if}
      </label>
      <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
        {$t('voice_prompt_window')}
        <input type="number" min="3" max="30" step="1" required bind:value={voiceSettings.prompt_seconds} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
      </label>
      <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
        {$t('voice_code_window')}
        <input type="number" min="3" max="30" step="1" required bind:value={voiceSettings.code_seconds} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
      </label>
      <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
        {$t('voice_length')}
        <input type="number" min="4" max="8" step="1" required bind:value={voiceSettings.otp_length} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
      </label>
      <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
        {$t('voice_deadline')}
        <input type="number" min="30" max="240" step="1" required bind:value={voiceSettings.deadline_seconds} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
      </label>
    </fieldset>
    {#if voiceSaved}<p class="text-xs text-green-600 dark:text-green-400 mt-2" role="status">{$t('voice_saved')}</p>{/if}
    {#if voiceError}<p class="text-xs text-red-600 dark:text-red-400 mt-2 break-words" role="alert">{voiceError}</p>{/if}
  </form>

  <form
      aria-label={$t('aliyun_title')}
      class="px-4 py-3 border-b border-gray-200/70 dark:border-zinc-800 shrink-0"
      onsubmit={(event) => { event.preventDefault(); saveAliyun(); }}
    >
      <div class="flex items-center justify-between gap-3 mb-3">
        <div class="flex items-center gap-3 min-w-0 flex-wrap">
          <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-100">{$t('aliyun_title')}</h3>
          <span class="text-xs {aliyun.ready ? 'text-green-600 dark:text-green-400' : 'text-amber-600 dark:text-amber-400'}">
            {aliyun.ready ? $t('aliyun_ready') : $t('aliyun_not_ready')}
          </span>
        </div>
        <div class="flex gap-2 shrink-0">
          <button type="button" onclick={testAliyun} disabled={aliyunBusy || !aliyun.ready} aria-label={$t('aliyun_test')} title={$t('aliyun_test')}
            class="w-9 h-9 rounded-md border border-gray-200 dark:border-zinc-700 text-gray-700 dark:text-gray-200 disabled:opacity-40 flex items-center justify-center">
            <Icon icon="carbon:connection-signal" class="w-4 h-4" />
          </button>
          <button type="submit" disabled={aliyunBusy} aria-label={$t('aliyun_save')} title={$t('aliyun_save')}
            class="w-9 h-9 rounded-md bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40 flex items-center justify-center">
            <Icon icon={aliyunBusy ? 'carbon:renew' : 'carbon:save'} class="w-4 h-4" />
          </button>
        </div>
      </div>
      <fieldset disabled={aliyunBusy} class="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-6 gap-3 min-w-0">
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_app_key')}
          <input type="text" autocomplete="off" bind:value={aliyun.app_key} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        </label>
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_token')}
          <input type="password" autocomplete="new-password" bind:value={aliyunToken} placeholder={aliyun.has_token ? $t('aliyun_saved_secret') : ''} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        </label>
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_ak_id')}
          <input type="text" autocomplete="off" bind:value={aliyun.access_key_id} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        </label>
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_ak_secret')}
          <input type="password" autocomplete="new-password" bind:value={aliyunSecret} placeholder={aliyun.has_access_key_secret ? $t('aliyun_saved_secret') : ''} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5" />
        </label>
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_region')}
          <select bind:value={aliyun.region} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5">
            <option value="cn-shanghai">华东2（上海）</option>
            <option value="cn-beijing">华北2（北京）</option>
            <option value="cn-shenzhen">华南1（深圳）</option>
            <option value="ap-southeast-1">新加坡</option>
          </select>
        </label>
        <label class="flex flex-col gap-1 text-xs text-gray-600 dark:text-gray-400 min-w-0">
          {$t('aliyun_sample_rate')}
          <select bind:value={aliyun.sample_rate} class="w-full min-w-0 text-sm rounded-md border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5">
            <option value={16000}>16000 Hz</option>
            <option value={8000}>8000 Hz</option>
          </select>
        </label>
      </fieldset>
      {#if aliyunMessage}<p class="text-xs text-green-600 dark:text-green-400 mt-2" role="status">{aliyunMessage}</p>{/if}
      {#if aliyunError}<p class="text-xs text-red-600 dark:text-red-400 mt-2 break-words" role="alert">{aliyunError}</p>{/if}
    </form>

  <!-- Make a call -->
  <div class="px-4 py-3 border-b border-gray-200/70 dark:border-zinc-800 shrink-0">
    <p class="text-xs font-medium text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wide">{$t('make_a_call')}</p>
    <div class="flex gap-2 items-center flex-wrap">
      <select
        bind:value={makeCallSimId}
        disabled={!!filterSimId}
        class="text-sm rounded-lg border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-2 py-1.5 min-w-0 max-w-[180px] shrink-0"
      >
        {#each sims as sim}
          <option value={sim.id}>{sim.alias ?? sim.phone_number ?? sim.id.slice(0, 8)}</option>
        {/each}
      </select>
      <input
        type="tel"
        placeholder={$t('phone_number_ph')}
        bind:value={makeCallPhone}
        onkeydown={(e) => e.key === 'Enter' && handleMakeCall()}
        class="flex-1 text-sm rounded-lg border border-gray-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 text-gray-800 dark:text-gray-100 px-3 py-1.5 outline-none focus:ring-2 focus:ring-green-500/50"
      />
      <button
        onclick={handleMakeCall}
        disabled={making || !makeCallPhone.trim() || !!active || !!incoming}
        class="shrink-0 w-9 h-9 rounded-full bg-green-500 hover:bg-green-400 active:scale-95 transition flex items-center justify-center disabled:opacity-40"
        aria-label="Call"
      >
        <Icon icon="carbon:phone" class="w-4 h-4 text-white" />
      </button>
    </div>
    {#if makeError}
      <p class="text-xs text-red-400 mt-1">{makeError}</p>
    {/if}
  </div>

  <!-- Log list -->
  <div class="flex-1 overflow-y-auto divide-y divide-gray-100 dark:divide-zinc-800/70">
    {#if filteredLog.length === 0}
      <div class="flex flex-col items-center justify-center h-full gap-2 text-zinc-400">
        <Icon icon="carbon:phone-off" class="w-10 h-10 opacity-30" />
        <p class="text-sm">{$t('no_calls')}</p>
      </div>
    {:else}
      {#each filteredLog as call (call.id)}
        {@const voice = voiceProgress(call.voice_otp)}
        <div class="px-4 py-3 hover:bg-gray-50 dark:hover:bg-zinc-800/40 transition-colors">
          <div class="flex items-center gap-3">
          <!-- Direction + status icon -->
          <div class="relative shrink-0 w-10 h-10 rounded-full bg-gray-100 dark:bg-zinc-800 flex items-center justify-center">
            <Icon icon={statusIcon(call.status)} class="w-5 h-5 {statusColor(call.status)}" />
            <span class="absolute -bottom-0.5 -right-0.5 w-4 h-4 rounded-full bg-white dark:bg-zinc-900 flex items-center justify-center">
              <Icon icon={directionIcon(call.direction)} class="w-3 h-3 text-zinc-400" />
            </span>
          </div>

          <!-- Info -->
          <div class="flex-1 min-w-0">
            <p class="text-sm font-medium text-gray-900 dark:text-white truncate">
              {call.phone ?? $t('unknown')}
            </p>
            <p class="text-xs text-gray-500 dark:text-zinc-500 truncate">
              {call.sim_id.slice(0, 12)}… · {call.status}
              {#if call.ended_at && call.status === 'ended'}
                · {durationLabel(call.started_at, call.ended_at)}
              {/if}
            </p>
          </div>

          <!-- Time -->
          <p class="text-xs text-gray-400 dark:text-zinc-500 shrink-0">
            {formatDate(call.started_at)}
          </p>
          </div>
          {#if voice}
            <div class="mt-3 pl-0 sm:pl-[52px] min-w-0">
              <div class="flex items-center gap-3 flex-wrap text-xs text-gray-600 dark:text-gray-300">
                <span class={voice.error ? 'text-red-600 dark:text-red-400' : 'text-blue-600 dark:text-blue-400'}>{$t(`voice_state_${voice.state}`)}</span>
                {#if voice.dtmf}<span>{$t('voice_dtmf')}: <span class="font-mono">{voice.dtmf}</span></span>{/if}
                {#if voice.otp}<span class="text-sm font-semibold text-green-700 dark:text-green-400">{$t('voice_code')}: <span class="font-mono">{voice.otp}</span></span>{/if}
                {#if voice.prompt_transcript}
                  <button type="button" onclick={() => downloadVoice(call.id, 'prompt')} aria-label={$t('voice_download_prompt')} title={$t('voice_download_prompt')} class="w-8 h-8 rounded-md border border-gray-200 dark:border-zinc-700 flex items-center justify-center">
                    <Icon icon="carbon:download" class="w-4 h-4" />
                  </button>
                {/if}
                {#if voice.code_transcript || voice.otp}
                  <button type="button" onclick={() => downloadVoice(call.id, 'code')} aria-label={$t('voice_download_code')} title={$t('voice_download_code')} class="w-8 h-8 rounded-md border border-gray-200 dark:border-zinc-700 flex items-center justify-center">
                    <Icon icon="carbon:document-download" class="w-4 h-4" />
                  </button>
                {/if}
                {#if voiceRunning(voice)}
                  <button type="button" onclick={() => cancelVoice(call.id)} aria-label={$t('voice_cancel')} title={$t('voice_cancel')} class="w-8 h-8 rounded-md border border-red-200 dark:border-red-900 text-red-600 flex items-center justify-center">
                    <Icon icon="carbon:stop" class="w-4 h-4" />
                  </button>
                {/if}
              </div>
              {#if voice.error}<p class="mt-1 text-xs text-red-600 dark:text-red-400 break-words">{voice.error}</p>{/if}
              {#if voice.prompt_comparison}
                <p class="mt-2 text-xs {voice.prompt_comparison === 'matched' ? 'text-green-700 dark:text-green-400' : 'text-amber-700 dark:text-amber-400'}">
                  {$t('voice_prompt_comparison')}: {$t(`voice_comparison_${voice.prompt_comparison}`)}
                </p>
                {#each voice.prompt_readings ?? [] as reading, index}
                  <p class="mt-1 text-xs text-gray-600 dark:text-gray-300">
                    {$t('voice_prompt_reading')} {index + 1}: <span class="font-mono font-semibold">{reading}</span>
                  </p>
                {/each}
              {/if}
              {#if voice.prompt_transcript || voice.code_transcript}
                <details class="mt-2 text-xs text-gray-600 dark:text-gray-300">
                  <summary class="cursor-pointer">{$t('voice_transcripts')}</summary>
                  {#if voice.prompt_transcript}<p class="mt-2 whitespace-pre-wrap break-words"><span class="font-semibold">{$t('voice_prompt_transcript')}: </span>{voice.prompt_transcript}</p>{/if}
                  {#if voice.code_transcript}<p class="mt-2 whitespace-pre-wrap break-words"><span class="font-semibold">{$t('voice_code_transcript')}: </span>{voice.code_transcript}</p>{/if}
                </details>
              {/if}
            </div>
          {/if}
        </div>
      {/each}
    {/if}
  </div>
</div>
