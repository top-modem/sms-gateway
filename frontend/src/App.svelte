<script>
  import { fly } from "svelte/transition";
  import { quartOut } from "svelte/easing";
  import { isAuthenticated, isAuthLoading } from "./stores/auth";
  import { initConversation } from "./stores/conversation";
  import { connectCallSSE, disconnectCallSSE } from "./stores/calls.js";
  import Login from "./pages/Login.svelte";
  import Dashboard from "./pages/Dashboard.svelte";
  import SimDashboard from "./pages/SimDashboard.svelte";
  import CallLogPage from "./pages/CallLogPage.svelte";
  import SimCardsPage from "./pages/SimCardsPage.svelte";
  import SimSetPhonePage from "./pages/SimSetPhonePage.svelte";
  import PlatformConnectPage from "./pages/PlatformConnectPage.svelte";
  import PhoneNumberPage from "./pages/PhoneNumberPage.svelte";

  /** @type {'sim' | 'messages' | 'calllog' | 'simcards' | 'setphone' | 'platform' | 'phonenumber'} */
  let currentPage = $state('sim');
  let filterSimId = $state(null);

  function goToSim() {
    filterSimId = null;
    currentPage = 'sim';
  }

  function goToMessages(simId) {
    filterSimId = simId;
    currentPage = 'messages';
  }

  function goToCallLog(simId) {
    filterSimId = simId;
    currentPage = 'calllog';
  }

  function goToSimCards(simId) {
    filterSimId = simId;
    currentPage = 'simcards';
  }

  function goToSetPhone(simId) {
    filterSimId = simId;
    currentPage = 'setphone';
  }

  function goToPlatform() {
    currentPage = 'platform';
  }

  function goToPhoneNumber() {
    currentPage = 'phonenumber';
  }

  $effect(() => {
    if ($isAuthenticated) {
      initConversation();
      connectCallSSE();
    } else {
      disconnectCallSSE();
    }
  });
</script>

<div class="app-container">
  {#if $isAuthLoading}
    <div class="h-dvh w-screen flex items-center justify-center text-gray-500 dark:text-gray-400">
      Loading...
    </div>
  {:else}
    <div class="h-dvh w-screen overflow-hidden">
      {#if $isAuthenticated}

        {#if currentPage === 'sim'}
          <div in:fly={{ x: -40, duration: 350, easing: quartOut }} out:fly={{ x: -40, duration: 250, easing: quartOut }}>
            <SimDashboard
              onNavigate={goToMessages}
              onNavigateCall={goToCallLog}
              onNavigateSim={goToSimCards}
              onNavigateSetPhone={goToSetPhone}
              onNavigatePlatform={goToPlatform}
              onNavigatePhoneNumber={goToPhoneNumber}
            />
          </div>

        {:else if currentPage === 'messages'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <Dashboard onNavigate={goToSim} initialSimId={filterSimId} />
          </div>

        {:else if currentPage === 'calllog'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <CallLogPage onBack={goToSim} filterSimId={filterSimId} />
          </div>

        {:else if currentPage === 'simcards'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <SimCardsPage onBack={goToSim} filterSimId={filterSimId} />
          </div>

        {:else if currentPage === 'setphone'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <SimSetPhonePage onBack={goToSim} simId={filterSimId} />
          </div>

        {:else if currentPage === 'platform'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <PlatformConnectPage onBack={goToSim} />
          </div>

        {:else if currentPage === 'phonenumber'}
          <div in:fly={{ x: 40, duration: 350, easing: quartOut }} out:fly={{ x: 40, duration: 250, easing: quartOut }}>
            <PhoneNumberPage onBack={goToSim} />
          </div>
        {/if}

      {:else}
        <div
          in:fly={{ x: -50, duration: 500, easing: quartOut }}
          out:fly={{ x: 50, duration: 300, easing: quartOut }}
        >
          <Login />
        </div>
      {/if}
    </div>
  {/if}
</div>
