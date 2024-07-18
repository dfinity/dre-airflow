<h1>Rollouts</h1>

<script>
  import { onMount } from 'svelte';
  import { rollouts } from './lib/stores.js';
  import Rollout from './lib/Rollout.svelte';
  import { writable } from 'svelte/store'

  let my_rollouts = writable([]);
  onMount( async() => {
      my_rollouts = rollouts()
  })	
</script>

{#each $my_rollouts as rollout}
<Rollout rollout={rollout}/>
{/each}
