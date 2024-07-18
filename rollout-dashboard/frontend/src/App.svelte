<script lang="ts">
  import { onMount } from 'svelte';
  import url from './lib/url.js';
  import { rollouts } from './lib/stores.js';
  import Rollout from './lib/Rollout.svelte';
  import { writable } from 'svelte/store'
  import { ButtonGroup, Button } from 'flowbite-svelte';

  let my_rollouts = writable([]);
  onMount( async() => {
      my_rollouts = rollouts()
  })	

</script>

<ButtonGroup class="*:!ring-primary-700">
  <Button href="#active">Active</Button>
  <Button href="#complete">Complete</Button>
  <Button href="#failed">Failed</Button>
  <Button href="#all">All</Button>
</ButtonGroup>

{#each $my_rollouts as rollout}
{#if
  (
    (
      $url.hash === ''
      ||
      $url.hash === '#active'
    ) && (
      rollout.state !== "complete"
      &&
      rollout.state !== "failed"
    )
  )
  ||
  (
    $url.hash === '#complete'
    && 
    rollout.state === "complete"
  )
  ||
  (
    $url.hash === '#failed'
    && 
    rollout.state === "failed"
  )
  ||
  (
    $url.hash === '#all'
  )
}
<Rollout rollout={rollout}/>
{/if}
{/each}
