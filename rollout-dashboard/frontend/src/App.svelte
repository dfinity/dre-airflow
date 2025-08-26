<script module>
</script>

<script lang="ts">
  import "./app.css";
  import {
    FooterCopyright,
    Footer,
    FooterLink,
    FooterLinkGroup,
    FooterBrand,
  } from "flowbite-svelte";

  import Index from "./lib/index.svelte";
  import HostOsBatchDetail from "./lib/HostOSBatchDetail.svelte";
  import { writable, type Writable } from "svelte/store";
  import NotFound from "./lib/NotFound.svelte";
  import { rollouts_view_with_cancellation } from "./lib/stores";
  import { onDestroy } from "svelte";

  type index_route = { name: "index" };
  type not_found = { name: "NotFound" };
  type host_os_batch_detail_route = {
    name: "HostOsBatchDetail";
    dag_run_id: string;
    stage_name: string;
    batch_number: number;
  };
  type routes = index_route | host_os_batch_detail_route | not_found;

  function determineRoute(fragment: string): routes | null {
    if (fragment == "" || fragment == "#/" || fragment === "#") {
      return { name: "index" };
    }

    let m = fragment.match(
      /#\/rollouts\/rollout_ic_os_to_mainnet_nodes\/(?<dag_run_id>.+)\/stages\/(?<stage_name>.+)\/batches\/(?<batch_number>.+)/,
    );
    if (m !== null && m.groups !== undefined) {
      return {
        name: "HostOsBatchDetail",
        dag_run_id: decodeURIComponent(m.groups.dag_run_id),
        stage_name: decodeURIComponent(m.groups.stage_name),
        batch_number: parseInt(decodeURIComponent(m.groups.batch_number)),
      };
    }
    return { name: "NotFound" };
  }
  let url = new URL(document.URL);
  let selectedRoute = writable(determineRoute(url.hash)) as Writable<routes>;

  // @ts-ignore
  navigation.addEventListener("navigate", (e) => {
    // Get the path for the URL being navigated to
    let u = new URL(e.destination.url);
    let path = u.pathname;
    if (path == "/index.html") {
      path = "/";
    }
    if (path !== "/") return;

    console.log(`About to navigate to ${path} with fragment ${u.hash}`);
    let possible_route = determineRoute(u.hash);
    console.log(possible_route);
    if (possible_route) {
      e.intercept({
        handler: function () {
          selectedRoute.set(possible_route);
        },
      });
    }

    // If it's not covered by your SPA, return and it works as normal
    // For me, this means if it's not part of my admin path
    // if (!url.startsWith("/admin")) return;
  });

  let [rollouts_view, cancel] = rollouts_view_with_cancellation();
  onDestroy(() => {
    cancel();
  });
</script>

<div>
  {#key $selectedRoute}
    {#if $selectedRoute.name === "index"}
      <Index {rollouts_view} />
    {:else if $selectedRoute.name === "HostOsBatchDetail"}
      <HostOsBatchDetail
        dag_run_id={$selectedRoute.dag_run_id}
        stage_name={$selectedRoute.stage_name}
        batch_number={$selectedRoute.batch_number}
        {rollouts_view}
      />
    {:else}
      <NotFound />
    {/if}
  {/key}
</div>

<Footer footerType="logo">
  <div class="sm:flex sm:items-center sm:justify-between">
    <FooterCopyright by="DFINITY Foundation" copyrightMessage="/ Apache 2.0"
    ></FooterCopyright>
    <FooterBrand name="Rollout dashboard" src="favicon-512x512.png"
    ></FooterBrand>
    <FooterLinkGroup
      class="flex flex-wrap items-center mt-3 text-sm text-gray-500 dark:text-gray-400 sm:mt-0"
    >
      <FooterLink href="/doc/rollout_dashboard" target="_blank"
        >Documentation</FooterLink
      >
      <FooterLink
        href="https://dfinity.enterprise.slack.com/archives/C01DB8MQ5M1"
        target="_blank">Help</FooterLink
      >
    </FooterLinkGroup>
  </div>
</Footer>
