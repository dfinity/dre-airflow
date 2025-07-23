<script lang="ts">
    import { rollouts_view } from "../lib/stores.js";
    import { rolloutKindName, getRolloutEngineStates } from "../lib/types.js";
    import GuestOSRollout from "../lib/GuestOSRollout.svelte";
    import ApiBoundaryNodesRollout from "../lib/ApiBoundaryNodesRollout.svelte";
    import HostOsRollout from "../lib/HostOSRollout.svelte";
    import { FooterCopyright } from "flowbite-svelte";
    import {
        Footer,
        FooterLink,
        FooterLinkGroup,
        FooterBrand,
    } from "flowbite-svelte";
    import { SvelteToast } from "@zerodevx/svelte-toast";
    import { ChevronDownOutline } from "flowbite-svelte-icons";

    let view = rollouts_view();

    import {
        Navbar,
        NavLi,
        NavUl,
        NavBrand,
        NavHamburger,
        DropdownItem,
        Dropdown,
    } from "flowbite-svelte";

    import { url, isActive } from "@roxi/routify";
    import { writable } from "svelte/store";

    let rolloutsState = writable("active");
    let currentUrl = writable("/");

    $: {
        let s = "active";
        if ($isActive(".", { state: "failed" })) {
            s = "failed";
        } else if ($isActive(".", { state: "complete" })) {
            s = "complete";
        } else if ($isActive(".", { state: "all" })) {
            s = "all";
        }
        rolloutsState.set(s);
    }
    $: {
        let c = $url(".");
        if ($rolloutsState === "failed") {
            c = $url(".", { state: "failed" });
        } else if ($rolloutsState === "complete") {
            c = $url(".", { state: "complete" });
        } else if ($rolloutsState === "all") {
            c = $url(".", { state: "all" });
        }
        currentUrl.set(c);
    }
</script>

<SvelteToast />

<div id="header">
    {#key $currentUrl}
        <Navbar>
            <NavBrand href="/">
                <img
                    src="/favicon-512x512.png"
                    class="me-3 h-6 sm:h-9"
                    alt="IC Logo"
                />
                <span
                    class="self-center whitespace-nowrap text-xl font-semibold dark:text-white"
                    >Rollouts</span
                >
            </NavBrand>
            <NavHamburger />
            <NavUl activeUrl={$currentUrl}>
                <NavLi class="cursor-pointer"
                    >Resources<ChevronDownOutline
                        class="w-6 h-6 ms-2 text-primary-800 dark:text-white inline"
                    />
                </NavLi>
                <Dropdown class="w-44 z-20">
                    <DropdownItem
                        href="https://grafana.mainnet.dfinity.network/d/release/release?orgId=1&from=now-7d&to=now&var-ic=mercury&var-ic_subnet=$__all&refresh=30s"
                        target="_blank">Mainnet versions</DropdownItem
                    >
                    <DropdownItem
                        href="https://grafana.ch1-rel1.dfinity.network/d/release-controller/release-controller?from=now-30m&to=now&timezone=UTC&refresh=30s"
                        target="_blank">Release controller</DropdownItem
                    >
                </Dropdown>
                <NavLi href={$url(".")}>Active</NavLi>
                <NavLi href={$url(".", { state: "complete" })}>Complete</NavLi>
                <NavLi href={$url(".", { state: "failed" })}>Failed</NavLi>
                <NavLi href={$url(".", { state: "all" })}>All</NavLi>
            </NavUl>
        </Navbar>
    {/key}

    {#if $view.error && $view.error !== "loading"}
        <!-- note use of me-3 in svg icon to ensure icon actually shows not too stuck to the text -->
        <div
            class="flex items-center p-4 mb-4 text-sm text-red-800 border border-red-300 rounded-lg bg-red-50 dark:bg-gray-800 dark:text-red-400 dark:border-red-800"
            role="alert"
        >
            <svg
                class="w-6 h-6 me-3 text-gray-800 dark:text-white"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                fill="currentColor"
                viewBox="0 0 24 24"
            >
                <path
                    fill-rule="evenodd"
                    d="M2 12C2 6.477 6.477 2 12 2s10 4.477 10 10-4.477 10-10 10S2 17.523 2 12Zm11-4a1 1 0 1 0-2 0v5a1 1 0 1 0 2 0V8Zm-1 7a1 1 0 1 0 0 2h.01a1 1 0 1 0 0-2H12Z"
                    clip-rule="evenodd"
                />
            </svg>
            <span class="sr-only">Error</span>
            <div>
                <span class="font-medium">Cannot retrieve rollout data:</span>
                {$view.error}
            </div>
        </div>
    {/if}

    {#if $view.error === "loading"}
        <div
            class="flex items-center justify-center w-56 h-56 border border-gray-200 rounded-lg bg-gray-50 dark:bg-gray-800 dark:border-gray-700"
            style="margin-left: auto; margin-right: auto;"
        >
            <div role="status">
                <svg
                    aria-hidden="true"
                    class="w-8 h-8 text-gray-200 animate-spin dark:text-gray-600 fill-blue-600"
                    viewBox="0 0 100 101"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                    ><path
                        d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z"
                        fill="currentColor"
                    /><path
                        d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z"
                        fill="currentFill"
                    /></svg
                >
                <span class="sr-only">Loading...</span>
            </div>
        </div>
    {/if}

    {#each getRolloutEngineStates($view.rollout_engine_states) as [kind, state]}
        {#if state === "missing"}
            <div
                class="flex items-center p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300"
                role="alert"
            >
                <svg
                    class="flex-shrink-0 inline w-4 h-4 me-3"
                    aria-hidden="true"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                >
                    <path
                        d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"
                    />
                </svg>
                <span class="sr-only">Info</span>
                <div>
                    <span class="font-medium"
                        >{rolloutKindName(kind)} flow missing.</span
                    >
                    Airflow cannot find the flow in charge of executing the {rolloutKindName(
                        kind,
                    )}. Use the <i>Help</i> link below to contact DRE.
                </div>
            </div>
        {/if}

        {#if state === "inactive"}
            <div
                class="flex items-center p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300"
                role="alert"
            >
                <svg
                    class="flex-shrink-0 inline w-4 h-4 me-3"
                    aria-hidden="true"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                >
                    <path
                        d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"
                    />
                </svg>
                <span class="sr-only">Info</span>
                <div>
                    <span class="font-medium"
                        >{rolloutKindName(kind)} inactive.</span
                    >
                    The Airflow scheduler cannot see the flow in charge of executing
                    the
                    {rolloutKindName(kind)}. Use the
                    <i>Help</i> link below to contact DRE.
                </div>
            </div>
        {/if}

        {#if state === "broken"}
            <div
                class="flex items-center p-4 mb-4 text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300"
                role="alert"
            >
                <svg
                    class="flex-shrink-0 inline w-4 h-4 me-3"
                    aria-hidden="true"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                >
                    <path
                        d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"
                    />
                </svg>
                <span class="sr-only">Info</span>
                <div>
                    <span class="font-medium"
                        >{rolloutKindName(kind)} flow broken.</span
                    >
                    The Airflow scheduler cannot process the flow in charge of executing
                    the
                    {rolloutKindName(kind)}. Use the <i>Help</i> link below to contact
                    DRE.
                </div>
            </div>
        {/if}

        {#if state === "paused"}
            <div
                class="flex items-center p-4 mb-4 text-sm text-blue-800 rounded-lg bg-blue-50 dark:bg-gray-800 dark:text-blue-400"
                role="alert"
            >
                <svg
                    class="flex-shrink-0 inline w-4 h-4 me-3"
                    aria-hidden="true"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="currentColor"
                    viewBox="0 0 20 20"
                >
                    <path
                        d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"
                    />
                </svg>
                <span class="sr-only">Info</span>
                <div>
                    <span class="font-medium"
                        >{rolloutKindName(kind)} engine paused.</span
                    >
                    {rolloutKindName(kind)} has been paused by DRE. Use the
                    <i>Help</i> link below if you want to inquire why.
                </div>
            </div>
        {/if}
    {/each}
</div>

{#each $view.rollouts as rollout}
    {#if ($rolloutsState === "active" && rollout.state !== "complete" && rollout.state !== "failed") || ($rolloutsState === "complete" && rollout.state === "complete") || ($rolloutsState === "failed" && rollout.state === "failed") || $rolloutsState === "all"}
        {#if rollout.kind === "rollout_ic_os_to_mainnet_subnets"}
            <GuestOSRollout {rollout} />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_api_boundary_nodes"}
            <ApiBoundaryNodesRollout {rollout} />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_nodes"}
            <HostOsRollout {rollout} />
        {/if}
    {/if}
{/each}

<Footer>
    <div class="sm:flex sm:items-center sm:justify-between">
        <FooterCopyright by="DFINITY Foundation" copyrightMessage="/ Apache 2.0"
        ></FooterCopyright>
        <FooterBrand name="Rollout dashboard" src="favicon-512x512.png"
        ></FooterBrand>
        <FooterLinkGroup
            ulClass="flex flex-wrap items-center mt-3 text-sm text-gray-500 dark:text-gray-400 sm:mt-0"
        >
            <FooterLink
                href="https://github.com/dfinity/dre-airflow/tree/main/rollout-dashboard"
                target="_blank">Documentation</FooterLink
            >
            <FooterLink
                href="https://dfinity.enterprise.slack.com/archives/C01DB8MQ5M1"
                target="_blank">Help</FooterLink
            >
        </FooterLinkGroup>
    </div>
</Footer>
