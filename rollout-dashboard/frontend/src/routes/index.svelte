<script lang="ts">
    import { rollouts_view } from "../lib/stores.js";
    import { rolloutKindName, getRolloutEngineStates } from "../lib/types.js";
    import GuestOSRollout from "../lib/GuestOSRollout.svelte";
    import ApiBoundaryNodesRollout from "../lib/ApiBoundaryNodesRollout.svelte";
    import HostOsRollout from "../lib/HostOSRollout.svelte";

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
    import ErrorBlock from "../lib/ErrorBlock.svelte";
    import LoadingBlock from "../lib/LoadingBlock.svelte";
    import WarningBlock from "../lib/WarningBlock.svelte";
    import InfoBlock from "../lib/InfoBlock.svelte";

    let rolloutsState = $derived.by(() => {
        let s = "active";
        if ($isActive(".", { state: "failed" })) {
            s = "failed";
        } else if ($isActive(".", { state: "complete" })) {
            s = "complete";
        } else if ($isActive(".", { state: "all" })) {
            s = "all";
        }
        return s;
    });
    let currentUrl = $derived.by(() => {
        let c = $url(".");
        if (rolloutsState === "failed") {
            c = $url(".", { state: "failed" });
        } else if (rolloutsState === "complete") {
            c = $url(".", { state: "complete" });
        } else if (rolloutsState === "all") {
            c = $url(".", { state: "all" });
        }
        return c;
    });

    let activeClass =
        "text-white bg-orange-700 md:bg-transparent md:text-orange-700 md:dark:text-white dark:bg-orange-600 md:dark:bg-transparent";
    let nonActiveClass =
        "text-gray-700 hover:bg-gray-100 md:hover:bg-transparent md:border-0 md:hover:text-orange-700 dark:text-gray-400 md:dark:hover:text-white dark:hover:bg-gray-700 dark:hover:text-white md:dark:hover:bg-transparent";
    let navClasses = { active: activeClass, nonActive: nonActiveClass };
</script>

<SvelteToast />

<div id="header">
    {#key currentUrl}
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
            <NavUl activeUrl={currentUrl} classes={navClasses}>
                <NavLi class="cursor-pointer"
                    >Resources<ChevronDownOutline
                        class="text-primary-800 dark:text-white inline"
                    />
                </NavLi>
                <Dropdown class="w-44 z-20">
                    <DropdownItem
                        ><a
                            href="https://grafana.mainnet.dfinity.network/d/release/release?orgId=1&from=now-7d&to=now&var-ic=mercury&var-ic_subnet=$__all&refresh=30s"
                            target="_blank">Mainnet GuestOS versions</a
                        ></DropdownItem
                    >
                    <DropdownItem
                        ><a
                            href="https://grafana.mainnet.dfinity.network/d/hostos-versions/hostos-versions"
                            target="_blank">Mainnet HostOS versions</a
                        ></DropdownItem
                    >
                    <DropdownItem
                        ><a
                            href="https://grafana.ch1-rel1.dfinity.network/d/release-controller/release-controller?from=now-30m&to=now&timezone=UTC&refresh=30s"
                            target="_blank">Release controller</a
                        ></DropdownItem
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
        <ErrorBlock>
            <span class="font-medium">Cannot retrieve rollout data:</span>
            {$view.error}
        </ErrorBlock>
    {/if}

    {#if $view.error === "loading"}
        <LoadingBlock />
    {/if}
    {#each getRolloutEngineStates($view.rollout_engine_states) as [kind, state]}
        {#if state === "missing"}
            <WarningBlock>
                <span class="font-medium"
                    >{rolloutKindName(kind)} flow missing.</span
                >
                Airflow cannot find the flow in charge of executing the {rolloutKindName(
                    kind,
                )}. Use the <i>Help</i> link below to contact DRE.
            </WarningBlock>
        {/if}

        {#if state === "inactive"}
            <WarningBlock>
                <span class="font-medium"
                    >{rolloutKindName(kind)} inactive.</span
                >
                The Airflow scheduler cannot see the flow in charge of executing
                the
                {rolloutKindName(kind)}. Use the
                <i>Help</i> link below to contact DRE.
            </WarningBlock>
        {/if}

        {#if state === "broken"}<WarningBlock>
                <span class="font-medium"
                    >{rolloutKindName(kind)} flow broken.</span
                >
                The Airflow scheduler cannot process the flow in charge of executing
                the
                {rolloutKindName(kind)}. Use the <i>Help</i> link below to contact
                DRE.
            </WarningBlock>
        {/if}

        {#if state === "paused"}
            <InfoBlock>
                <span class="font-medium"
                    >{rolloutKindName(kind)} engine paused.</span
                >
                {rolloutKindName(kind)} has been paused by DRE. Use the
                <i>Help</i> link below if you want to inquire why.
            </InfoBlock>
        {/if}
    {/each}
</div>

{#each $view.rollouts as rollout}
    {#if (rolloutsState === "active" && rollout.state !== "complete" && rollout.state !== "failed") || (rolloutsState === "complete" && rollout.state === "complete") || (rolloutsState === "failed" && rollout.state === "failed") || rolloutsState === "all"}
        {#if rollout.kind === "rollout_ic_os_to_mainnet_subnets"}
            <GuestOSRollout {rollout} />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_api_boundary_nodes"}
            <ApiBoundaryNodesRollout {rollout} />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_nodes"}
            <HostOsRollout {rollout} />
        {/if}
    {/if}
{/each}
