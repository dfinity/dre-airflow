<script lang="ts">
    import { rollouts_view } from "../lib/stores.js";
    import {
        rolloutKindName,
        getRolloutEngineStates,
        RolloutKindName,
    } from "../lib/types.js";
    import GuestOSRollout from "../lib/GuestOSRollout.svelte";
    import ApiBoundaryNodesRollout from "../lib/ApiBoundaryNodesRollout.svelte";
    import HostOsRollout from "../lib/HostOSRollout.svelte";
    import { onDestroy, onMount } from "svelte";

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
        Button,
        Checkbox,
        Listgroup,
        type CheckboxItem,
    } from "flowbite-svelte";

    import { url, isActive } from "@roxi/routify";
    import ErrorBlock from "../lib/ErrorBlock.svelte";
    import LoadingBlock from "../lib/LoadingBlock.svelte";
    import WarningBlock from "../lib/WarningBlock.svelte";
    import InfoBlock from "../lib/InfoBlock.svelte";
    import ExternalLinkIcon from "../lib/ExternalLinkIcon.svelte";

    let stateChoices: CheckboxItem[] = [
        { value: "active", label: "Active" },
        { value: "complete", label: "Complete" },
        { value: "failed", label: "Failed" },
    ];
    let visibleStates = $state(["active"]);

    let kindChoices: CheckboxItem[] = Object.entries(RolloutKindName).map(
        (v) => {
            return { value: v[0], label: v[1] };
        },
    );
    let visibleKinds = $state(
        Object.entries(RolloutKindName).map((v) => {
            return v[0];
        }),
    );

    /* Preserve filters between navigations. */
    type RolloutsViewFilters = {
        visibleStates: string[];
        visibleKinds: string[];
    };

    onMount(() => {
        let savedFiltersJSON: string | null = sessionStorage.getItem(
            "RolloutsViewFilters",
        );
        console.log(savedFiltersJSON);
        if (savedFiltersJSON !== null) {
            let savedFilters = JSON.parse(
                savedFiltersJSON,
            ) as RolloutsViewFilters;
            if (visibleStates != savedFilters.visibleStates) {
                visibleStates = savedFilters.visibleStates;
            }
            if (visibleKinds != savedFilters.visibleKinds) {
                visibleKinds = savedFilters.visibleKinds;
            }
        }
    });
    onDestroy(() => {
        let savedFilters = {
            visibleStates: visibleStates,
            visibleKinds: visibleKinds,
        } as RolloutsViewFilters;
        let savedFiltersJSON = JSON.stringify(savedFilters);
        sessionStorage.setItem("RolloutsViewFilters", savedFiltersJSON);
    });
</script>

<SvelteToast />

<div id="header">
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
        <NavUl>
            <NavLi class="cursor-pointer"
                >Resources<ChevronDownOutline
                    class="text-primary-800 dark:text-white inline"
                />
            </NavLi>
            <Dropdown class="w-72 z-20" placement="bottom-end">
                <DropdownItem
                    ><a
                        href="https://grafana.mainnet.dfinity.network/d/release/release?orgId=1&from=now-7d&to=now&var-ic=mercury&var-ic_subnet=$__all&refresh=30s"
                        target="_blank"
                        >Mainnet GuestOS versions <ExternalLinkIcon /></a
                    ></DropdownItem
                >
                <DropdownItem
                    ><a
                        href="https://grafana.mainnet.dfinity.network/d/hostos-versions/hostos-versions"
                        target="_blank"
                        >Mainnet HostOS versions <ExternalLinkIcon /></a
                    ></DropdownItem
                >
                <DropdownItem
                    ><a
                        href="https://grafana.ch1-rel1.dfinity.network/d/release-controller/release-controller?from=now-30m&to=now&timezone=UTC&refresh=30s"
                        target="_blank"
                        >Release controller <ExternalLinkIcon /></a
                    ></DropdownItem
                >
            </Dropdown>
            <NavLi class="cursor-pointer"
                >States<ChevronDownOutline
                    class="text-primary-800 dark:text-white inline"
                /></NavLi
            >
            <Dropdown class="space-y-3 p-3 text-sm" placement="bottom-end">
                <Listgroup class="w-32">
                    <Checkbox
                        name="states"
                        choices={stateChoices}
                        bind:group={visibleStates}
                        classes={{ div: "p-3" }}
                    />
                </Listgroup>
            </Dropdown>
            <NavLi class="cursor-pointer"
                >Rollouts<ChevronDownOutline
                    class="text-primary-800 dark:text-white inline"
                /></NavLi
            >
            <Dropdown class="space-y-3 p-3 text-sm" placement="bottom-end">
                <Listgroup class="w-80">
                    <Checkbox
                        name="kinds"
                        choices={kindChoices}
                        bind:group={visibleKinds}
                        classes={{ div: "p-3" }}
                    />
                </Listgroup>
            </Dropdown>
        </NavUl>
    </Navbar>

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
    {/each}
</div>

{#each $view.rollouts as rollout}
    {#if (visibleStates.includes("active") && rollout.state !== "complete" && rollout.state !== "failed") || (visibleStates.includes("complete") && rollout.state === "complete") || (visibleStates.includes("failed") && rollout.state === "failed")}
        {#if rollout.kind === "rollout_ic_os_to_mainnet_subnets" && visibleKinds.includes("rollout_ic_os_to_mainnet_subnets")}
            <GuestOSRollout
                {rollout}
                paused={$view.rollout_engine_states[
                    "rollout_ic_os_to_mainnet_subnets"
                ] === "paused"}
            />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_api_boundary_nodes" && visibleKinds.includes("rollout_ic_os_to_mainnet_api_boundary_nodes")}
            <ApiBoundaryNodesRollout
                {rollout}
                paused={$view.rollout_engine_states[
                    "rollout_ic_os_to_mainnet_api_boundary_nodes"
                ] === "paused"}
            />
        {:else if rollout.kind === "rollout_ic_os_to_mainnet_nodes" && visibleKinds.includes("rollout_ic_os_to_mainnet_nodes")}
            <HostOsRollout
                {rollout}
                paused={$view.rollout_engine_states[
                    "rollout_ic_os_to_mainnet_nodes"
                ] === "paused"}
            />
        {/if}
    {/if}
{/each}
