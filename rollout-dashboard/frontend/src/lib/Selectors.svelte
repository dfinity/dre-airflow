<script lang="ts">
    import { formatSpecifier, type HostOsNodeSelectors } from "./types";
    import Selectors from "./Selectors.svelte";

    interface Props {
        selectors: HostOsNodeSelectors | null;
    }

    let { selectors }: Props = $props();
</script>

{#if selectors === null}
    Selectors not known
{:else if "intersect" in selectors}
    {#if selectors.intersect.length === 0}
        <div class="specifier">All remaining nodes</div>
    {:else if selectors.intersect.length === 1}
        <Selectors selectors={selectors.intersect[0]} />
    {:else}
        <div class="intersect">
            {#each Object.entries(selectors.intersect) as [index, selector]}
                {#if index !== "0"}<div
                        class="operator"
                        title="intersected with"
                    >
                        ⋂
                    </div>{/if}
                <Selectors selectors={selector} />
            {/each}
        </div>
    {/if}
{:else if "join" in selectors}
    {#if selectors.join.length === 0}
        <div class="specifier">No nodes</div>
    {:else if selectors.join.length === 1}
        <Selectors selectors={selectors.join[0]} />
    {:else}
        <div class="join">
            {#each Object.entries(selectors.join) as [index, selector]}
                {#if index !== "0"}<div class="operator" title="joined to">
                        ⋃
                    </div>{/if}
                <Selectors selectors={selector} />
            {/each}
        </div>
    {/if}
{:else if "not" in selectors}
    <div class="complement">
        <div class="operator" title="except for">∖</div>
        <Selectors selectors={selectors.not} />
    </div>
{:else}
    <div class="specifier">{formatSpecifier(selectors)}</div>
{/if}

<style>
    div {
        display: flex;
        text-align: center;
        justify-content: center;
        align-self: center;
        padding: 0.5em;
        border-radius: 1.5em;
        border: 1px solid transparent;
        flex-wrap: wrap;
        gap: 1em;
        flex-direction: column;
    }
    .intersect {
        background-color: rgb(233, 220, 243);
    }
    .join {
        background-color: rgb(209, 221, 236);
    }
    .complement {
        background-color: rgb(248, 187, 187);
    }
    .specifier {
        border-style: dashed;
        border-color: rgb(21, 51, 9);
        padding-top: 0.5em;
        padding-bottom: 0.5em;
        background-color: white;
    }
    .operator {
        padding: 0em;
        font-size: 150%;
        font-weight: bold;
        line-height: 0.5em;
    }
    /* .complement > * {
        background-color: transparent;
        border-style: none;
        padding: 0em;
    } */
</style>
