<script lang="ts">
    import { CircleMinusOutline } from "flowbite-svelte-icons";
    import { InfoCircleOutline } from "flowbite-svelte-icons";
    import { CloseCircleOutline } from "flowbite-svelte-icons";

    interface Props {
        children: any;
        type: "warning" | "info" | "black-info" | "error";
        shrink_to_content?: "true" | "false";
    }

    let { children, type, shrink_to_content = "false" }: Props = $props();
    let style = $state("");
    if (shrink_to_content === "true") {
        style = "width: fit-content";
    }
</script>

{#if type === "info"}
    <div
        class="flex items-center p-4 mb-2 mt-2 text-sm text-blue-800 border border-blue-300 rounded-lg bg-blue-50 dark:bg-blue-800 dark:text-blue-400 dark:border-blue-600"
        role="alert"
        {style}
    >
        <InfoCircleOutline class="shrink-0 h-6 w-6 me-3" />
        <span class="sr-only">Info</span>
        <div>{@render children()}</div>
    </div>
{:else if type === "black-info"}
    <div
        class="flex items-center p-4 mb-2 mt-2 text-sm text-gray-800 border border-gray-300 rounded-lg bg-gray-50 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600"
        role="alert"
        {style}
    >
        <InfoCircleOutline class="shrink-0 h-6 w-6 me-3" />
        <span class="sr-only">Info</span>
        <div>{@render children()}</div>
    </div>
{:else if type === "warning"}
    <div
        class="flex items-center p-4 mb-2 mt-2 text-sm text-yellow-800 border border-yellow-300 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300 dark:border-yellow-600"
        role="alert"
        {style}
    >
        <CircleMinusOutline class="shrink-0 h-6 w-6 me-3" />
        <span class="sr-only">Info</span>
        <div>
            {@render children()}
        </div>
    </div>
{:else}
    <div
        class="flex items-center p-4 mb-2 mt-2 text-sm text-red-800 border border-red-300 rounded-lg bg-red-50 dark:bg-gray-800 dark:text-red-400 dark:border-red-800"
        role="alert"
        {style}
    >
        <CloseCircleOutline class="shrink-0 h-6 w-6 me-3" />

        <span class="sr-only">Error</span>
        <div>
            {@render children()}
        </div>
    </div>
{/if}
