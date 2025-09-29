<script lang="ts">
    import Alert from "./Alert.svelte";

    interface Props {
        children: any;
        shrink_to_content?: "true" | "false";
    }

    let { children, shrink_to_content = "false" }: Props = $props();
</script>

<!-- note use of me-3 in svg icon to ensure icon actually shows not too stuck to the text -->
<Alert type="warning" {shrink_to_content}>{@render children()}</Alert>
