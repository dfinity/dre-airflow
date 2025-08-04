<script lang="ts">
    import Alert from "./Alert.svelte";

    interface Props {
        children: any;
    }

    let { children }: Props = $props();
</script>

<!-- note use of me-3 in svg icon to ensure icon actually shows not too stuck to the text -->
<Alert type="black-info">{@render children()}</Alert>
