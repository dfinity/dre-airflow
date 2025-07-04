export function selectTextOnFocus(node: HTMLDivElement | HTMLAnchorElement) {
    const handleFocus = (event: Event) => {
        function selectText(element: HTMLDivElement | HTMLAnchorElement) {
            if (window.getSelection != null) {
                var range = document.createRange();
                range.selectNode(element);
                var selection = window.getSelection();
                if (selection != null) {
                    selection.removeAllRanges();
                    selection.addRange(range);
                }
            }
        }
        node && selectText(node);
    };
    const handleDeFocus = (event: Event) => {
        function clearSelection() {
            if (window.getSelection != null) {
                var selection = window.getSelection();
                if (selection !== null) {
                    selection.removeAllRanges();
                }
            }
        }
        node && clearSelection();
    };

    node.addEventListener("focus", handleFocus);
    node.addEventListener("focusout", handleDeFocus);

    return {
        destroy() {
            node.removeEventListener("focus", handleFocus);
            node.removeEventListener("focusout", handleDeFocus);
        },
    };
}

export function cap(val: String) {
    return String(val).charAt(0).toUpperCase() + String(val).slice(1);
}

export function activeClass(rolloutClass: String) {
    if (rolloutClass !== "complete" && rolloutClass !== "failed") {
        rolloutClass = "active";
    }
    return rolloutClass;
}