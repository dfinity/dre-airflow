<script context="module">
  import { Router, createRouter } from "@roxi/routify";
  import routes from "../.routify/routes.default.js";
  const router = createRouter({
    routes,
    urlRewrite: {
      toExternal: (url) => `#${url}`, // prepend URLs with #
      toInternal: (url) => url.replace(/^.+#/, ""), // remove leading #
    },
  });
</script>

<Router {router} />
