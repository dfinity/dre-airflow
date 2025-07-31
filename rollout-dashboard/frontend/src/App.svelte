<script module>
  import { Router, createRouter } from "@roxi/routify";
  import routes from "../.routify/routes.default.js";
  const router = createRouter({
    routes,
    urlRewrite: {
      toExternal: (url) => `#${url}`, // prepend URLs with #
      toInternal: (url) => url.replace(/^.+#/, ""), // remove leading #
    },
  });
  import {
    FooterCopyright,
    Footer,
    FooterLink,
    FooterLinkGroup,
    FooterBrand,
  } from "flowbite-svelte";
</script>

<script lang="ts">
  import "./app.css";
</script>

<Router {router} />

<Footer footerType="logo">
  <div class="sm:flex sm:items-center sm:justify-between">
    <FooterCopyright by="DFINITY Foundation" copyrightMessage="/ Apache 2.0"
    ></FooterCopyright>
    <FooterBrand name="Rollout dashboard" src="favicon-512x512.png"
    ></FooterBrand>
    <FooterLinkGroup
      class="flex flex-wrap items-center mt-3 text-sm text-gray-500 dark:text-gray-400 sm:mt-0"
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
