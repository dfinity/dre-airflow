import { defineConfig, type PluginOption } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { visualizer } from 'rollup-plugin-visualizer';
import tailwindcss from '@tailwindcss/vite';

const BASE_URL = 'http://localhost:4174';

export default defineConfig({
	plugins: [
		tailwindcss(),
		svelte(),
		visualizer({ emitFile: true, filename: "stats.html" }) as PluginOption,
	],
	server: { proxy: { '/api': { target: BASE_URL } } }
});
