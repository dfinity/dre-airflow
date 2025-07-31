import { defineConfig } from 'vite';
import routify from '@roxi/routify/vite-plugin';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

const BASE_URL = 'http://localhost:4174';

export default defineConfig({
	plugins: [
		tailwindcss(),
		svelte(),
		routify({})
	],
	server: { proxy: { '/api': { target: BASE_URL } } }
});
