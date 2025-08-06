import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

const BASE_URL = 'http://localhost:4174';

export default defineConfig({
	plugins: [
		tailwindcss(),
		svelte(),
	],
	server: { proxy: { '/api': { target: BASE_URL } } }
});
