import { defineConfig } from 'vite'
import routify from '@roxi/routify/vite-plugin'
import { svelte } from '@sveltejs/vite-plugin-svelte'

const BASE_URL = 'http://localhost:4174';

export default defineConfig({
	plugins: [
		svelte(),
		routify({})
	],
	server: {
		proxy: {
			'/api': { target: BASE_URL },
		},
	},
})
