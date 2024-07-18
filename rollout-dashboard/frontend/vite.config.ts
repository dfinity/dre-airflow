import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

const BASE_URL = 'http://localhost:4174';

export default defineConfig({
	plugins: [svelte()],
	server: {
		proxy: {
			'/api': { target: BASE_URL },
		},
	},
})
