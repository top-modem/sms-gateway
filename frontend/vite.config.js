import { defineConfig, loadEnv } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd())
  const pkg = JSON.parse(readFileSync(resolve(process.cwd(), 'package.json'), 'utf-8'))

  return {
    define: {
      __APP_VERSION__: JSON.stringify(pkg.version),
    },
    server: {
      host: env.VITE_DEV_HOST || 'localhost',
      port: 5173,
      allowedHosts: true,
      proxy: {
        '/api': {
          target: env.VITE_API_PROXY,
          changeOrigin: true,
          secure: false,
          ws: false,
        },
      },
    },
    plugins: [svelte()],
    build: {
      terserOptions: {
        compress: {
          drop_console: true,
          drop_debugger: true,
        },
      },
      rollupOptions: {
        output: {
          manualChunks: {
            vendor: ['svelte'],
          },
        },
      },
      chunkSizeWarningLimit: 1000,
    },
  }
})
