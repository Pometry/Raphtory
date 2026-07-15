import { defineConfig } from '@playwright/test';

/**
 * Prerequisites for running this perf project:
 *   1. cd applications/jira/BTS && make graphs    # one-time, generates the master graph
 *   2. cd applications/jira/BTS && make run       # starts Raphtory GraphQL on :1736
 *   3. ENABLE_TESTING=true pnpm build && pnpm build-vanilla && pnpm start
 *                                                  # builds + serves vanilla on :3000.
 *
 * Then: pnpm test:perf
 *
 * Override the URL via UI_BASE_URL if vanilla is on a different host/port.
 */
export default defineConfig({
    testDir: '.',
    // Generous test timeout — bulk-add iterations on >1000 nodes can take
    // many seconds each, and we run 5 iterations.
    timeout: 15 * 60 * 1000,
    workers: 1, // perf measurements need exclusive resources
    use: {
        baseURL: process.env.UI_BASE_URL ?? 'http://localhost:3000',
        headless: false,
    },
    projects: [{ name: 'perf', use: { browserName: 'chromium' } }],
});
