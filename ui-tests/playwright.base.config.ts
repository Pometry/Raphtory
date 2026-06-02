import { defineConfig } from '@playwright/test';

export default defineConfig({
    testDir: 'tests/e2e',
    workers: 1,
    // Required so that --shard distributes individual tests across shards
    // instead of whole spec files (we have very uneven file sizes).
    fullyParallel: true,
    forbidOnly: !!process.env.CI, // Fail the build on CI if you accidentally left test.only in the source code
    retries: 1,
    // Stop the run gracefully before the CI job's 30-minute step timeout so the HTML report is still written and uploaded.
    globalTimeout: 25 * 60 * 1000,
    reporter: [['html', { open: 'on-failure', host: '0.0.0.0', port: 9323 }]],
    use: {
        timezoneId: 'Europe/London',
        locale: 'en-US',
        baseURL: process.env.UI_BASE_URL,
        trace: 'on-first-retry',
        video: 'on-first-retry',
    },
    timeout: 30000,
    expect: {
        toMatchSnapshot: {
            maxDiffPixels: 2000,
        },
    },
});
