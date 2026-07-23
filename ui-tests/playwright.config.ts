import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
    testDir: 'tests/e2e',
    // CI shards across runners so each shard wants just one worker; locally
    // we run a single process, so more workers gives a meaningful speedup.
    workers: process.env.CI ? 1 : 2,
    // Required so that --shard distributes individual tests across shards
    // instead of whole spec files (we have very uneven file sizes).
    fullyParallel: true,
    forbidOnly: !!process.env.CI, // Fail the build on CI if you accidentally left test.only in the source code
    retries: 1,
    // Stop the run gracefully before the CI job's 30-minute step timeout so the HTML report is still written and uploaded.
    globalTimeout: 25 * 60 * 1000,
    reporter: [
        [
            'html',
            {
                open: 'on-failure',
                host: '0.0.0.0',
                port: parseInt(process.env.REPORT_PORT ?? '9323'),
                ...(process.env.PLAYWRIGHT_HTML_REPORT && {
                    outputFolder: process.env.PLAYWRIGHT_HTML_REPORT,
                }),
            },
        ],
    ],
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
    projects: [
        {
            name: 'chromium',
            use: { ...devices['Desktop Chrome'] },
        },
        {
            name: 'firefox',
            use: { ...devices['Desktop Firefox'] },
        },
        {
            name: 'webkit',
            use: { ...devices['Desktop Safari'] },
        },
    ],
});
