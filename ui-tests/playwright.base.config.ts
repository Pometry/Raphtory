import { defineConfig } from '@playwright/test';

export default defineConfig({
    testDir: 'tests/e2e',
    workers: 1,
    forbidOnly: !!process.env.CI, // Fail the build on CI if you accidentally left test.only in the source code
    retries: 2,
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
