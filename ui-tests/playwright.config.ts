import { devices } from '@playwright/test';
import config from './playwright.base.config';

config.projects = [
    {
        name: 'chromium',
        use: { ...devices['Desktop Chrome'] },
    },

    {
        name: 'firefox',
        use: { ...devices['Desktop Firefox'] },
    },
];

export default config;
