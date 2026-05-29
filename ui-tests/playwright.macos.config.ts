import { devices } from '@playwright/test';
import config from './playwright.base.config';

config.projects = [
    {
        name: 'webkit',
        use: { ...devices['Desktop Safari'] },
    },
];

export default config;
