import { expect, test } from '@playwright/test';

import { navigateInSavedGraphs } from './saved-graphs.utils';
import { setNavbarExpanded } from './utils';

test('Page has title', async ({ page }) => {
    await page.goto('/');

    await expect(page).toHaveTitle('Search | Pometry UI');
});

test('Search page link works', async ({ page }) => {
    await page.goto('/saved-graphs');

    await page.getByRole('link', { name: 'Search', exact: true }).click();
    await expect(page).toHaveTitle('Search | Pometry UI');
    await expect(page).toHaveURL(/\/search$/);
    await expect(page.getByText('Start Your Search')).toBeVisible();
});

test('Saved graphs page link works', async ({ page }) => {
    await page.goto('/');

    await page.getByRole('link', { name: 'Explorations', exact: true }).click();
    await expect(page).toHaveURL(/\/saved-graphs$/);
    await expect(
        page.getByRole('button', {
            name: 'new_folder FOLDER',
        }),
    ).toBeVisible();
});

test('Home page link works', async ({ page }) => {
    await page.goto('/saved-graphs');

    await page.getByRole('link', { name: 'Pometry', exact: true }).click();
    await expect(page).toHaveURL(/\/search$/);
    await expect(page).toHaveTitle('Search | Pometry UI');
    await expect(page.getByText('Start Your Search')).toBeVisible();
});

test('Playground link works', async ({ page }) => {
    await page.goto('/');

    await page.getByRole('link', { name: 'GraphQL Playground', exact: true }).click();
    await expect(page).toHaveURL(/\/playground$/);
});

test('Explorer link returns to the last viewed graph', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await expect(page).toHaveURL(/\/graph\/vanilla\/event/);

    await page.getByRole('link', { name: 'Search', exact: true }).click();
    await expect(page).toHaveURL(/\/search$/);

    await page.getByRole('link', { name: 'Explorer', exact: true }).click();
    await expect(page).toHaveURL(/\/graph\/vanilla\/event/);
});

test('Navbar state persists after page reload', async ({ page }) => {
    // Start expanded so the Collapse button is the action under test
    await setNavbarExpanded(page, true);
    await page.goto('/');

    await page.getByRole('button', { name: 'Collapse', exact: true }).click();
    await page.waitForTimeout(500);

    await page.reload();

    await page.getByRole('link', { name: 'Search', exact: true }).isVisible();
    await expect(page.getByRole('link', { name: 'Search', exact: true })).not.toHaveText('Search');

    await page.getByRole('button', { name: 'Expand', exact: false }).click();

    await page.reload();

    await page.getByRole('link', { name: 'Search', exact: true }).isVisible();
    await expect(page.getByRole('link', { name: 'Search', exact: true })).toHaveText('Search');
});

test('Browser back navigation restores previous page title', async ({ page }) => {
    // Start at saved graphs page
    await page.goto('/saved-graphs');
    await expect(page).toHaveTitle('Explorations | Pometry UI');

    // Navigate to search page
    await page.getByRole('link', { name: 'Search' }).click();
    await expect(page).toHaveTitle('Search | Pometry UI');

    // Go back
    await page.goBack();
    await expect(page).toHaveTitle('Explorations | Pometry UI');
});
