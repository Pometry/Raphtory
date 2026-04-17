import { expect, test } from '@playwright/test';

test('Page has title', async ({ page }) => {
    await page.goto('/');

    await expect(page).toHaveTitle('Pometry UI');
});

test('Search page link works', async ({ page }) => {
    await page.goto('/');

    await page.getByRole('button', { name: 'Search for a graph' }).click();
    await expect(page).toHaveURL(/\/search$/);
    await expect(page.getByText('Search for entities')).toBeVisible();
});

test('Saved graphs page link works', async ({ page }) => {
    await page.goto('/');

    await page.getByRole('button', { name: 'List of saved graphs' }).click();
    await expect(page).toHaveURL(/\/saved-graphs$/);
    await expect(
        page.getByText(
            'To see object details, please select an object in the graph canvas.',
        ),
    ).toBeVisible();
});

test('Home page link works', async ({ page }) => {
    await page.goto('/');

    await page.locator('img[alt="Pometry Logo"]').nth(0).click();
    await expect(page).toHaveURL(/\/search$/);
    await expect(page.getByText('Search for entities')).toBeVisible();
});

test('Playground link works', async ({ page }) => {
    await page.goto('/');

    await page.getByRole('button', { name: 'GraphQL Playground' }).click();
    await expect(page).toHaveURL(/\/playground$/);
});
