import { expect, test } from '@playwright/test';

test('Saved graphs table is visible', async ({ page }) => {
    await page.goto('/saved-graphs');

    await expect(
        page.getByText(
            'To see object details, please select an object in the graph canvas.',
        ),
    ).toBeVisible();

    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.getByRole('table').locator('tbody tr').first().click();
    await expect(page.getByText('Properties')).toBeVisible();
    await expect(page.getByText('Preview')).toBeVisible();
});

test('Change rows per page to 5 in table and check pagination', async ({
    page,
}) => {
    await page.goto('/saved-graphs');
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.getByRole('combobox').click();
    await page.getByRole('option', { exact: true, name: '5' }).click();
    const rows = page.locator('table tbody tr');
    await expect(rows).toHaveCount(5);
    await page.getByLabel('next page').click();
    await expect(rows).toHaveCount(2);
    await page.getByLabel('previous page').click();
    await expect(rows).toHaveCount(5);
    await page.getByRole('button', { name: '2' }).click();
    await expect(rows).toHaveCount(2);
    await page.getByRole('button', { name: '1' }).click();
    await expect(rows).toHaveCount(5);
});

test('Row sorting on saved graphs table by columns', async ({ page }) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    //test Name
    const nameHeader = table.getByRole('columnheader', { name: 'Name' });
    await nameHeader.click();
    const firstRow = table.locator('tbody tr').first();
    const firstRowName = await firstRow.locator('td').nth(1).textContent();
    await expect(firstRowName).toBe('second_filler');
    await nameHeader.click();
    const firstRowAfterSort = table.locator('tbody tr').first();
    const firstRowNameAfterSort = await firstRowAfterSort
        .locator('td')
        .nth(1)
        .textContent();
    await expect(firstRowNameAfterSort).toBe('event');
    await nameHeader.click();
    const firstRowAfterSortAgain = table.locator('tbody tr').first();
    const firstRowNameAfterSortAgain = await firstRowAfterSortAgain
        .locator('td')
        .nth(1)
        .textContent();
    await expect(firstRowNameAfterSortAgain).toBe('event');

    //test Node Count
    const nodeCountHeader = table.getByRole('columnheader', {
        name: 'Node Count',
    });
    await nodeCountHeader.click();
    const firstRowNodeCount = await firstRow.locator('td').nth(2).textContent();
    await expect(firstRowNodeCount).toBe('501');
    await nodeCountHeader.click();
    const firstRowNodeCountAfterSort = await firstRowAfterSort
        .locator('td')
        .nth(2)
        .textContent();
    await expect(firstRowNodeCountAfterSort).toBe('5');

    //test Edge Count
    const edgeCountHeader = table.getByRole('columnheader', {
        name: 'Edge Count',
    });
    await edgeCountHeader.click();
    const firstRowEdgeCount = await firstRow.locator('td').nth(3).textContent();
    await expect(firstRowEdgeCount).toBe('500');
    await edgeCountHeader.click();
    const firstRowEdgeCountAfterSort = await firstRowAfterSort
        .locator('td')
        .nth(3)
        .textContent();
    await expect(firstRowEdgeCountAfterSort).toBe('4');
});

test('Open graph by double clicking row', async ({ page }) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.waitForLoadState('networkidle');
    const firstRow = table.locator('tbody tr').first();
    await firstRow.dblclick();
    await expect(page).toHaveURL(
        /\/graph\?graphSource=vanilla%2Fevent&initialNodes=%5B%5D/,
    );
});

test('Open graph by clicking open button on rhs panel', async ({ page }) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await table.locator('tbody tr').first().click();
    await page.getByText('vanilla/event').isVisible();
    await page.getByText('Namespace').isVisible();
    await page
        .getByLabel('Button group with a nested')
        .getByRole('button', { name: 'Open' })
        .click();

    await page.waitForSelector('text=Overview');
    await page.getByText('Overview').isVisible();
    await expect(page).toHaveURL(
        /\/graph\?graphSource=vanilla%2Fevent&initialNodes=%5B%5D/,
    );
});

test('Open and close accordions on right hand side panel and open graph from minimap preview', async ({
    page,
}) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    const firstRow = table.locator('tbody tr').first();
    await firstRow.click();
    await page.getByRole('button', { name: 'Properties' }).click();
    await expect(page.getByText('Namespace')).not.toBeVisible();
    await page.getByRole('button', { name: 'Properties' }).click();
    await expect(page.getByText('Namespace')).toBeVisible();
    await page.getByRole('button', { name: 'Open' }).nth(1).click();
    await expect(page).toHaveURL(
        /\/graph\?graphSource=vanilla%2Fevent&initialNodes=%5B%5D/,
    );
});

test('Search saved graphs table, clear search and hide search', async ({
    page,
}) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.getByRole('button', { name: 'Show/Hide search' }).click();
    const searchInput = page.getByPlaceholder('Search saved graphs');
    await searchInput.fill('event');
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(1);
    const firstRowName = await rows.first().locator('td').nth(1).textContent();
    await expect(firstRowName).toBe('event');
    await page.getByRole('button', { name: 'Clear search' }).click();
    await page.getByRole('button', { name: 'Show/Hide search' }).click();
    await expect(searchInput).toBeHidden();
});

test('Filter by Columns', async ({ page }) => {
    await page.goto('/saved-graphs');
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.waitForLoadState('networkidle');
    await page.getByRole('button', { name: 'Show/Hide filters' }).click();
    const filterNameInput = page.getByPlaceholder('Filter by Name');
    await filterNameInput.fill('event');
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(1);
    const firstRowName = await rows.first().locator('td').nth(1).textContent();
    await expect(firstRowName).toBe('event');
    await page.getByRole('button', { name: 'Clear filter' }).nth(1).click();
    const filterNodeCountInput = page.getByPlaceholder('Filter by Node Count');
    await filterNodeCountInput.fill('501');
    await expect(rows).toHaveCount(1);
    await page.getByRole('button', { name: 'Clear filter' }).nth(2).click();
    const filterEdgeCountInput = page.getByPlaceholder('Filter by Edge Count');
    await filterEdgeCountInput.fill('4');
    await expect(rows).toHaveCount(2);
});
