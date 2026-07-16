import { expect, test } from '@playwright/test';
import {
    clickSavedGraphsGraph,
    navigateInSavedGraphs,
    OPEN_SAVED_GRAPH_METHODS,
} from './saved-graphs.utils';
import { waitForLayoutToFinish } from './utils';

const PAGE_SIZE = 8;

test('Saved graphs table is visible', async ({ page }) => {
    await navigateInSavedGraphs(page, { namespace: 'vanilla' });
    await clickSavedGraphsGraph(page, 'event');
    await page.getByRole('button', { name: 'event GRAPH' }).click();
    await expect(
        page.getByRole('heading', { name: 'vanilla/event', exact: true }),
    ).toBeVisible();
    await expect(page.getByText('PREVIEW')).toBeVisible();
    await expect(page.getByText('METADATA')).toBeVisible();
});

test(`Card view has N cards per page`, async ({ page }) => {
    await navigateInSavedGraphs(page, { namespace: 'vanilla' });
    await expect(page.getByText(`1-${PAGE_SIZE} of`)).toBeVisible();
    await expect(page.getByRole('button', { name: 'GRAPH' })).toHaveCount(
        PAGE_SIZE,
    );
});

test('Page index is preserved in URL and survives reload', async ({ page }) => {
    await navigateInSavedGraphs(page, { namespace: 'vanilla' });
    await expect(page.getByText(`1-${PAGE_SIZE} of`)).toBeVisible();

    await page.getByRole('button', { name: 'Next page', exact: true }).click();
    const countOnPage2 = page.getByText(`${PAGE_SIZE + 1}-`);
    await expect(countOnPage2).toBeVisible();
    await expect(page).toHaveURL(/[?&]page=1\b/);

    await page.reload();
    await page.waitForLoadState('networkidle');
    await expect(page).toHaveURL(/[?&]page=1\b/);
    await expect(countOnPage2).toBeVisible();
});

test('Row sorting on saved graphs table by columns', async ({ page }) => {
    await navigateInSavedGraphs(
        page,
        { namespace: 'vanilla' },
        { view: 'table' },
    );
    const table = await page.getByRole('table');

    // Name
    await page.getByRole('button', { name: 'Sort by Name ascending' }).click();
    const firstRowAscending = table.locator('tbody tr').first();
    const firstRowAscendingName = await firstRowAscending
        .locator('td')
        .nth(1)
        .textContent();
    await expect(firstRowAscendingName).toBe('event');
    await page
        .getByRole('button', { name: 'Sorted by Name ascending' })
        .click();
    const firstRowDescending = table.locator('tbody tr').first();
    const firstRowDescendingName = await firstRowDescending
        .locator('td')
        .nth(1)
        .textContent();
    await expect(firstRowDescendingName).toBe('variant_test');
    await page
        .getByRole('button', { name: 'Sorted by Name descending' })
        .click();
    const firstRowBackToNormal = table.locator('tbody tr').first();
    const firstRowBackToNormalName = await firstRowBackToNormal
        .locator('td')
        .nth(1)
        .textContent();
    await expect(firstRowBackToNormalName).toBe('event');

    // Node Count
    await page
        .getByRole('button', { name: 'Sort by Node Count descending' })
        .click();
    const firstRowNodeCountDescending = table.locator('tbody tr').first();
    const firstRowNodeCountDescendingName = await firstRowNodeCountDescending
        .locator('td')
        .nth(2)
        .textContent();
    await expect(firstRowNodeCountDescendingName).toBe('501');
    await page
        .getByRole('button', { name: 'Sorted by Node Count descending' })
        .click();
    const firstRowNodeCountAscending = table.locator('tbody tr').first();
    const firstRowNodeCountAscendingName = await firstRowNodeCountAscending
        .locator('td')
        .nth(2)
        .textContent();
    await expect(firstRowNodeCountAscendingName).toBe('2');

    // Edge Count
    await page
        .getByRole('button', { name: 'Sort by Edge Count descending' })
        .click();
    const firstRowEdgeCountAscending = table.locator('tbody tr').first();
    const firstRowEdgeCountAscendingName = await firstRowEdgeCountAscending
        .locator('td')
        .nth(3)
        .textContent();
    await expect(firstRowEdgeCountAscendingName).toBe('500');
    await page
        .getByRole('button', { name: 'Sorted by Edge Count descending' })
        .click();
    const firstRowEdgeCountDescending = table.locator('tbody tr').first();
    const firstRowEdgeCountDescendingName = await firstRowEdgeCountDescending
        .locator('td')
        .nth(3)
        .textContent();
    await expect(firstRowEdgeCountDescendingName).toBe('0');
});

test('Open graph by all available methods', async ({ page }) => {
    for (const method of OPEN_SAVED_GRAPH_METHODS) {
        await navigateInSavedGraphs(
            page,
            { namespace: 'vanilla', graphName: 'temporal_props' },
            {
                method,
            },
        );
        await expect(page).toHaveURL(/\/graph\/vanilla\/temporal_props/);
    }
});

test('Search saved graphs table, clear search and hide search', async ({
    page,
}) => {
    await navigateInSavedGraphs(
        page,
        { namespace: 'vanilla' },
        { view: 'table' },
    );
    const table = await page.getByRole('table');
    await page.getByRole('button', { name: 'Show/Hide search' }).click();
    const searchInput = page.getByRole('textbox', {
        name: 'Search explorations',
    });
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
    await navigateInSavedGraphs(
        page,
        { namespace: 'vanilla' },
        { view: 'table' },
    );
    const table = await page.getByRole('table');
    await page.getByRole('button', { name: 'Show/Hide filters' }).click();
    const filterNameInput = page.getByPlaceholder('Filter by Name');
    await filterNameInput.fill('event');
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(1);
    const firstRowName = await rows.first().locator('td').nth(1).textContent();
    await expect(firstRowName).toBe('event');
    await page
        .locator('button[aria-label="Clear filter"]:not(:disabled)')
        .click();
    const filterNodeCountInput = page.getByPlaceholder('Filter by Node Count');
    await filterNodeCountInput.fill('501');
    await expect(rows).toHaveCount(1);
    await page
        .locator('button[aria-label="Clear filter"]:not(:disabled)')
        .click();
    const filterEdgeCountInput = page.getByPlaceholder('Filter by Edge Count');
    await filterEdgeCountInput.fill('4');
    await expect(rows).toHaveCount(2);
});

test('Switching between previews', async ({ page }) => {
    await navigateInSavedGraphs(page, { namespace: 'vanilla' });
    await page.getByRole('button', { name: 'Expand details' }).click();
    await page.waitForTimeout(500);
    await clickSavedGraphsGraph(page, 'event');
    await waitForLayoutToFinish(page);
    expect(
        await page
            .getByRole('region', { name: 'Graph preview' })
            .getByRole('link', { name: 'Open' })
            .screenshot(),
    ).toMatchSnapshot('event-preview-first-click.png');
    await clickSavedGraphsGraph(page, 'persistent');
    await waitForLayoutToFinish(page);
    expect(
        await page
            .getByRole('region', { name: 'Graph preview' })
            .getByRole('link', { name: 'Open' })
            .screenshot(),
    ).toMatchSnapshot('persistent-preview-first-click.png');
    await clickSavedGraphsGraph(page, 'event');
    await waitForLayoutToFinish(page);
    // We expect no difference between the first time we preview and the second time
    expect(
        await page
            .getByRole('region', { name: 'Graph preview' })
            .getByRole('link', { name: 'Open' })
            .screenshot(),
    ).toMatchSnapshot('event-preview-first-click.png');
});
