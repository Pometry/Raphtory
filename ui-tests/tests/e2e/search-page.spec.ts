import { expect, Page, test } from '@playwright/test';
import {
    fillInCondition,
    searchForEntity,
    selectGraphInQueryBuilder,
} from './search.utils';
import { waitForLayoutToFinish } from './utils';

// Caller passes node names rather than a count because the search result row
// order from raphtory is non-deterministic — selecting by name keeps tests
// stable.
async function searchAndPinNodes(page: Page, names: string[]) {
    if (names.length === 0) {
        return;
    }

    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    // Scope to the first table — once a row is clicked, a second table (the
    // direct-connections list) appears, and `getByRole('table')` would then
    // match both, making `rows.filter({ hasText: name })` ambiguous.
    const table = page.getByRole('table').first();
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    for (const name of names) {
        await rows.filter({ hasText: name }).click({ button: 'right' });
        await page
            .getByRole('menuitem', {
                name: 'Add to Pinned',
            })
            .click();
    }

    const pinnedTab = page.getByRole('tab', { name: 'Pinned' });
    await expect(pinnedTab).toBeVisible();
    await pinnedTab.click();

    await expect(
        page.getByRole('button', { name: 'Unpin all items' }),
    ).toBeVisible();
    await expect(
        page.getByRole('button', { name: 'Open all items in a new graph' }),
    ).toBeVisible();
    const pinnedRows = page.getByRole('table').locator('tbody tr');
    await expect(pinnedRows).toHaveCount(Math.min(8, names.length));
}

test('Search for a graph in the query builder, navigate direct connections table, check activity log and navigate to graph page', async ({
    page,
}) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    // Scope rows to the first table — clicking a row makes the
    // direct-connections table appear, and we must not match its rows.
    const table = page.getByRole('table').first();
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(3);
    // Row order from raphtory is non-deterministic, so select by name. Pedro
    // verifies node selection; Hamza has enough connections (7) to span 2
    // pages in the direct-connections table.
    const pedroRow = rows.filter({ hasText: 'Pedro' });
    const hamzaRow = rows.filter({ hasText: 'Hamza' });
    await pedroRow.click();
    const selectedTab = page.getByRole('tab', { name: 'Selected' });
    await expect(selectedTab).toHaveAttribute('aria-selected', 'true');
    await hamzaRow.click();
    await page.getByRole('button', { name: 'next page', exact: true }).click();
    await expect(page.getByText('Page 2 of 2')).toBeVisible();
    await expect(page.getByRole('table').nth(1)).toBeVisible();
    await expect(
        page.getByRole('table').nth(1).locator('tbody tr'),
    ).toHaveCount(1);
    const traceLogTab = page.getByRole('tab', { name: 'Trace Log' });
    await expect(traceLogTab).toBeVisible();
    await traceLogTab.click();
    await expect(page.getByText('Event')).toBeVisible();
    await expect(page.getByText('Timestamp')).toBeVisible();
    await pedroRow.dblclick();
    await expect(page).toHaveURL(
        /\/graph\/vanilla\/event\?initialNodes=%5B%22Pedro%22%5D/,
    );
});

test('Clear search results in query builder', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    await page.getByRole('button', { name: 'Clear all', exact: true }).click();
    await expect(page.getByText('Start Your Search')).toBeVisible();
    await expect(page.getByPlaceholder('Select type')).toBeVisible();
});

test('Open node from right hand side panel open button', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table').first();
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await rows.filter({ hasText: 'Pedro' }).click();
    const openNodeButton = page.getByRole('button', {
        name: 'Open',
        exact: true,
    });
    await expect(openNodeButton).toBeVisible();
    await openNodeButton.click();
    await expect(page).toHaveURL(
        '/graph/vanilla/event?initialNodes=%5B%22Pedro%22%5D',
    );
});

test('Pin and unpin a node with right hand side menu on search cards', async ({
    page,
}) => {
    await searchAndPinNodes(page, ['Pedro']);
    const pinnedRows = page.getByRole('table').locator('tbody tr');
    await pinnedRows.first().click({ button: 'right' });
    await page
        .getByRole('menuitem', {
            name: 'Unpin',
        })
        .click();
    await expect(page.getByRole('tab', { name: 'Pinned' })).toBeHidden();
});

test('Unpin all nodes from pinned tab', async ({ page }) => {
    await searchAndPinNodes(page, ['Pedro', 'Hamza']);
    await expect(page.getByRole('tab', { name: 'Pinned' })).toBeVisible();
    const unpinAllButton = page.getByRole('button', {
        name: 'Unpin all items',
        exact: true,
    });
    await expect(unpinAllButton).toBeVisible();
    await unpinAllButton.click();
    await expect(page.getByRole('tab', { name: 'Pinned' })).toBeHidden();
    await expect(
        page.getByRole('button', {
            name: 'Unpin all items',
            exact: true,
        }),
    ).toBeHidden();
});

test('View information in right hand side panel and open in graph view button in right click menu', async ({
    page,
}) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table').first();
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    const pedroRow = rows.filter({ hasText: 'Pedro' });
    await pedroRow.click({ button: 'right' });
    await page
        .getByRole('menuitem', {
            name: 'View information',
        })
        .click();
    await page.getByText('NETWORK').isVisible();
    await pedroRow.click({ button: 'right' });
    const openInGraphButton = page.getByRole('menuitem', {
        name: 'Open in graph view',
        exact: true,
    });
    await expect(openInGraphButton).toBeVisible();
    await openInGraphButton.click();
    await expect(page).toHaveURL(
        '/graph/vanilla/event?initialNodes=%5B%22Pedro%22%5D',
    );
});

test('Open all items to new graph button on pinned tab', async ({ page }) => {
    await searchAndPinNodes(page, ['Pedro']);
    const attachAllButton = page.getByRole('button', {
        name: 'Open all items in a new graph',
        exact: true,
    });
    await expect(attachAllButton).toBeVisible();
    await attachAllButton.click();
    await expect(page).toHaveURL(
        '/graph/vanilla/event?initialNodes=%5B%22Pedro%22%5D',
    );
});

test('pin and unpin via right hand side menu button', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await rows.first().click();
    await page.getByRole('button', { name: 'select merge strategy' }).click();
    await page
        .getByRole('menuitem', { name: 'Pin Node' })
        .locator('div')
        .first()
        .click();
    await page.getByRole('tab', { name: 'Pinned' }).click();
    await rows.first().click();
    await page.getByRole('button', { name: 'select merge strategy' }).click();
    await page.getByRole('menuitem', { name: 'Unpin Node' }).click();
    await expect(page.getByRole('tab', { name: 'Pinned' })).toBeHidden();
});

test('Search for relationships in query builder', async ({ page }) => {
    await searchForEntity(page, {
        type: 'edge',
        src: 'Ben',
        dst: 'Pedro',
        layers: ['meets', 'founds', 'transfers'],
    });
    await page.getByRole('button', { name: 'Ben - Pedro Edge meets' }).click();
    await page.getByRole('button', { name: 'EDGE STATISTICS' }).click();
    await page.waitForTimeout(100);
    await expect(page.getByText('Madrid', { exact: true })).toBeVisible();
    await expect(page.getByText('Layer Names')).toBeVisible();
    await expect(page.getByText('meets', { exact: true }).last()).toBeVisible();
    await page.getByRole('button', { name: 'Open', exact: true }).click();
    await page.getByRole('link', { name: 'Ben' }).click();
    await page.waitForSelector('text=Overview');
    await expect(page).toHaveURL(
        '/graph/vanilla/event?initialNodes=%5B%22Ben%22%5D',
    );
});

test('Search for relationships in certain date range', async ({ page }) => {
    await page.goto('/search');
    await selectGraphInQueryBuilder(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await page
        .getByRole('button', {
            name: 'Confirm',
        })
        .click();
    await page
        .getByRole('button', {
            name: 'Confirm',
        })
        .waitFor({ state: 'hidden' });
    await page
        .getByRole('button', {
            name: 'Choose date, selected date is 1 Jan',
        })
        .or(
            page.getByRole('textbox', {
                name: 'Choose date, selected date is 1 Jan',
            }),
        )
        .click();
    await page
        .getByRole('button', { name: 'calendar view is open, switch' })
        .click();
    await page.getByRole('radio', { name: '2023' }).click();
    for (let i = 0; i < 10; i++) {
        await page.getByRole('button', { name: 'Next month' }).click();
    }
    await page
        .getByRole('row', { name: '1 2 3 4 5', exact: true })
        .locator('button')
        .first()
        .click();
    // Handle the narrower version of the date picker (which appears in tests
    // sometimes for unknown reasons)
    const okButtonVisible = await page
        .getByRole('button', { name: 'OK' })
        .isVisible();
    if (okButtonVisible) {
        await page.getByRole('button', { name: 'OK' }).click();
    }
    await page.getByRole('combobox').filter({ hasText: 'Entity' }).click();
    await page.getByRole('option', { name: 'Relationship' }).click();
    await page.getByRole('textbox', { name: 'Source ID' }).click();
    await page.getByRole('textbox', { name: 'Source ID' }).fill('Hamza');
    await page.getByRole('textbox', { name: 'Destination ID' }).click();
    await page.getByRole('textbox', { name: 'Destination ID' }).fill('Pedro');
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await page.waitForLoadState('networkidle');
    await page.getByRole('button', { name: 'Hamza - Pedro' }).click();
    await page.getByRole('button', { name: 'EDGE STATISTICS' }).click();
    await expect(page.getByText('meets, transfers')).toBeVisible();
});

test('Search for conditions in query builder', async ({ page }) => {
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Person',
        conditions: [{ name: 'age', value: '28' }],
    });

    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(1);
});

test('Delete condition in query builder', async ({ page }) => {
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Company',
        conditions: [
            {
                name: 'ID',
                value: 'Pom',
                op: { current: 'Is', new: 'Contains' },
            },
        ],
    });
    await page.getByText('Nothing turned up!').isVisible();
    await page.getByLabel('Remove condition').click();
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);
});

test('is, is not condition statements in query builder', async ({ page }) => {
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Person',
        conditions: [{ name: 'ID', value: 'Pedro' }],
    });
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);

    await fillInCondition(page, {
        op: { current: 'Is', new: 'Not' },
        value: 'Pedro',
    });
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(2);
});

test('includes, excludes condition statements in query builder', async ({
    page,
}) => {
    test.setTimeout(60000);
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Person',
        conditions: [
            { name: 'ID', op: { current: 'Is', new: 'Contains' }, value: 'Pe' },
        ],
    });
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);

    await fillInCondition(page, {
        op: { current: 'Contains', new: 'Excludes' },
        value: 'Pe',
    });
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(2);
});

test('Multiple condition rows can be added independently', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });

    await fillInCondition(page, { name: 'age', value: '28' });
    await fillInCondition(page, { name: 'ID', value: 'Pedro', blur: true });
    await expect(page.getByPlaceholder('Value')).toHaveCount(2);

    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await expect(page.getByText('Start Your Search')).toBeHidden();

    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);
});

test('Condition value shows dropdown for few variants and text field for many', async ({
    page,
}) => {
    test.setTimeout(60000);
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Person',
        graph: 'variant_test',
    });
    await fillInCondition(page, { name: 'status' });
    await page.getByRole('combobox').nth(3).click();
    await page.getByRole('option', { name: 'active', exact: true }).click();
    await page.getByRole('combobox').nth(3).click();
    await page.getByRole('option', { name: 'inactive', exact: true }).click();
    await page.getByRole('button', { name: 'Remove condition' }).click();
    await fillInCondition(page, { name: 'code', value: 'A1' });
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await expect(page.getByText('Start Your Search')).toBeHidden();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);
});
