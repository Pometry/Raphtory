import { expect, Page, test } from '@playwright/test';
import {
    fillInCondition,
    searchForEntity,
    waitForLayoutToFinish,
} from './utils';

async function searchAndPinNodes(page: Page, amount: number) {
    if (amount <= 0) {
        return;
    }

    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    for (let i = 0; i < amount; i++) {
        await rows.nth(i).click({ button: 'right' });
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
    await expect(pinnedRows).toHaveCount(Math.min(5, amount));
}

test('Search for a graph in the query builder, navigate direct connections table, check activity log and navigate to graph page', async ({
    page,
}) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await expect(rows).toHaveCount(3);
    await rows.first().click();
    const selectedTab = page.getByRole('tab', { name: 'Selected' });
    await expect(selectedTab).toHaveAttribute('aria-selected', 'true');
    await rows.nth(2).click();
    await page.locator('button[aria-label="next page"]').click();
    await expect(page.getByText('Page 2 of 2')).toBeVisible();
    await expect(page.getByRole('table').nth(2)).toBeVisible();
    await expect(
        page.getByRole('table').nth(2).locator('tbody tr'),
    ).toHaveCount(1);
    const activityLogTab = page.getByRole('tab', { name: 'Activity Log' });
    await expect(activityLogTab).toBeVisible();
    await activityLogTab.click();
    await expect(page.getByText('Event')).toBeVisible();
    await expect(page.getByText('Timestamp')).toBeVisible();
    await rows.first().dblclick();
    await expect(page).toHaveURL(
        /\/graph\?initialNodes=%5B%22Pedro%22%5D&baseGraph=vanilla%2Fevent/,
    );
});

test('Clear search results in query builder', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    await page
        .getByRole('button', { name: 'Clear', exact: true })
        .first()
        .click();
    await page
        .getByRole('button', {
            name: 'Select a graph',
        })
        .isVisible();
    await expect(
        page.getByRole('combobox', { name: 'Select type' }),
    ).toHaveValue('');
});

test('Open node from right hand side panel open button', async ({ page }) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await rows.first().click();
    const openNodeButton = page.getByRole('button', {
        name: 'Open',
        exact: true,
    });
    await expect(openNodeButton).toBeVisible();
    await openNodeButton.click();
    await expect(page).toHaveURL(
        '/graph?baseGraph=vanilla%2Fevent&initialNodes=%5B%22Pedro%22%5D',
    );
});

test('Pin and unpin a node with right hand side menu on search cards', async ({
    page,
}) => {
    await searchAndPinNodes(page, 1);
    const pinnedRows = page.getByRole('table').locator('tbody tr');
    await pinnedRows.first().click({ button: 'right' });
    await page
        .getByRole('menuitem', {
            name: 'Unpin',
        })
        .click();
    await expect(pinnedRows).toHaveCount(0);
});

test('Unpin all nodes from pinned tab', async ({ page }) => {
    await searchAndPinNodes(page, 2);
    const unpinAllButton = page.getByRole('button', {
        name: 'Unpin all items',
        exact: true,
    });
    await expect(unpinAllButton).toBeVisible();
    await unpinAllButton.click();
    const pinnedRows = page.getByRole('table').locator('tbody tr');
    await expect(pinnedRows).toHaveCount(0);
});

test('View information in right hand side panel and open in graph view button in right click menu', async ({
    page,
}) => {
    await searchForEntity(page, { type: 'node', nodeType: 'Person' });
    const table = page.getByRole('table');
    await expect(table).toBeVisible();
    const rows = table.locator('tbody tr');
    await rows.first().click({ button: 'right' });
    await page
        .getByRole('menuitem', {
            name: 'View information',
        })
        .click();
    await page.getByRole('button', { name: 'Explorer' }).isVisible();
    await page.getByRole('button', { name: 'Explorer' }).click();
    await rows.first().click({ button: 'right' });
    const openInGraphButton = page.getByRole('menuitem', {
        name: 'Open in graph view',
        exact: true,
    });
    await expect(openInGraphButton).toBeVisible();
    await openInGraphButton.click();
    await expect(page).toHaveURL(
        '/graph?initialNodes=%5B%22Pedro%22%5D&baseGraph=vanilla%2Fevent',
    );
});

test('Open all items to new graph button on pinned tab', async ({ page }) => {
    await searchAndPinNodes(page, 1);
    const attachAllButton = page.getByRole('button', {
        name: 'Open all items in a new graph',
        exact: true,
    });
    await expect(attachAllButton).toBeVisible();
    await attachAllButton.click();
    await expect(page).toHaveURL(
        '/graph?initialNodes=%5B%22Pedro%22%5D&baseGraph=vanilla%2Fevent',
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
    await page.getByRole('button', { name: 'Pedro Age' }).click();
    await page.getByRole('button', { name: 'select merge strategy' }).click();
    await page.getByRole('menuitem', { name: 'Unpin Node' }).click();
    await expect(page.getByText('No pinned nodes.')).toBeVisible();
});

test('Search for relationships in query builder', async ({ page }) => {
    await searchForEntity(page, {
        type: 'edge',
        src: 'Ben',
        dst: 'Pedro',
        layers: ['meets', 'founds', 'transfers'],
    });
    await page
        .getByRole('button', { name: 'Ben - Pedro Layers: meets' })
        .click();
    await page.getByRole('button', { name: 'Edge Statistics' }).click();
    await page.getByRole('button', { name: 'Open', exact: true }).click();
    await page.getByRole('link', { name: 'Ben' }).click();
    await page.waitForSelector('text=Overview');
    await expect(page).toHaveURL(
        '/graph?baseGraph=vanilla%2Fevent&initialNodes=%5B%22Ben%22%5D',
    );
});

test('Search for relationships in certain date range', async ({ page }) => {
    await page.goto('/search');
    await page
        .getByRole('button', {
            name: 'Select a graph',
        })
        .click();
    await page
        .getByRole('row', { name: /^vanilla$/ })
        .waitFor({ state: 'visible' });
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page
        .getByRole('row', { name: /^event$/ })
        .waitFor({ state: 'visible' });
    await page.getByRole('row', { name: /^event$/ }).click();
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
    for (let i = 0; i < 10; i++) {
        await page.getByRole('button', { name: 'Next month' }).click();
    }
    await page
        .getByRole('row', { name: '1 2 3 4 5', exact: true })
        .locator('button')
        .first()
        .click();
    await page.getByRole('combobox').filter({ hasText: 'Entity' }).click();
    await page.getByRole('option', { name: 'Relationship' }).click();
    await page.getByRole('textbox', { name: 'Source ID' }).click();
    await page.getByRole('textbox', { name: 'Source ID' }).fill('Hamza');
    await page.getByRole('textbox', { name: 'Destination ID' }).click();
    await page.getByRole('textbox', { name: 'Destination ID' }).fill('Pedro');
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await page.getByRole('button', { name: 'Hamza - Pedro Layers:' }).click();
    await page.getByRole('button', { name: 'Edge Statistics' }).click();
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
        conditions: [{ name: 'ID', value: 'Pom' }],
    });
    await page.getByText('Nothing turned up!').isVisible();
    await page
        .locator(
            '.MuiButtonBase-root.MuiIconButton-root.MuiIconButton-sizeMedium.css-g6vrc0-MuiButtonBase-root-MuiIconButton-root',
        )
        .click();
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);
});

test('is, is not condition statements in query builder', async ({ page }) => {
    test.setTimeout(60000);
    await searchForEntity(page, {
        type: 'node',
        nodeType: 'Person',
        conditions: [{ name: 'ID', value: 'Pedro' }],
    });
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table')).toBeVisible();
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);

    await fillInCondition(page, {
        op: { current: 'Is', new: 'Is Not' },
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
            { name: 'ID', op: { current: 'Is', new: 'Includes' }, value: 'Pe' },
        ],
    });
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(1);

    await fillInCondition(page, {
        op: { current: 'Includes', new: 'Excludes' },
        value: 'Pe',
    });
    await page.getByRole('button', { name: 'Search', exact: true }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('table').locator('tbody tr')).toHaveCount(2);
});
