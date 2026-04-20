import { expect, Page, test } from '@playwright/test';
import {
    navigateToGraphPageBySearch,
    navigateToSavedGraphBySavedGraphsTable,
    selectLayout,
    waitForLayoutToFinish,
} from './utils';

async function fitView(page: Page) {
    await page
        .getByRole('button', { name: 'Fit all nodes within visible region' })
        .click();
    await waitForLayoutToFinish(page);
}

test('Close right hand side panel button and open again', async ({ page }) => {
    await page.goto('/graph?graphSource=vanilla%2Fevent&initialNodes=%5B%5D');

    await page
        .locator(
            '.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary',
        )
        .nth(5)
        .click();
    await (await page.waitForSelector('text="Pedro"')).isHidden();
    await page.getByText('Overview').isHidden();

    await page
        .locator(
            '.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary',
        )
        .nth(5)
        .click();

    await page.getByText('Overview').isVisible();
});

test('Click save as button opens save as dialog', async ({ page }) => {
    await page.goto('/graph?graphSource=vanilla%2Fevent&initialNodes=%5B%5D');
    await page.locator('button:has-text("Save As")').click();
    await page.getByRole('button', { name: 'Cancel' }).waitFor();
    await expect(page.getByText('New Graph Name')).toBeVisible();

    await page.getByRole('button', { name: 'Cancel' }).click();
    await expect(page.getByText('New Graph Name')).toBeHidden();
});

test('Highlight founds then transfers', async ({ page }) => {
    await page.goto('/graph?graphSource=vanilla%2Fevent&initialNodes=%5B%5D');
    await expect(page.getByRole('progressbar')).toBeHidden();
    await page.getByRole('button', { name: 'Relationships' }).waitFor();
    await page
        .getByRole('row', { name: 'transfers' })
        .getByRole('button')
        .click();
    await expect(
        page.locator('g', { hasText: /^Ben$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(117, 135, 72)');
    await expect(
        page.locator('g', { hasText: /^Hamza$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(117, 135, 72)');
    await expect(
        page.locator('g', { hasText: /^Pedro$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(117, 135, 72)');
    await page.getByRole('row', { name: 'founds' }).getByRole('button').click();
    await expect(
        page.locator('g', { hasText: /^Ben$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(117, 135, 72)');
    await expect(
        page.locator('g', { hasText: /^Hamza$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(117, 135, 72)');
    await expect(
        page.locator('g', { hasText: /^Pometry$/ }).locator('circle'),
    ).toHaveCSS('fill', 'rgb(93, 212, 223)');
});

test('Test layouts', async ({ page }) => {
    test.setTimeout(60000);
    navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');
    // The extra timeout here helps to make the next line more consistent
    await waitForLayoutToFinish(page, 3000);
    await selectLayout(page, 'Concentric Layout');
    expect(await page.screenshot()).toMatchSnapshot('concentric-layout.png');
    await selectLayout(page, 'Force Based Layout');

    expect(await page.screenshot()).toMatchSnapshot('force-based-layout.png');
    await selectLayout(page, 'Hierarchical TD Layout');

    expect(await page.screenshot()).toMatchSnapshot(
        'hierarchical-td-layout.png',
    );
    await selectLayout(page, 'Hierarchical LR Layout');
    expect(await page.screenshot()).toMatchSnapshot(
        'hierarchical-lr-layout.png',
    );
    await selectLayout(page, 'Default Layout');
    expect(await page.screenshot()).toMatchSnapshot('default-layout.png');
});

test('Zoom in, zoom out, fit view button', async ({ page }) => {
    navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');
    await page.getByRole('button', { name: 'Zoom in' }).click();
    await waitForLayoutToFinish(page);
    expect(await page.screenshot()).toMatchSnapshot('zoomedin.png');
    await page.getByRole('button', { name: 'Zoom out' }).click();
    await waitForLayoutToFinish(page);
    expect(await page.screenshot()).toMatchSnapshot('zoomedout.png');
    await fitView(page);
    expect(await page.screenshot()).toMatchSnapshot('fitview.png');
});

test('Click on Pometry node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pometry',
        nodeType: 'Company',
    });
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Pometry' })).toBeVisible();
    await expect(
        page.getByText('No properties found', { exact: true }),
    ).toBeVisible();
});

test('Click on Pedro node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Pedro' })).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
});

test('Click on Hamza node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Hamza',
        nodeType: 'Person',
    });
    await waitForLayoutToFinish(page);
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Hamza' })).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
});

test('Click on Ben node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Ben',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Ben' })).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
});

test('Double click expand node and delete by floating actions button', async ({
    page,
}) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 178,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Pedro' })).toBeVisible();
    await page
        .getByRole('button', {
            name: 'Delete selected node from current graph',
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page);
    expect(await page.screenshot()).toMatchSnapshot('deletednode.png', {
        maxDiffPixels: 100,
        maxDiffPixelRatio: 0.01,
    });
    await page.getByRole('tab', { name: 'Overview' }).click();
    await page.getByRole('button', { name: 'Undo', exact: true }).click();

    await waitForLayoutToFinish(page);
    await page
        .locator('canvas')
        .nth(1)
        .dblclick({
            position: {
                x: 350,
                y: 175,
            },
        });

    await page.waitForSelector('text=Pedro');
    await expect(page.getByText('Ben')).toBeVisible();
    await expect(page.getByText('Hamza')).toBeVisible();
});

test('Expand node by floating actions button', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page
        .getByRole('button', {
            name: 'Explore',
            exact: true,
        })
        .click();
    await page
        .getByRole('menuitem', {
            name: 'Expand and show all nodes directly related to selected node',
            exact: true,
        })
        .click();
    await page.waitForSelector('text=Pedro');
    await expect(page.getByText('Ben')).toBeVisible();
    await expect(page.getByText('Hamza')).toBeVisible();
    await expect(page.getByText('Pedro')).toBeVisible();
});

test('Expand two-hop by floating actions button', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page
        .getByRole('button', {
            name: 'Explore',
            exact: true,
        })
        .click();
    await page
        .getByRole('menuitem', {
            name: 'Expand Two-Hop',
            exact: true,
        })
        .click();
    await page.waitForSelector('text=Pedro');
    await expect(page.getByText('Ben')).toBeVisible();
    await expect(page.getByText('Hamza')).toBeVisible();
    await expect(page.getByText('Pedro')).toBeVisible();
    await expect(page.getByText('Pometry')).toBeVisible();
});

test('Expand shared neighbours by floating actions button', async ({
    page,
}) => {
    await navigateToGraphPageBySearch(page, {
        type: 'edge',
        src: 'Hamza',
        dst: 'Pedro',
        layers: ['meets'],
    });

    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 395,
                y: 315,
            },
        });

    await page
        .locator('canvas')
        .nth(1)
        .click({
            modifiers: ['Shift'],
            position: {
                x: 324,
                y: 105,
            },
        });

    await page
        .getByRole('button', {
            name: 'Explore',
            exact: true,
        })
        .click();
    await page
        .getByRole('menuitem', {
            name: 'Shared Neighbours',
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page);
    await page.waitForSelector('text=Ben');
    await expect(page.getByText('Ben')).toBeVisible();
    await expect(page.getByText('Hamza')).toBeVisible();
    await expect(page.getByText('Pedro')).toBeVisible();
});

test('Click edge to reveal right hand side panel details', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    await page
        .locator('canvas')
        .nth(1)
        .dblclick({
            position: {
                x: 350,
                y: 175,
            },
        });
    await waitForLayoutToFinish(page, 2000);
    await fitView(page);
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 164,
            },
        });
    await page.waitForTimeout(100);
    await page.getByRole('tab', { name: 'Selected' }).click();
    await page.getByRole('button', { name: 'Edge Statistics' }).click();
    await expect(page.getByText('Madrid')).toBeVisible();
    await expect(page.getByText('Layer Names')).toBeVisible();
    await expect(page.getByText('Earliest Time')).toBeVisible();
    await expect(page.getByText('Latest Time')).toBeVisible();
    await expect(page.getByText('meets, transfers')).toBeVisible();
    await expect(page.getByText('Hamza -> Pedro')).toBeVisible();
    await expect(page.getByText('Amount')).toBeVisible();
    await page.getByRole('tab', { name: 'Pedro -> Hamza Log' }).click();
    await expect(page.getByText('Pedro -> transfers -> Hamza')).toBeVisible();
});

test('Undo and redo in floating actions menu', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .dblclick({
            position: {
                x: 350,
                y: 175,
            },
        });
    await waitForLayoutToFinish(page);
    await page.getByRole('button', { name: 'Undo', exact: true }).click();
    await page.getByRole('button', { name: 'Redo', exact: true }).click();
    await page.waitForSelector('text=Pedro');
    await expect(page.getByText('Ben')).toBeVisible();
    await expect(page.getByText('Hamza')).toBeVisible();
    await expect(page.getByText('Pedro')).toBeVisible();
});

test('Expand node, fit view and select all similar nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    const temporalView = await page
        .locator('.css-gnkdhv-MuiPaper-root')
        .boundingBox();
    await page
        .locator('canvas')
        .nth(1)
        .dblclick({
            position: {
                x: 350,
                y: 175,
            },
        });
    await waitForLayoutToFinish(page);
    await page.waitForSelector('text="Pedro"');
    await fitView(page);
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 430,
                y: 104,
            },
        });
    await page.getByRole('button', { name: 'Selection' }).click();
    await page
        .getByRole('menuitem', { name: 'Select all similar nodes' })
        .click();
    await waitForLayoutToFinish(page);
    if (temporalView) {
        expect(await page.screenshot({ clip: temporalView })).toMatchSnapshot(
            'selectsimilarnodes.png',
            {
                maxDiffPixels: 20000,
                maxDiffPixelRatio: 0.01,
            },
        );
    } else {
        throw new Error('Element not found or not visible');
    }
});

test('Click and deselect by floating actions', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 350,
                y: 175,
            },
        });
    await page.getByRole('tab', { name: 'Selected' }).click();

    await expect(page.getByText('Pedro').nth(0)).toBeVisible();
    await page.getByRole('button', { name: 'Selection' }).click();
    await page.getByRole('menuitem', { name: 'Deselect all nodes' }).click();
    await waitForLayoutToFinish(page);
    expect(await page.screenshot()).toMatchSnapshot('deselectnodes.png', {
        maxDiffPixels: 20000,
        maxDiffPixelRatio: 0.01,
    });
});

test('Click backspace to delete nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await page
        .locator('canvas')
        .nth(1)
        .dblclick({
            position: {
                x: 350,
                y: 175,
            },
        });
    await waitForLayoutToFinish(page);
    await page.waitForSelector('text="Pedro"');
    await fitView(page);
    await page
        .locator('canvas')
        .nth(1)
        .click({
            position: {
                x: 260,
                y: 235,
            },
        });
    await page.keyboard.press('Backspace');
    await expect(page.getByText('Hamza')).toBeHidden();
    await expect(page.getByText('Pedro')).toBeVisible();
    await expect(page.getByText('Ben')).toBeVisible();
});

// skipping because we have a lot of random errors in the console, need to fix that first
test.skip('catch any errors in the console', async ({ page }) => {
    const consoleErrors: string[] = [];

    page.on('console', (message) => {
        if (message.type() === 'error') {
            consoleErrors.push(message.text());
        }
    });

    await page.goto('/graph?graphSource=vanilla%2Fevent&initialNodes=%5B%5D');

    expect(consoleErrors, 'Console errors found').toEqual([]);
});

// skipping because it only works for one browser test, fails on other browser repeats (graph already exists after creating once in chromium)
test.skip('save new graph with save as dialog', async ({ page }) => {
    await page.goto('/graph?initialNodes=%5B"Pedro"%5D&baseGraph=event');

    await page.locator('button:has-text("Save As")').click();
    await expect(page.getByLabel('New Graph Name')).toBeVisible();

    await page.getByLabel('New Graph Name').fill('Test Graph');
    await page.getByRole('button', { name: 'Confirm' }).click();
    await page.waitForLoadState('networkidle');

    await page.waitForURL(
        /\/graph\?graphSource=Test(\+|%20)Graph&initialNodes=%5B%5D/,
    );

    await expect(page.locator('input')).toHaveValue('Test Graph');
});
