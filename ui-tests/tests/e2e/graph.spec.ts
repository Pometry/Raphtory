import { expect } from '@playwright/test';
import { test } from '../fixtures';
import {
    changeTab,
    clickOnEdge,
    clickOnNode,
    clickOnNodes,
    ctrlClickOnNode,
    doubleClickOnNode,
    dragSlider,
    fillColorPickerHexInput,
    fillInStyling,
    fitView,
    getGraphState,
    getNodePositions,
    getNodeScreenshotClip,
    navigateToGraphPageBySearch,
    navigateToSavedGraphBySavedGraphsTable,
    openTimeline,
    rightClickOnNode,
    selectLayout,
    waitForLayoutToFinish,
} from './utils';

test('Graph page title includes the graph name', async ({ page }) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');
    await expect(page).toHaveTitle('event | Pometry UI');
});

test('Document title updates when navigating between graphs', async ({
    page,
}) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');
    await expect(page).toHaveTitle('event | Pometry UI');

    // Navigate to a different graph
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await expect(page).toHaveTitle('persistent | Pometry UI');
});

test('Close right hand side panel button and open again', async ({ page }) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');

    await page.getByRole('button', { name: 'Collapse panel' }).click();
    await expect(
        page.getByRole('button', { name: 'Collapse panel' }),
    ).toBeHidden();

    await page.getByRole('button', { name: 'Expand Overview' }).click();

    await expect(
        page.getByRole('button', { name: 'Collapse panel' }),
    ).toBeVisible();
});

test('Click save as button opens save as dialog', async ({ page }) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');
    await page.getByRole('button', { name: 'Save graph as' }).click();
    await page.getByRole('button', { name: 'Cancel' }).waitFor();
    await expect(page.getByText('New Graph Name')).toBeVisible();

    await page.getByRole('button', { name: 'Cancel' }).click();
    await expect(page.getByText('New Graph Name')).toBeHidden();
});

test('Highlight founds then transfers', async ({ page }) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');
    await waitForLayoutToFinish(page);
    await page.getByText('Relationships').waitFor();

    await page
        .getByText('transfers3')
        .getByRole('button', { name: 'Highlight on graph' })
        .click();
    await waitForLayoutToFinish(page);
    // transfers edges: Hamza→Pedro, Pedro→Hamza, Ben→Hamza
    const transfersState = await getGraphState(page);
    expect(transfersState.highlighted.map((n) => n.id).sort()).toEqual([
        'Ben',
        'Hamza',
        'Pedro',
    ]);
    // Highlighting must not select the endpoint nodes
    expect(transfersState.selected).toEqual([]);

    await page
        .getByText('founds2')
        .getByRole('button', { name: 'Highlight on graph' })
        .click();
    await waitForLayoutToFinish(page);
    // founds edges: Ben→Pometry, Hamza→Pometry
    const foundsState = await getGraphState(page);
    expect(foundsState.highlighted.map((n) => n.id).sort()).toEqual([
        'Ben',
        'Hamza',
        'Pometry',
    ]);
    expect(foundsState.selected).toEqual([]);
});

test('Test layouts', async ({ page }) => {
    test.setTimeout(60000);
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');

    // The extra timeout here helps to make the next line more consistent
    await waitForLayoutToFinish(page, 3000, 3000);
    await selectLayout(page, 'Arrange nodes in concentric circles');
    expect(await page.screenshot()).toMatchSnapshot('concentric-layout.png');
    await selectLayout(page, 'Force-directed layout algorithm');

    expect(await page.screenshot()).toMatchSnapshot('force-based-layout.png');
    await selectLayout(page, 'Top-to-bottom hierarchical tree');

    expect(await page.screenshot()).toMatchSnapshot(
        'hierarchical-td-layout.png',
    );
    await selectLayout(page, 'Left-to-right hierarchical tree');
    expect(await page.screenshot()).toMatchSnapshot(
        'hierarchical-lr-layout.png',
    );
    await selectLayout(page, 'Physics-based layout with natural clustering');
    expect(await page.screenshot()).toMatchSnapshot('default-layout.png');

    // Re-run the same layout by clicking it again
    await selectLayout(page, 'Re-run layout');
    // Force layout is non-deterministic, so we verify re-run doesn't error
    // and produces a valid layout via snapshot
    expect(await page.screenshot()).toMatchSnapshot('force-layout-rerun.png');
});

test('Zoom in, zoom out, fit view button', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');

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
    await clickOnNode(page, 'Pometry');
    await changeTab(page, 'Selected');
    await expect(page.getByRole('heading', { name: 'Pometry' })).toBeVisible();
    await expect(page.getByText('PROPERTIES')).toBeHidden();
    await expect(page.getByText('STATISTICS')).toBeVisible();
});

test('Click on Pedro node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await clickOnNode(page, 'Pedro');
    await changeTab(page, 'Selected');
    await expect(page.getByRole('heading', { name: 'Pedro' })).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
});

test('Click on Hamza node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Hamza',
        nodeType: 'Person',
    });
    await clickOnNode(page, 'Hamza');
    await changeTab(page, 'Selected');
    await expect(page.getByRole('heading', { name: 'Hamza' })).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
});

test('Click on Ben node in graph', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Ben',
        nodeType: 'Person',
    });
    await clickOnNode(page, 'Ben');
    await changeTab(page, 'Selected');
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
    await clickOnNode(page, 'Pedro');
    await changeTab(page, 'Selected');
    await expect(page.getByRole('heading', { name: 'Pedro' })).toBeVisible();
    await page
        .getByRole('button', {
            name: 'Delete selected (⌫)',
        })
        .click();
    // Don't include the delete snapshot in tooltip (the ⌫ symbol can create
    // font problems on the pipeline)
    await page.mouse.move(0, 0);
    await waitForLayoutToFinish(page);
    await expect(page.getByRole('heading', { name: 'Pedro' })).toBeHidden();
    await changeTab(page, 'Overview');
    await page.getByRole('button', { name: 'Undo (⌘Z)', exact: true }).click();

    await waitForLayoutToFinish(page);
    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes.map((n) => n.id)).toEqual([
        'Pedro',
        'Ben',
        'Hamza',
    ]);
    // After expansion, Pedro's neighbours are all on graph — badge should be gone
    const expandedState = await getGraphState(page);
    expect(
        expandedState.nodes.find((n) => n.id === 'Pedro')?.badgeText,
    ).toBeUndefined();
});

test('Expand node by floating actions button', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    // Before expansion, Pedro has hidden neighbours — badge should show count
    const beforeState = await getGraphState(page);
    const pedroBefore = beforeState.nodes.find((n) => n.id === 'Pedro');
    expect(pedroBefore?.badgeText).toBeDefined();

    await clickOnNode(page, 'Pedro');
    await page
        .getByRole('button', {
            name: 'Explore',
            exact: true,
        })
        .click();
    await page
        .getByRole('menuitem', {
            name: 'Show all nodes directly connected to selection',
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes.map((n) => n.id)).toEqual([
        'Pedro',
        'Ben',
        'Hamza',
    ]);
    const afterState = await getGraphState(page);
    expect(
        afterState.nodes.find((n) => n.id === 'Pedro')?.badgeText,
    ).toBeUndefined();
});

test('Degree badge appears before expansion and disappears after', async ({
    page,
}) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await fitView(page);

    // Before expansion: Pedro has hidden neighbours, badge should be visible
    const clipBefore = await getNodeScreenshotClip(page, 'Pedro');
    expect(await page.screenshot({ clip: clipBefore })).toMatchSnapshot(
        'pedro-badge-before-expansion.png',
    );

    // Expand Pedro
    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page);
    await fitView(page);

    // After expansion: all neighbours visible, badge should be gone
    const clipAfter = await getNodeScreenshotClip(page, 'Pedro');
    expect(await page.screenshot({ clip: clipAfter })).toMatchSnapshot(
        'pedro-badge-after-expansion.png',
    );
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

    await clickOnNodes(page, ['Hamza', 'Pedro']);
    await page
        .getByRole('button', {
            name: 'Explore',
            exact: true,
        })
        .click();
    await page
        .getByRole('menuitem', {
            name: 'Show nodes connected to all selected nodes',
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes.map((n) => n.id)).toEqual([
        'Pedro',
        'Hamza',
        'Ben',
    ]);
});

test('Click edge to reveal right hand side panel details', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page, 2000, 2000);
    await fitView(page);
    await clickOnEdge(page, 'Hamza', 'Pedro');
    await page.waitForTimeout(100);
    await changeTab(page, 'Selected');
    await page.getByRole('button', { name: 'EDGE STATISTICS' }).click();
    await expect(page.getByText('Madrid')).toBeVisible();
    await expect(page.getByText('Layer Names')).toBeVisible();
    await expect(page.getByText('Earliest Time')).toBeVisible();
    await expect(page.getByText('Latest Time')).toBeVisible();
    await expect(page.getByText('meets, transfers')).toBeVisible();
    await expect(page.getByText('Hamza -> Pedro')).toBeVisible();
    await expect(page.getByText('Amount')).toBeVisible();
    await changeTab(page, 'Pedro -> Hamza Log');
    await expect(page.getByText('Pedro -> transfers -> Hamza')).toBeVisible();
});

test('Undo and redo in floating actions menu', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page);
    await page.getByRole('button', { name: 'Undo (⌘Z)', exact: true }).click();
    await waitForLayoutToFinish(page);
    await page.getByRole('button', { name: 'Redo (⌘⇧Z)', exact: true }).click();
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes).toHaveLength(3);
});

test('Expand node, fit view and select all similar nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await waitForLayoutToFinish(page);
    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page);
    await fitView(page);
    await clickOnNode(page, 'Pedro');
    await page.getByRole('button', { name: 'Selection' }).click();
    await page
        .getByRole('menuitem', {
            name: 'Select all nodes with the same type as selection',
        })
        .click();
    await waitForLayoutToFinish(page);
    const state = await getGraphState(page);
    expect(state.selected).toHaveLength(3);
});

test('Click and deselect by floating actions', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await clickOnNode(page, 'Pedro');
    await changeTab(page, 'Selected');

    await expect(page.getByText('Pedro').nth(0)).toBeVisible();
    await page.getByRole('button', { name: 'Selection' }).click();
    await page
        .getByRole('menuitem', { name: 'Clear current selection' })
        .click();
    await waitForLayoutToFinish(page);
    await expect(page.getByText('Pedro').nth(0)).toBeHidden();
});

test('Select all from menu and via shortcut', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await page.waitForTimeout(500);
    await page.locator('canvas').nth(1).click();
    await page.waitForTimeout(100);
    await page.keyboard.down('Control');
    await page.waitForTimeout(100);
    await page.locator('canvas').nth(1).press('a');
    await page.waitForTimeout(100);
    await page.keyboard.up('Control');
    await page.waitForTimeout(500);
    const state = await getGraphState(page);
    expect(state.selected).toEqual([
        'None',
        'Pedro',
        'Ben',
        'Hamza',
        'Pometry',
    ]);
    await page.getByRole('button', { name: 'Selection' }).click();
    await page
        .getByRole('menuitem', { name: 'Clear current selection', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    const state2 = await getGraphState(page);
    expect(state2.selected).toHaveLength(0);
    await page.getByRole('button', { name: 'Selection' }).click();
    await page
        .getByRole('menuitem', {
            name: 'Select every node in the graph',
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page);
    const state3 = await getGraphState(page);
    expect(state3.selected).toEqual([
        'None',
        'Pedro',
        'Ben',
        'Hamza',
        'Pometry',
    ]);
});

test('Click backspace to delete nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await doubleClickOnNode(page, 'Pedro');
    await selectLayout(page, 'Arrange nodes in concentric circles');
    await fitView(page);
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes).toHaveLength(3);
    await clickOnNode(page, 'Hamza');
    await page.keyboard.press('Backspace');
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes).toHaveLength(2);
});

test('RHS Selected properties has max height for table cells', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'new_folder',
        'persistent_second_filler',
    );
    await changeTab(page, 'Selected');
    await clickOnNode(page, 'Rabbit Inc');
    // Expect that table cells have a max height that hides the majority of the
    // text such that you can still see elements below the properties, such as
    // Direct Connections.
    await expect(page.getByText('Connections')).toBeVisible();
});

test.describe('Change colour and size of individual node', () => {
    test.use({ isolatedGraphsConfig: ['new_folder/persistent_filler'] });

    test('Change colour and size of individual node', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);
        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: 'BD10E0', size: 30 });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        const state = await getGraphState(page);
        expect(state.nodes.find((n) => n.id === 'Pedro')?.colour).toEqual(
            '#bd10e0',
        );
        expect(state.nodes.find((n) => n.id === 'Pedro')?.size).toEqual(30);
    });
});

test.describe('Change colour only of individual node persists', () => {
    test.use({ isolatedGraphsConfig: ['new_folder/persistent_filler'] });

    test('Change colour only of individual node persists', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);
        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: 'BD10E0' });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        const stateImmediate = await getGraphState(page);
        expect(
            stateImmediate.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#bd10e0');
        const hexInputAfterSave = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterSave.toLowerCase()).toBe('bd10e0');

        await page.reload();
        await waitForLayoutToFinish(page);
        const stateAfterReload = await getGraphState(page);
        expect(
            stateAfterReload.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#bd10e0');
        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        const hexInputAfterReload = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterReload.toLowerCase()).toBe('bd10e0');
    });
});

test.describe('Change colour only of node by type persists', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Change colour only of node by type persists', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'Person' }).click();
        await fillInStyling(page, { colourValue: 'D0021B' });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        await page.waitForTimeout(2000);
        const stateImmediate = await getGraphState(page);
        expect(
            stateImmediate.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#d0021b');
        const hexInputAfterSave = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterSave.toLowerCase()).toBe('d0021b');

        await page.reload();
        await waitForLayoutToFinish(page);
        const stateAfterReload = await getGraphState(page);
        expect(
            stateAfterReload.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#d0021b');
        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'Person' }).click();
        const hexInputAfterReload = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterReload.toLowerCase()).toBe('d0021b');
    });
});

test.describe('Change colour only of edge persists in picker after save', () => {
    test.use({
        isolatedGraphsConfig: ['new_folder/persistent_second_filler'],
    });

    test('Change colour only of edge persists in picker after save', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_second_filler');
        await fitView(page);

        await clickOnEdge(page, 'Judy', 'Rabbit Inc');
        await changeTab(page, 'Styling');
        await page.waitForTimeout(100);
        await page.getByText('Select Edge Layer').click();
        await page.getByRole('option', { name: 'advises' }).click();
        await fillInStyling(page, { colourValue: 'F5A623' });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        await page.waitForTimeout(2000);
        const hexInputAfterSave = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterSave.toLowerCase()).toBe('f5a623');

        await page.reload();
        await waitForLayoutToFinish(page);
        await clickOnEdge(page, 'Judy', 'Rabbit Inc');
        await changeTab(page, 'Styling');
        await page.waitForTimeout(100);
        await page.getByText('Select Edge Layer').click();
        await page.getByRole('option', { name: 'advises' }).click();
        const hexInputAfterReload = await page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox')
            .inputValue();
        expect(hexInputAfterReload.toLowerCase()).toBe('f5a623');
    });
});

test.describe('Change colour of edge by layer dropdown', () => {
    test.use({
        isolatedGraphsConfig: ['new_folder/persistent_second_filler'],
    });

    test('Change colour of edge by layer dropdown', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_second_filler');
        await fitView(page);

        await clickOnEdge(page, 'Judy', 'Rabbit Inc');
        await changeTab(page, 'Styling');
        await page.waitForTimeout(100);
        await page.getByText('Select Edge Layer').click();
        await page.getByRole('option', { name: 'advises' }).click();
        await fillInStyling(page, { colourValue: 'F5A623' });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        await openTimeline(page);
        await page.waitForTimeout(5000);
        await expect(
            page
                .getByLabel('Edge ID Judy->RabbitInc_advises_100')
                .locator('path'),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
    });
});

test.describe('Change colour and size of node by type', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Change colour and size of node by type', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'Person' }).click();
        await fillInStyling(page, { colourValue: 'D0021B', size: 30 });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(page.getByText('Styling updated')).toBeVisible({
            timeout: 5000,
        });
        await page.waitForTimeout(2000);
        const state = await getGraphState(page);
        await expect(
            state?.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#d0021b');
        expect(state.nodes.find((n) => n.id === 'Pedro')?.size).toEqual(30);
        expect(state.nodes.find((n) => n.id === 'Hamza')?.colour).toEqual(
            '#d0021b',
        );
        await expect(state?.nodes.find((n) => n.id === 'Hamza')?.size).toEqual(
            30,
        );
        await expect(state?.nodes.find((n) => n.id === 'Ben')?.colour).toEqual(
            '#d0021b',
        );
        await expect(state?.nodes.find((n) => n.id === 'Ben')?.size).toEqual(
            30,
        );
    });
});

test('Preview colour and size by type changes', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'second_filler',
    );
    await fitView(page);
    await changeTab(page, 'Styling');
    await page.getByText('Select Node Type').click();
    await page.getByRole('option', { name: 'Person' }).click();
    await fillInStyling(page, { colourValue: 'D0021B', size: 30 });
    await page.waitForTimeout(1000);
    const state = await getGraphState(page);
    expect(state.nodes.find((n) => n.id === 'Fred')?.colour).toEqual('#d0021b');
    expect(state.nodes.find((n) => n.id === 'Fred')?.size).toEqual(30);
});

test('Preview colour and size changes', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );
    await fitView(page);
    await clickOnNode(page, 'Ben');
    await changeTab(page, 'Styling');
    await fillInStyling(page, { colourValue: 'BD10E0', size: 30 });
    await page.waitForTimeout(1000);
    const state = await getGraphState(page);
    expect(state.nodes.find((n) => n.id === 'Ben')?.colour).toEqual('#bd10e0');
    expect(state.nodes.find((n) => n.id === 'Ben')?.size).toEqual(30);
});

test('Preview edge colour changes', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_second_filler',
    );
    await fitView(page);

    await clickOnEdge(page, 'Judy', 'Rabbit Inc');
    await changeTab(page, 'Styling');
    await page.getByText('Select Edge Layer').click();
    await page.getByRole('option', { name: 'advises' }).click();
    await fillColorPickerHexInput(page, 'F5A623');
    await openTimeline(page);
    // Wait for the timeline to open fully
    await page.waitForTimeout(500);
    await expect(
        page.getByLabel('Edge ID Judy->RabbitInc_advises_100').locator('path'),
    ).toHaveCSS('fill', 'rgb(245, 166, 35)');
});

test('Layout Customizer Default Advanced Options', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );
    await changeTab(page, 'Layout');

    expect(await page.locator('canvas').nth(1).screenshot()).toMatchSnapshot(
        'layout-customizer-default.png',
    );
    await dragSlider({
        page,
        slider: page.getByLabel('Nodes start repelling each'),
        root: page.getByLabel('Collision Radius Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('The strength with which the'),
        root: page.getByLabel('Collision Strength Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('The ideal edge length'),
        root: page.getByLabel('Link Distance Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('The strength of the link'),
        root: page.getByLabel('Link Strength Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('A negative force represents "'),
        root: page.getByLabel('Many-Body Force Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('The minimum/maximum distance').first(),
        root: page.getByLabel('Many-Body Range Slider Container'),
        sliderPosition: 0.2,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('The minimum/maximum distance').nth(1),
        root: page.getByLabel('Many-Body Range Slider Container'),
        sliderPosition: 0.8,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('Force pulls nodes towards'),
        root: page.getByLabel('Center Force Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('Strength of the radial force'),
        root: page.getByLabel('Radial force strength Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('Radius of the radial force'),
        root: page.getByLabel('Radial force radius Slider Container'),
        sliderPosition: 0.5,
    });
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    expect(await page.locator('canvas').nth(1).screenshot()).toMatchSnapshot(
        'layout-customizer-default-all-changed.png',
    );
});

test('Layout Customizer Default Pre-layout', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );
    await changeTab(page, 'Layout');

    await page.getByRole('checkbox', { name: 'Clockwise' }).check();
    await page.getByRole('checkbox', { name: 'Equidistant rings' }).check();
    await dragSlider({
        page,
        slider: page.getByLabel('Used for collision detection'),
        root: page.getByLabel('Node size (diameter) Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('Minimum spacing between rings'),
        root: page.getByLabel('Node spacing Slider Container'),
        sliderPosition: 0.5,
    });
    await page.getByRole('checkbox', { name: 'Prevent overlap' }).check();
    await dragSlider({
        page,
        slider: page.getByLabel('The angle (in radians) to'),
        root: page.getByLabel('Start Angle (pi radians) Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel(
            'The angle difference between the first and last node in the same layer',
        ),
        root: page.getByLabel('Sweep Slider Container'),
        sliderPosition: 0.5,
    });
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    expect(await page.locator('canvas').nth(1).screenshot()).toMatchSnapshot(
        'layout-customizer-prelayout-all-changed.png',
    );
});

test('Layout Customizer can change to concentric layout', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );
    await changeTab(page, 'Layout');

    await page.getByText('Default Layout').click();
    await page.getByRole('option', { name: 'Concentric Layout' }).click();

    await page.getByRole('checkbox', { name: 'Clockwise' }).check();
    await page.getByRole('checkbox', { name: 'Equidistant rings' }).check();
    await dragSlider({
        page,
        slider: page.getByLabel('Used for collision detection'),
        root: page.getByLabel('Node size (diameter) Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel('Minimum spacing between rings'),
        root: page.getByLabel('Node spacing Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel(
            'The angle (in radians) to start laying out nodes',
        ),
        root: page.getByLabel('Start Angle (pi radians) Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel(
            'The angle difference between the first and last node in the same layer',
        ),
        root: page.getByLabel('Sweep Slider Container'),
        sliderPosition: 0.5,
    });
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    await fitView(page);
    expect(await page.locator('canvas').nth(1).screenshot()).toMatchSnapshot(
        'layout-customizer-concentric-all-changed.png',
    );
});

test('Layout Customizer can use dagre for pre-layout', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );
    await changeTab(page, 'Layout');

    // TODO: make this into a reusable function for picking an option from a MUI dropdown like this (note this does not use timeouts)
    await page.getByText('Concentric Layout').click();
    await page.getByRole('option', { name: 'Hierarchical TD Layout' }).click();
    await page.locator('ul[role="listbox"]').waitFor({ state: 'detached' }); // wait for MUI Select portal to be fully removed from DOM (close animation completes)

    await page.getByRole('checkbox', { name: 'Invert direction' }).check();

    await page.getByRole('combobox', { name: 'Alignment' }).click();
    await page.getByRole('option', { name: 'Upper Right' }).click();
    await page.locator('ul[role="listbox"]').waitFor({ state: 'detached' });

    await dragSlider({
        page,
        slider: page.getByLabel("For TB or BT, it's the horizontal spacing"),
        root: page.getByLabel('Node separation (px) Slider Container'),
        sliderPosition: 0.5,
    });

    await dragSlider({
        page,
        slider: page.getByLabel("For TB or BT, it's the vertical spacing"),
        root: page.getByLabel('Rank separation (px) Slider Container'),
        sliderPosition: 0.5,
    });

    await page.getByRole('combobox', { name: 'Ranking algorithm' }).click();
    await page.getByRole('option', { name: 'tight-tree' }).click();
    await page.locator('ul[role="listbox"]').waitFor({ state: 'detached' });
    // Needed to make the dragSlider work
    await page.getByLabel('Size of node').click();
    await dragSlider({
        page,
        slider: page.getByLabel('Size of node'),
        root: page.getByLabel('Node size Slider Container'),
        sliderPosition: 0.5,
    });
    await page.getByRole('checkbox', { name: 'Control points' }).check();
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    expect(await page.locator('canvas').nth(1).screenshot()).toMatchSnapshot(
        'layout-customizer-prelayout-dagre-all-changed.png',
    );
});

test('Brush select on main canvas works from first click', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(
        page,
        'vanilla',
        'persistent_filler',
    );

    // Box from None to Ben in the current persistent_filler layout. Padding so
    // both node centres are inside the brush region; coordinates derived from
    // the live layout so the test isn't tied to a hardcoded layout.
    const positions = await getNodePositions(page, ['None', 'Ben']);
    const padding = 30;
    const left = Math.min(positions.None.x, positions.Ben.x) - padding;
    const right = Math.max(positions.None.x, positions.Ben.x) + padding;
    const top = Math.min(positions.None.y, positions.Ben.y) - padding;
    const bottom = Math.max(positions.None.y, positions.Ben.y) + padding;

    await page.keyboard.down('Shift');
    await page.mouse.move(right, top);
    await page.mouse.down();
    await page.waitForTimeout(100);
    await page.mouse.move(left, bottom);
    await page.mouse.up();
    await page.keyboard.up('Shift');
    const state = await getGraphState(page);
    expect(state.selected).toEqual(['None', 'Ben']);
});

test('Shift+click an already-selected node deselects it', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await fitView(page);

    await clickOnNode(page, 'Pedro');
    await clickOnNode(page, 'Pedro', { modifiers: ['Shift'] });
    const state = await getGraphState(page);
    expect(state.selected).toHaveLength(0);
    await clickOnNode(page, 'Ben', { modifiers: ['Shift'] });
    const multiSelectState = await getGraphState(page);
    expect(multiSelectState.selected).toHaveLength(1);
    expect(multiSelectState.selected).toContain('Ben');
});

test('catch console logs and errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    const consoleLogs: string[] = [];

    page.on('console', (message) => {
        switch (message.type()) {
            case 'error': {
                consoleErrors.push(message.text());
                break;
            }
            case 'log': {
                consoleLogs.push(message.text());
                break;
            }
        }
    });

    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');

    expect(consoleErrors, 'Console errors found').toStrictEqual([]);
    expect(consoleLogs, 'Console logs found').toStrictEqual([]);
});

test.describe('Comprehensive styling, selection, highlighting, layout and saving', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Comprehensive styling, selection, highlighting, layout and saving', async ({
        page,
        isolatedGraphs,
    }) => {
        test.setTimeout(90000);
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        // Save individual node styling for Pedro
        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: 'BD10E0', size: 30 });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await page.waitForSelector('text=Styling updated');
        await page.waitForTimeout(2000);
        await expect(page.getByText('#bd10e0', { exact: true })).toBeVisible();
        await expect(page.getByText('30', { exact: true })).toBeVisible();
        // Save Person type styling
        await page.getByRole('button', { name: 'Selection' }).click();
        await page
            .getByRole('menuitem', { name: 'Clear current selection' })
            .click();
        await page.waitForTimeout(2000);
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'Person' }).click();
        await fillInStyling(page, { colourValue: 'D0021B', size: 25 });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await page.waitForSelector('text=Styling updated');
        await expect(page.getByText('#d0021b', { exact: true })).toBeVisible();
        await expect(page.getByText('25', { exact: true })).toBeVisible();
        // Save meets edge layer styling
        await clickOnEdge(page, 'Pedro', 'Hamza');
        await changeTab(page, 'Styling');
        await page.waitForTimeout(100);
        await page.getByText('Select Edge Layer').click();
        await page.getByRole('option', { name: 'meets' }).click();
        await fillInStyling(page, { colourValue: 'F5A623' });
        await page.getByRole('button', { name: 'Save', exact: true }).click();
        await expect(
            page
                .locator('div')
                .filter({ hasText: /^Hex$/ })
                .getByRole('textbox'),
        ).toHaveValue('F5A623');
        await page.waitForSelector('text=Styling updated');
        // Delete Ben
        await clickOnNode(page, 'Ben');
        await page
            .getByRole('button', {
                name: 'Delete selected (⌫)',
            })
            .click();
        await waitForLayoutToFinish(page);
        // Un-focuses the delete tooltip so it doesn't block the click on the None node
        await page.mouse.move(0, 0);
        // Delete None
        await clickOnNode(page, 'None');
        await page
            .getByRole('button', {
                name: 'Delete selected (⌫)',
            })
            .click();
        await waitForLayoutToFinish(page);
        // Undo (restores None)
        await page
            .getByRole('button', { name: 'Undo (⌘Z)', exact: true })
            .click();
        // Redo (deletes None again)
        await page
            .getByRole('button', { name: 'Redo (⌘⇧Z)', exact: true })
            .click();
        await waitForLayoutToFinish(page);
        // Undo (restores None)
        await page
            .getByRole('button', { name: 'Undo (⌘Z)', exact: true })
            .click();
        await waitForLayoutToFinish(page);
        // Preview individual node styling for Hamza
        await clickOnNode(page, 'Hamza');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: '4A90D9', size: 35 });
        await page.waitForTimeout(1000);
        // Preview Company type styling
        await page.getByRole('button', { name: 'Selection' }).click();
        await page
            .getByRole('menuitem', { name: 'Clear current selection' })
            .click();
        await page.waitForTimeout(2000);
        await page.getByText('Person').click();
        await page.getByRole('option', { name: 'Company' }).click();
        await fillInStyling(page, { colourValue: '7ED321', size: 20 });
        // Preview founds edge layer styling
        await clickOnEdge(page, 'Hamza', 'Pometry');
        await changeTab(page, 'Styling');
        await page.waitForTimeout(100);
        await page.getByText('Select Edge Layer').click();
        await page.getByRole('option', { name: 'founds' }).click();
        await fillInStyling(page, { colourValue: 'FF6B6B' });
        await openTimeline(page);
        await selectLayout(page, 'Arrange nodes in concentric circles');
        await fitView(page);
        await waitForLayoutToFinish(page);
        expect(
            await page.locator('canvas').nth(1).screenshot(),
        ).toMatchSnapshot(
            'comprehensive-styling-selecting-highlighting-layout-saving.png',
        );

        // Save the graph (positions + styles persist to metadata)
        const newGraphName = `${isolatedGraphs.namespace}/comprehensive_save_test`;
        await page.getByRole('button', { name: 'Save graph as' }).click();
        await page.getByLabel('New Graph Name').fill(newGraphName);
        await page.getByRole('button', { name: 'Confirm' }).click();
        await waitForLayoutToFinish(page);
        isolatedGraphs.trackForCleanup(newGraphName);

        // Reload to confirm the saved positions are loaded back from metadata
        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        // Capture concentric-layout positions (reloaded from saved metadata)
        const positionsAfterReload = await getNodePositions(page, [
            'Pedro',
            'Hamza',
            'Pometry',
        ]);

        // Switch to a different layout — should override the saved positions
        await selectLayout(page, 'Force-directed layout algorithm');
        await fitView(page);

        // Capture the post-switch positions
        const positionsAfterSwitch = await getNodePositions(page, [
            'Pedro',
            'Hamza',
            'Pometry',
        ]);

        // The layout switch should have moved nodes meaningfully. Sum of
        // per-node Manhattan deltas across the three sampled nodes is the
        // regression check: if the bug returns, the layout switch is silently
        // ignored and this sum is ~0.
        const totalDelta = (['Pedro', 'Hamza', 'Pometry'] as const).reduce(
            (sum, name) =>
                sum +
                Math.abs(
                    positionsAfterSwitch[name].x - positionsAfterReload[name].x,
                ) +
                Math.abs(
                    positionsAfterSwitch[name].y - positionsAfterReload[name].y,
                ),
            0,
        );
        expect(totalDelta).toBeGreaterThan(50);
    });
});

test('Save new graph with save as dialog', async ({ page, isolatedGraphs }) => {
    await page.goto(
        isolatedGraphs.graphUrl('event', 'initialNodes=%5B%22Pedro%22%5D'),
    );
    await waitForLayoutToFinish(page);

    await page.getByRole('button', { name: 'Save graph as' }).click();
    await expect(page.getByLabel('New Graph Name')).toBeVisible();

    const newName = `${isolatedGraphs.namespace}/test_graph`;
    await page.getByLabel('New Graph Name').fill(newName);
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);

    isolatedGraphs.trackForCleanup(newName);
    await expect(page).toHaveURL(new RegExp(`/graph/${newName}`));
});

test('Right-clicking a node shows the context menu', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await expect(
        page.getByRole('menuitem', { name: 'Expand', exact: true }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Find Shortest Path' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Shared Neighbours' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Select all similar' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Deselect all' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Invert selection' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Select related' }),
    ).toBeVisible();
    await expect(
        page.getByRole('menuitem', { name: 'Open Trace Log' }),
    ).toBeVisible();
    await expect(page.getByRole('menuitem', { name: 'Delete' })).toBeVisible();
});

test('Right-clicking a selected node preserves multi-selection', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await clickOnNodes(page, ['Pedro', 'Hamza']);

    const selectedBefore = (await getGraphState(page)).selected;
    expect(selectedBefore).toContain('Pedro');
    expect(selectedBefore).toContain('Hamza');

    await rightClickOnNode(page, 'Pedro');
    await expect(
        page.getByRole('menuitem', { name: 'Expand', exact: true }),
    ).toBeVisible();

    const selectedAfter = (await getGraphState(page)).selected;
    expect(selectedAfter).toContain('Pedro');
    expect(selectedAfter).toContain('Hamza');
});

test('Ctrl+clicking a selected node opens the context menu and preserves multi-selection (macOS only)', async ({
    page,
    browserName,
}) => {
    test.skip(
        browserName !== 'webkit',
        'ctrl+click → contextmenu is a macOS-only behavior; covered by webkit project',
    );

    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await clickOnNodes(page, ['Pedro', 'Hamza']);

    const selectedBefore = (await getGraphState(page)).selected;
    expect(selectedBefore).toContain('Pedro');
    expect(selectedBefore).toContain('Hamza');

    await ctrlClickOnNode(page, 'Pedro');
    await expect(
        page.getByRole('menuitem', { name: 'Expand', exact: true }),
    ).toBeVisible();

    const selectedAfter = (await getGraphState(page)).selected;
    expect(selectedAfter).toContain('Pedro');
    expect(selectedAfter).toContain('Hamza');
});

test('Context menu deselect all clears node selection', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Hamza');
    await page.getByRole('menuitem', { name: 'Deselect all' }).click();

    const state = await getGraphState(page);
    expect(state.selected).toHaveLength(0);
});

test('Context menu invert selection', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Invert selection' }).click();

    const state = await getGraphState(page);
    expect(state.selected).not.toContain('Pedro');
    expect(state.selected.length).toEqual(4);
});

test('Context menu select all similar selects all nodes of the same type', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Select all similar' }).click();

    const state = await getGraphState(page);
    expect(state.selected).toContain('Pedro');
    expect(state.selected).toContain('Hamza');
    expect(state.selected).toContain('Ben');
});

test('Context menu delete removes the node from the graph', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Delete' }).click();
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.selected).not.toContain('Pedro');
});

test('Context menu select related selects connected nodes', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Select related' }).click();

    const state = await getGraphState(page);
    expect(state.selected).toContain('Hamza');
    expect(state.selected).toContain('Ben');
    expect(state.selected).toContain('Pedro');
});

test('Context menu open trace log opens the drawer on the trace log tab', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Open Trace Log' }).click();

    await expect(page.getByRole('tab', { name: 'Trace Log' })).toBeVisible();
});

test('After context menu opens trace log, switching to Connections tab works', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Open Trace Log' }).click();

    await expect(
        page.getByRole('columnheader', { name: 'Timestamp' }),
    ).toBeVisible();

    await page.getByRole('tab', { name: 'Connections' }).click();

    await expect(
        page.getByRole('columnheader', { name: 'Name' }),
    ).toBeVisible();
});

test('Clicking a trace log row goes to corresponding edge', async ({
    page,
}) => {
    await page.goto('/graph/vanilla/event?initialNodes=%5B%5D');
    await waitForLayoutToFinish(page);
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Open Trace Log' }).click();
    await page.getByRole('cell', { name: 'Ben -> meets -> Pedro' }).click();
    await page.getByText('Ben → Pedro').click();
    await page.getByRole('cell', { name: 'Ben -> meets -> Pedro' }).click();
    await waitForLayoutToFinish(page);
    await expect(page.getByText('Ben → Pedro')).toBeVisible();
    await expect(
        page.getByRole('button', { name: 'Time Appeared' }),
    ).toBeVisible();

    await openTimeline(page);
    const benPedroEdge = page.getByLabel(
        'Edge ID Ben->Pedro_meets_1679356800000',
    );
    await expect(benPedroEdge.locator('circle[r="7"]').first()).toHaveCSS(
        'fill-opacity',
        '0.5',
    );
});

test('Context menu expand adds connected nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Expand', exact: true }).click();
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).toEqual(['Pedro', 'Ben', 'Hamza']);
});

test('Context menu find shortest path between two nodes', async ({ page }) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await clickOnNodes(page, ['Pedro', 'Pometry']);
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Find Shortest Path' }).click();
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.selected.length).toBeGreaterThanOrEqual(3);
    expect(state.selected).toContain('Pedro');
    expect(state.selected).toContain('Pometry');
});

test('Context menu shared neighbours finds common connections', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await clickOnNodes(page, ['Pedro', 'Pometry']);
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Shared Neighbours' }).click();
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).toContain('Hamza');
    expect(state.nodes.map((n) => n.id)).toContain('Ben');
});

test('Shift+click multi-select and plain click single-select stay synced with G6 state', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');

    // Single click Pedro — only Pedro selected
    await clickOnNode(page, 'Pedro');
    let state = await getGraphState(page);
    expect(state.selected).toEqual(['Pedro']);

    // Shift+click Hamza — both Pedro and Hamza selected
    await clickOnNode(page, 'Hamza', { modifiers: ['Shift'] });
    state = await getGraphState(page);
    expect(state.selected).toContain('Pedro');
    expect(state.selected).toContain('Hamza');
    expect(state.selected).toHaveLength(2);

    // Shift+click Ben — all three selected
    await clickOnNode(page, 'Ben', { modifiers: ['Shift'] });
    state = await getGraphState(page);
    expect(state.selected).toContain('Pedro');
    expect(state.selected).toContain('Hamza');
    expect(state.selected).toContain('Ben');
    expect(state.selected).toHaveLength(3);

    // Shift+click Pedro again — deselects Pedro, Hamza and Ben remain
    await clickOnNode(page, 'Pedro', { modifiers: ['Shift'] });
    state = await getGraphState(page);
    expect(state.selected).not.toContain('Pedro');
    expect(state.selected).toContain('Hamza');
    expect(state.selected).toContain('Ben');
    expect(state.selected).toHaveLength(2);

    // Plain click on Pometry — only Pometry selected (clears multi-select)
    await clickOnNode(page, 'Pometry');
    state = await getGraphState(page);
    expect(state.selected).toEqual(['Pometry']);
});

test('Adding a node from connections table does not change selection state', async ({
    page,
}) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await clickOnNode(page, 'Pedro');
    await changeTab(page, 'Selected');

    // Verify Pedro is selected
    const state = await getGraphState(page);
    expect(state.selected).toEqual(['Pedro']);

    // Switch to Connections tab
    await changeTab(page, 'Connections');
    await expect(
        page.getByRole('columnheader', { name: 'Name' }),
    ).toBeVisible();

    // Click the add button for a node not yet on the graph
    const addButton = page
        .getByRole('row', {
            name: 'Add to graph Person Ben meets 3/21/',
        })
        .getByLabel('Add to graph');
    await addButton.click();
    await waitForLayoutToFinish(page);

    // Verify: new node was added to the graph
    const stateAfter = await getGraphState(page);
    expect(stateAfter.nodes.length).toBeGreaterThan(1);

    // Verify: selection state unchanged — Pedro still selected, nothing else
    expect(stateAfter.selected).toEqual(['Pedro']);
});

test('Dragging one of multiple selected nodes moves all selected nodes', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');
    await fitView(page);

    // Select Pedro and Hamza
    await clickOnNodes(page, ['Pedro', 'Hamza']);
    const state = await getGraphState(page);
    expect(state.selected).toContain('Pedro');
    expect(state.selected).toContain('Hamza');

    // Record positions before drag
    const positionsBefore = await getNodePositions(page, [
        'Pedro',
        'Hamza',
        'Ben',
    ]);

    // Drag Pedro by (50, 50) — Hamza should move too, Ben should not
    const pedroPos = positionsBefore['Pedro'];
    const canvas = page.locator('canvas').nth(1);
    await canvas.hover({ position: pedroPos });
    await page.mouse.down();
    await page.waitForTimeout(200);
    await page.mouse.move(pedroPos.x + 50, pedroPos.y + 50, { steps: 5 });
    await page.mouse.up();
    await page.waitForTimeout(500);

    // Record positions after drag
    const positionsAfter = await getNodePositions(page, [
        'Pedro',
        'Hamza',
        'Ben',
    ]);

    // Pedro and Hamza should have moved
    const pedroDx = positionsAfter['Pedro'].x - positionsBefore['Pedro'].x;
    const pedroDy = positionsAfter['Pedro'].y - positionsBefore['Pedro'].y;
    const hamzaDx = positionsAfter['Hamza'].x - positionsBefore['Hamza'].x;
    const hamzaDy = positionsAfter['Hamza'].y - positionsBefore['Hamza'].y;

    expect(Math.abs(pedroDx)).toBeGreaterThan(10);
    expect(Math.abs(pedroDy)).toBeGreaterThan(10);
    expect(Math.abs(hamzaDx)).toBeGreaterThan(10);
    expect(Math.abs(hamzaDy)).toBeGreaterThan(10);

    // Pedro and Hamza should have moved by similar amounts
    expect(Math.abs(pedroDx - hamzaDx)).toBeLessThan(5);
    expect(Math.abs(pedroDy - hamzaDy)).toBeLessThan(5);

    // Ben (not selected) should not have moved significantly
    const benDx = Math.abs(positionsAfter['Ben'].x - positionsBefore['Ben'].x);
    const benDy = Math.abs(positionsAfter['Ben'].y - positionsBefore['Ben'].y);
    expect(benDx).toBeLessThan(5);
    expect(benDy).toBeLessThan(5);
});

test('Clicks at canvas edge positions reach the G6 canvas', async ({
    page,
}) => {
    await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'persistent');

    // Collapse the RHS panel to maximize canvas area
    await page.getByRole('button', { name: 'Collapse panel' }).click();
    await page.waitForTimeout(300);

    const canvas = page.locator('canvas').nth(1);
    const canvasBox = await canvas.boundingBox();
    if (!canvasBox) throw new Error('Canvas not visible');

    // The appbar (left), collapsed RHS (right), and floating actions menu
    // (bottom center) all float above the canvas. Compute the visible canvas
    // region by excluding their bounding boxes from the canvas bounds.
    const navBox = await page.locator('nav').first().boundingBox();
    const rhsCollapseToggle = await page
        .getByRole('button', { name: 'Expand Overview' })
        .boundingBox();
    // The floating actions menu is identified by walking up from the Layout
    // button (which is unique to that menu).
    const floatingActionsBox = await page
        .getByRole('button', { name: 'Layout' })
        .locator('xpath=ancestor::div[1]')
        .boundingBox();

    const safeLeft =
        navBox !== null ? navBox.x + navBox.width + 5 : canvasBox.x + 10;
    const safeRight =
        rhsCollapseToggle !== null
            ? rhsCollapseToggle.x - 5
            : canvasBox.x + canvasBox.width - 10;
    const safeBottom =
        floatingActionsBox !== null
            ? floatingActionsBox.y - 5
            : canvasBox.y + canvasBox.height - 10;
    const safeTop = canvasBox.y + 10;

    const edgeCasePositions = [
        { x: safeLeft, y: safeTop, label: 'top-left' },
        { x: safeRight, y: safeTop, label: 'top-right' },
        { x: safeLeft, y: safeBottom, label: 'bottom-left' },
        { x: safeRight, y: safeBottom, label: 'bottom-right' },
        {
            x: canvasBox.x + canvasBox.width / 2,
            y: canvasBox.y + canvasBox.height / 2,
            label: 'center',
        },
    ];

    for (const pos of edgeCasePositions) {
        const elementTag = await page.evaluate(
            ({ x, y }) => {
                const el = document.elementFromPoint(x, y);
                return el?.tagName?.toLowerCase() ?? 'none';
            },
            { x: pos.x, y: pos.y },
        );
        expect(
            elementTag,
            `Click at ${pos.label} (${pos.x}, ${pos.y}) should hit canvas, got ${elementTag}`,
        ).toBe('canvas');
    }

    // TODO: Future enhancement — random coordinate fuzzing within viewport bounds
});

test('Save As creates a new graph and navigates to it', async ({
    page,
    isolatedGraphs,
}) => {
    await isolatedGraphs.navigateToGraph(page, 'event');

    // Open Save As dialog
    await page.getByRole('button', { name: 'Save graph as' }).click();
    await expect(page.getByText('New Graph Name')).toBeVisible();

    // Enter new graph name within the same namespace
    const newGraphName = `${isolatedGraphs.namespace}/saved_test`;
    await page.getByLabel('New Graph Name').fill(newGraphName);

    // Confirm
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);

    // Verify URL changed to new graph path
    isolatedGraphs.trackForCleanup(newGraphName);
    await expect(page).toHaveURL(new RegExp(`/graph/${newGraphName}`));
});

test('Save As preserves graph changes after reload', async ({
    page,
    isolatedGraphs,
}) => {
    await isolatedGraphs.navigateToGraph(page, 'event');

    // Delete a node to create an unsaved change
    await clickOnNode(page, 'Ben');
    await page.getByRole('button', { name: 'Delete selected (⌫)' }).click();
    await page.mouse.move(0, 0);
    await waitForLayoutToFinish(page);

    // Verify Ben is gone
    let state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).not.toContain('Ben');

    // Save As to a new graph
    await page.getByRole('button', { name: 'Save graph as' }).click();
    const newGraphName = `${isolatedGraphs.namespace}/persisted_test`;
    await page.getByLabel('New Graph Name').fill(newGraphName);
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);

    isolatedGraphs.trackForCleanup(newGraphName);

    // Reload the page
    await page.reload();
    await waitForLayoutToFinish(page);

    // Verify Ben is still gone after reload
    state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).not.toContain('Ben');
});

test.describe('Saved positions', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Node positions persist after save and reload', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        // Record Pedro's initial position
        const positionsBefore = await getNodePositions(page, ['Pedro']);

        // Drag Pedro to a new position
        const pedroPos = positionsBefore['Pedro'];
        const canvas = page.locator('canvas').nth(1);
        await canvas.hover({ position: pedroPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(pedroPos.x + 80, pedroPos.y + 80, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);

        // Save As to preserve positions
        await page.getByRole('button', { name: 'Save graph as' }).click();
        const newGraphName = `${isolatedGraphs.namespace}/position_test`;
        await page.getByLabel('New Graph Name').fill(newGraphName);
        await page.getByRole('button', { name: 'Confirm' }).click();
        await waitForLayoutToFinish(page);

        isolatedGraphs.trackForCleanup(newGraphName);

        // Reload the page
        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        // Verify position changed from original
        const positionsAfterReload = await getNodePositions(page, ['Pedro']);
        const dxFromOriginal = Math.abs(
            positionsAfterReload['Pedro'].x - positionsBefore['Pedro'].x,
        );
        const dyFromOriginal = Math.abs(
            positionsAfterReload['Pedro'].y - positionsBefore['Pedro'].y,
        );
        expect(dxFromOriginal + dyFromOriginal).toBeGreaterThan(10);
    });
});
// ---- Icon colour tests -------------------------------------------------------
// vanilla/filler has a Person icon set.
// These tests verify it is rendered white in the G6 graph view and temporal view.

test.describe('icon colours', () => {
    test('graph view node icons are white on the canvas', async ({ page }) => {
        await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'filler');
        await waitForLayoutToFinish(page);
        await fitView(page);
        await waitForLayoutToFinish(page);

        // Primary check: iconSrc must have ?color=white appended, proving the
        // recolouring pipeline ran.
        //
        // Fallback: if getNodeData doesn't expose iconSrc, sample canvas pixels
        // at the node centre (converting G6 world → viewport → pixel-buffer
        // coords via getViewportByCanvas + devicePixelRatio).
        await page.waitForFunction(
            () => {
                interface G6Graph {
                    getData(): {
                        nodes: { id: string; displayName: string }[];
                    };
                    getNodeData(id: string): {
                        style?: Record<string, unknown>;
                    };
                    getElementPosition(id: string): [number, number];
                    getViewportByCanvas(p: [number, number]): [number, number];
                }
                const graph = (window as Window & { __G6_GRAPH__?: G6Graph })
                    .__G6_GRAPH__;
                if (!graph) return false;
                try {
                    const nodes = graph.getData().nodes;
                    const benNode = nodes.find(
                        (n) => n.id === 'Ben' || n.displayName === 'Ben',
                    );
                    if (!benNode) return false;

                    // Primary: verify iconSrc has ?color=white appended
                    const nodeData = graph.getNodeData(benNode.id);
                    const iconSrc = nodeData?.style?.iconSrc;
                    if (
                        typeof iconSrc === 'string' &&
                        iconSrc.includes('color=white')
                    )
                        return true;

                    // Fallback: canvas pixel sampling at the node centre
                    const worldPt = graph.getElementPosition(benNode.id);
                    const vp = graph.getViewportByCanvas(worldPt);
                    const dpr = window.devicePixelRatio || 1;
                    const px = Math.round(vp[0] * dpr);
                    const py = Math.round(vp[1] * dpr);
                    const canvas = document.querySelectorAll(
                        'canvas',
                    )[1] as HTMLCanvasElement;
                    if (!canvas) return false;
                    const radius = 12;
                    const { data } = canvas
                        .getContext('2d')!
                        .getImageData(
                            px - radius,
                            py - radius,
                            radius * 2,
                            radius * 2,
                        );
                    // checks if white pixels are found in sampled area
                    for (let i = 0; i < data.length; i += 4) {
                        if (
                            data[i] > 200 &&
                            data[i + 1] > 200 &&
                            data[i + 2] > 200 &&
                            data[i + 3] > 50
                        )
                            return true;
                    }
                } catch {
                    return false;
                }
            },
            { timeout: 15000 },
        );
    });

    test('temporal view Y-axis icons have ?color=white in their href', async ({
        page,
    }) => {
        await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'filler');
        await waitForLayoutToFinish(page);
        await openTimeline(page);
        await page.waitForTimeout(1000);

        // Icons are rendered as SVG <image> elements with ?color=white appended
        // to the Iconify URL so the server returns a white-coloured SVG.
        // Use evaluate because WebKit doesn't support CSS attribute selectors
        // on SVG-namespaced elements.
        const hasWhiteIconifyIcon = await page.evaluate(() => {
            const images = document.querySelectorAll('#yaxis-nodes image');
            for (const img of images) {
                const href = img.getAttribute('href');
                if (
                    href?.includes('iconify.design') &&
                    href?.includes('color=white')
                ) {
                    return true;
                }
            }
            return false;
        });
        expect(hasWhiteIconifyIcon).toBe(true);
    });

    test('graph view node icons screenshot', async ({ page }) => {
        await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'filler');
        await waitForLayoutToFinish(page);
        await fitView(page);
        await waitForLayoutToFinish(page);
        // waitForLayoutToFinish includes a 2s pause, which is enough for
        // useRecoloredIconSrcs canvas compositing to complete.
        const pos = await getNodePositions(page, ['Ben']);
        const box = await page.locator('canvas').nth(1).boundingBox();
        const radius = 30;
        expect(
            await page.screenshot({
                clip: {
                    x: box!.x + pos['Ben'].x - radius,
                    y: box!.y + pos['Ben'].y - radius,
                    width: radius * 2,
                    height: radius * 2,
                },
            }),
        ).toMatchSnapshot('icon-graph-view.png');
    });

    test('temporal view Y-axis icons screenshot', async ({ page }) => {
        await navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'filler');
        await waitForLayoutToFinish(page);
        await openTimeline(page);
        // Wait for network idle so SVG <image> elements have finished fetching
        // the Iconify URLs before the screenshot is taken.
        await page.waitForLoadState('networkidle');
        expect(await page.locator('#yaxis-nodes').screenshot()).toMatchSnapshot(
            'icon-temporal-view.png',
        );
    });
});
