import { expect } from '@playwright/test';
import { test } from '../fixtures';
import {
    changeTab,
    clickOnEdge,
    clickOnNode,
    clickOnNodes,
    ctrlClickOnNode,
    deleteNodes,
    doubleClickOnNode,
    dragSlider,
    expectStylingHex,
    expectStylingHexInput,
    fillColorPickerHexInput,
    fillInStyling,
    fitView,
    getGraphState,
    getInteractiveCanvas,
    getNodePositions,
    navigateToGraphPageBySearch,
    rightClickOnNode,
    save,
    saveAs,
    saveAsWithRandomName,
    selectLayout,
    style,
} from './graph.utils';
import { navigateInSavedGraphs } from './saved-graphs.utils';
import { openTimeline } from './temporalview.utils';
import { waitForLayoutToFinish } from './utils';

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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save as ...').click();
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

    // Move the cursor away so the now-active transfers row's "Remove
    // highlight" tooltip (MUI interactive popper, placed bottom) closes —
    // otherwise on webkit it lingers over the founds row directly below
    // and intercepts the next click.
    await page.mouse.move(0, 0);
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });

    // The extra timeout here helps to make the next line more consistent
    await waitForLayoutToFinish(page, 3000, 3000);
    await selectLayout(page, 'Arrange nodes in concentric circles');
    expect(await page.screenshot()).toMatchSnapshot('concentric-layout.png');
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });

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

for (const nodeName of ['Pedro', 'Hamza', 'Ben']) {
    test(`Click on ${nodeName} node in graph`, async ({ page }) => {
        await navigateToGraphPageBySearch(page, {
            type: 'node',
            nodeName,
            nodeType: 'Person',
        });
        await clickOnNode(page, nodeName);
        await changeTab(page, 'Selected');
        await expect(
            page.getByRole('heading', { name: nodeName }),
        ).toBeVisible();
        await expect(page.getByText('Age', { exact: true })).toBeVisible();
    });
}

test('Expand via all entry points and restore via all hide paths', async ({
    page,
}) => {
    // Three expand/restore iterations stack up many waitForLayoutToFinish
    // calls (each with a 2s fixed sleep), so the default 30s isn't enough.
    test.setTimeout(60000);
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });

    const expectExpanded = async () => {
        await waitForLayoutToFinish(page);
        const state = await getGraphState(page);
        expect(new Set(state.nodes.map((n) => n.id))).toEqual(
            new Set(['Pedro', 'Ben', 'Hamza']),
        );
        expect(
            state.nodes.find((n) => n.id === 'Pedro')?.badgeText,
        ).toBeUndefined();
    };

    const expectCollapsed = async () => {
        await waitForLayoutToFinish(page);
        const state = await getGraphState(page);
        expect(state.nodes.map((n) => n.id)).toEqual(['Pedro']);
        const badge = state.nodes.find((n) => n.id === 'Pedro')?.badgeText;
        expect(badge).toBeDefined();
        expect(Number(badge)).not.toBeNaN();
    };

    const expandByDoubleClick = () => doubleClickOnNode(page, 'Pedro');

    const expandByContextMenu = async () => {
        await rightClickOnNode(page, 'Pedro');
        await page
            .getByRole('menuitem', { name: 'Expand', exact: true })
            .click();
    };

    const expandByFloatingExplore = async () => {
        await clickOnNode(page, 'Pedro');
        await page
            .getByRole('button', { name: 'Explore', exact: true })
            .click();
        await page
            .getByRole('menuitem', {
                name: 'Show all nodes directly connected to selection',
                exact: true,
            })
            .click();
    };

    const restoreByUndo = () =>
        page.getByRole('button', { name: 'Undo (⌘Z)', exact: true }).click();

    // fitView so the post-expand layout positions for Ben/Hamza land inside
    // the canvas viewport — otherwise clickOnNodes misses. Multi-select both
    // neighbours and delete in one shot to avoid stacking another round of
    // fitView + waitForLayoutToFinish sleeps. expectCollapsed handles the
    // trailing layout wait.
    const restoreByBackspace = async () => {
        await fitView(page);
        await clickOnNodes(page, ['Hamza', 'Ben']);
        await page.keyboard.press('Backspace');
    };

    const restoreByFloatingDelete = async () => {
        await fitView(page);
        await deleteNodes(page, ['Hamza', 'Ben']);
    };

    const paths: {
        expand: () => Promise<unknown>;
        restore: () => Promise<unknown>;
    }[] = [
        { expand: expandByDoubleClick, restore: restoreByUndo },
        { expand: expandByContextMenu, restore: restoreByBackspace },
        { expand: expandByFloatingExplore, restore: restoreByFloatingDelete },
    ];

    // Initial state: Pedro alone, degree badge shows hidden-neighbour count
    await expectCollapsed();

    for (const { expand, restore } of paths) {
        await expand();
        await expectExpanded();
        await restore();
        await expectCollapsed();
    }
});

test('Expanding a node shows the querying loading indicator', async ({
    page,
}) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await waitForLayoutToFinish(page);

    const queryingIndicator = page.getByRole('progressbar', {
        name: 'Querying for graph...',
    });
    await expect(queryingIndicator).toBeHidden();

    // Hold every GraphQL POST (the app's only POSTs) so the querying state
    // lasts long enough to observe — the local test server answers faster
    // than Playwright polls, so an undelayed expand could complete between
    // visibility checks and the assertion would be flaky.
    await page.route('**', async (route) => {
        if (route.request().method() !== 'POST') {
            return route.continue();
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
        return route.continue();
    });

    await doubleClickOnNode(page, 'Pedro');
    await expect(queryingIndicator).toBeVisible({ timeout: 10000 });

    // Release remaining requests promptly and confirm the indicator clears
    // and the expansion actually landed. `unrouteAll` with `ignoreErrors`
    // avoids the "Route is already handled" race with handlers still
    // sleeping in their delay when the route is removed.
    await page.unrouteAll({ behavior: 'ignoreErrors' });
    await waitForLayoutToFinish(page);
    const state = await getGraphState(page);
    expect(new Set(state.nodes.map((n) => n.id))).toEqual(
        new Set(['Pedro', 'Ben', 'Hamza']),
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
    expect(new Set((await getGraphState(page)).nodes.map((n) => n.id))).toEqual(
        new Set(['Pedro', 'Hamza', 'Ben']),
    );
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
    await expect(page.getByText('Madrid')).toBeVisible();
    await page.getByRole('button', { name: 'EDGE STATISTICS' }).click();
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await page.waitForTimeout(500);
    await getInteractiveCanvas(page).click();
    await page.waitForTimeout(100);
    await page.keyboard.down('Control');
    await page.waitForTimeout(100);
    await getInteractiveCanvas(page).press('a');
    await page.waitForTimeout(100);
    await page.keyboard.up('Control');
    await page.waitForTimeout(500);
    const state = await getGraphState(page);
    expect(new Set(state.selected)).toEqual(
        new Set(['None', 'Pedro', 'Ben', 'Hamza', 'Pometry']),
    );
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
    expect(new Set(state3.selected)).toEqual(
        new Set(['None', 'Pedro', 'Ben', 'Hamza', 'Pometry']),
    );
});

test('Click backspace to delete nodes', async ({ page }) => {
    await navigateToGraphPageBySearch(page, {
        type: 'node',
        nodeName: 'Pedro',
        nodeType: 'Person',
    });
    await doubleClickOnNode(page, 'Pedro');
    await waitForLayoutToFinish(page);
    await fitView(page);
    expect((await getGraphState(page)).nodes).toHaveLength(3);
    await clickOnNode(page, 'Hamza');
    await page.keyboard.press('Backspace');
    await waitForLayoutToFinish(page);
    expect((await getGraphState(page)).nodes).toHaveLength(2);
});

test('RHS Selected properties has max height for table cells', async ({
    page,
}) => {
    await navigateInSavedGraphs(page, {
        namespace: 'new_folder',
        graphName: 'persistent_second_filler',
    });
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
        const expectNodeStyles = async () => {
            const state = await getGraphState(page);
            expect(state.nodes.find((n) => n.id === 'Pedro')?.colour).toEqual(
                '#bd10e0',
            );
            expect(state.nodes.find((n) => n.id === 'Pedro')?.size).toEqual(30);
        };

        await style(
            page,
            { kind: 'node', name: 'Pedro' },
            { colourValue: 'BD10E0', size: 30 },
        );
        await expectNodeStyles();

        await saveAs(
            page,
            `${isolatedGraphs.namespace}/persistent_filler_styled`,
        );
        await expectNodeStyles();
        await expectStylingHexInput(page, 'BD10E0');

        await page.reload();
        await waitForLayoutToFinish(page);
        await expectNodeStyles();
        await expectStylingHex(page, { kind: 'node', name: 'Pedro' }, 'BD10E0');
    });
});

// FIXME: this is mostly duplicated with:
// - Change colour of edge by layer dropdown
// - Preview edge colour changes
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

        await style(
            page,
            {
                kind: 'edge',
                src: 'Judy',
                dst: 'Rabbit Inc',
                layer: 'advises',
            },
            { colourValue: 'F5A623' },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.waitForTimeout(2000);
        await expectStylingHexInput(page, 'F5A623');

        await page.reload();
        await waitForLayoutToFinish(page);
        await expectStylingHex(
            page,
            {
                kind: 'edge',
                src: 'Judy',
                dst: 'Rabbit Inc',
                layer: 'advises',
            },
            'F5A623',
        );
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

        await style(
            page,
            {
                kind: 'edge',
                src: 'Judy',
                dst: 'Rabbit Inc',
                layer: 'advises',
            },
            { colourValue: 'F5A623' },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await openTimeline(page);
        await page.waitForTimeout(5000);
        await expect(
            page
                .getByLabel('Edge ID ["Judy","Rabbit Inc","advises",100]')
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

        const expectNodeStyles = async () => {
            const state = await getGraphState(page);
            for (const id of ['Pedro', 'Hamza', 'Ben']) {
                expect(state.nodes.find((n) => n.id === id)?.colour).toEqual(
                    '#d0021b',
                );
                expect(state.nodes.find((n) => n.id === id)?.size).toEqual(30);
            }
        };

        await style(
            page,
            { kind: 'node-type', type: 'Person' },
            { colourValue: 'D0021B', size: 30 },
        );
        await expectNodeStyles();

        await saveAs(page, `${isolatedGraphs.namespace}/persistent_styled`);
        await expectNodeStyles();
        await expectStylingHexInput(page, 'D0021B');

        await page.reload();
        await waitForLayoutToFinish(page);
        await expectNodeStyles();
        await expectStylingHex(
            page,
            { kind: 'node-type', type: 'Person' },
            'D0021B',
        );
    });
});

test('Preview colour and size by type changes', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'second_filler',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_filler',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_second_filler',
    });
    await fitView(page);

    await clickOnEdge(page, 'Judy', 'Rabbit Inc');
    await changeTab(page, 'Styling');
    await expect(
        page.getByRole('combobox', { name: 'Edge Layer' }),
    ).toContainText('advises');
    await fillColorPickerHexInput(page, 'F5A623');
    await openTimeline(page);
    // Wait for the timeline to open fully
    await page.waitForTimeout(500);
    await expect(
        page
            .getByLabel('Edge ID ["Judy","Rabbit Inc","advises",100]')
            .locator('path'),
    ).toHaveCSS('fill', 'rgb(245, 166, 35)');
});

test('Layout Customizer Default Advanced Options', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_filler',
    });
    await changeTab(page, 'Layout');

    expect(await getInteractiveCanvas(page).screenshot()).toMatchSnapshot(
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
        slider: page.getByLabel('pull toward (0, 0)'),
        root: page.getByLabel('Gravity Slider Container'),
        sliderPosition: 0.5,
    });
    // Radial force sliders are gone — the sigma layout worker uses
    // d3-force without `d3-force-radial`, so the customizer no longer
    // exposes radial strength/radius.
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    expect(await getInteractiveCanvas(page).screenshot()).toMatchSnapshot(
        'layout-customizer-default-all-changed.png',
    );
});

test('Layout Customizer can change to concentric layout', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_filler',
    });
    await changeTab(page, 'Layout');

    await page.getByText('Default Layout').click();
    await page.getByRole('option', { name: 'Concentric Layout' }).click();

    await page.getByRole('checkbox', { name: 'Clockwise' }).check();
    await page.getByRole('checkbox', { name: 'Prevent overlap' }).check();
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
        slider: page.getByLabel('The angle (in pi radians) at which to start'),
        root: page.getByLabel('Start Angle (pi radians) Slider Container'),
        sliderPosition: 0.5,
    });
    await dragSlider({
        page,
        slider: page.getByLabel(
            'The angle (in pi radians) between the first and last node',
        ),
        root: page.getByLabel('Sweep Slider Container'),
        sliderPosition: 0.5,
    });
    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    await fitView(page);
    expect(await getInteractiveCanvas(page).screenshot()).toMatchSnapshot(
        'layout-customizer-concentric-all-changed.png',
    );
});

test('Layout Customizer can use dagre for pre-layout', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_filler',
    });
    await changeTab(page, 'Layout');

    // The sigma DagreLayoutCustomizer is intentionally minimal vs the G6 one:
    // only invert + nodesep + ranksep + edgesep. Alignment, Ranking algorithm,
    // Node size, and Control points dropped with the G6 cleanup.
    await page.getByText('Default Layout').click();
    await page.getByRole('option', { name: 'Hierarchical TD Layout' }).click();
    await page.locator('ul[role="listbox"]').waitFor({ state: 'detached' }); // wait for MUI Select portal to be fully removed from DOM (close animation completes)

    await page.getByRole('checkbox', { name: 'Invert direction' }).check();

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

    await page
        .getByRole('button', { name: 'Apply Layout', exact: true })
        .click();
    await waitForLayoutToFinish(page);
    expect(await getInteractiveCanvas(page).screenshot()).toMatchSnapshot(
        'layout-customizer-prelayout-dagre-all-changed.png',
    );
});

test('Brush select on main canvas works from first click', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent_filler',
    });

    // Box from None to Hamza in the current persistent_filler layout. Padding so
    // both node centres are inside the brush region; coordinates derived from
    // the live layout so the test isn't tied to a hardcoded layout. This pair
    // bounds a clean rectangle at the bottom of the canvas with no other nodes
    // inside, so the brush picks up exactly the two we want.
    const positions = await getNodePositions(page, ['None', 'Hamza']);
    const padding = 30;
    const left = Math.min(positions.None.x, positions.Hamza.x) - padding;
    const right = Math.max(positions.None.x, positions.Hamza.x) + padding;
    const top = Math.min(positions.None.y, positions.Hamza.y) - padding;
    const bottom = Math.max(positions.None.y, positions.Hamza.y) + padding;

    await page.keyboard.down('Shift');
    await page.mouse.move(right, top);
    await page.mouse.down();
    await page.waitForTimeout(100);
    await page.mouse.move(left, bottom);
    await page.mouse.up();
    await page.keyboard.up('Shift');
    const state = await getGraphState(page);
    expect(new Set(state.selected)).toEqual(new Set(['None', 'Hamza']));
});

test('Shift+click an already-selected node deselects it', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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

    await page.goto('/graph/vanilla/event');

    expect(consoleErrors, 'Console errors found').toStrictEqual([]);
    expect(consoleLogs, 'Console logs found').toStrictEqual([]);
});

// TODO: we should be using utils here...
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
        await style(
            page,
            { kind: 'node', name: 'Pedro' },
            { colourValue: 'BD10E0', size: 30 },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.waitForTimeout(2000);
        await expect(page.getByText('#bd10e0', { exact: true })).toBeVisible();
        await expect(page.getByText('30', { exact: true })).toBeVisible();
        // Save Person type styling
        await page.getByRole('button', { name: 'Selection' }).click();
        await page
            .getByRole('menuitem', { name: 'Clear current selection' })
            .click();
        await page.waitForTimeout(2000);
        await style(
            page,
            { kind: 'node-type', type: 'Person' },
            { colourValue: 'D0021B', size: 25 },
        );
        await save(page);
        await expect(page.getByText('#d0021b', { exact: true })).toBeVisible();
        await expect(page.getByText('25', { exact: true })).toBeVisible();
        // Save meets edge layer styling
        await style(
            page,
            { kind: 'edge', src: 'Ben', dst: 'Pedro', layer: 'meets' },
            { colourValue: 'F5A623' },
        );
        await save(page);
        await expect(
            page
                .locator('div')
                .filter({ hasText: /^Hex$/ })
                .getByRole('textbox'),
        ).toHaveValue('F5A623');
        // Delete Ben
        await deleteNodes(page, ['Ben']);
        // Delete None
        await deleteNodes(page, ['None']);
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
        await expect(
            page.getByRole('combobox', { name: 'Edge Layer' }),
        ).toContainText('founds');
        await fillInStyling(page, { colourValue: 'FF6B6B' });
        await openTimeline(page);
        await selectLayout(page, 'Arrange nodes in concentric circles');
        await fitView(page);
        await waitForLayoutToFinish(page);
        expect(await getInteractiveCanvas(page).screenshot()).toMatchSnapshot(
            'comprehensive-styling-selecting-highlighting-layout-saving.png',
        );

        // Save the graph (positions + styles persist to metadata)
        await save(page);
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

        // Switch to a different layout — should override the saved positions.
        // selectLayout matches on the menu item's tooltip text (which MUI
        // promotes to the accessible name), not the visible label.
        await selectLayout(
            page,
            'Physics-based layout with natural clustering',
        );
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

    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save as ...').click();
    await expect(page.getByLabel('New Graph Name')).toBeVisible();

    const newName = `${isolatedGraphs.namespace}/test_graph`;
    await page.getByLabel('New Graph Name').fill(newName);
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);

    await expect(page).toHaveURL(new RegExp(`/graph/${newName}`));
});

test('Right-clicking a node shows the context menu', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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

    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await rightClickOnNode(page, 'Hamza');
    await page.getByRole('menuitem', { name: 'Deselect all' }).click();

    const state = await getGraphState(page);
    expect(state.selected).toHaveLength(0);
});

test('Context menu invert selection', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Invert selection' }).click();

    const state = await getGraphState(page);
    expect(state.selected).not.toContain('Pedro');
    expect(state.selected.length).toEqual(4);
});

test('Context menu select all similar selects all nodes of the same type', async ({
    page,
}) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Delete' }).click();
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.selected).not.toContain('Pedro');
});

test('Context menu select related selects connected nodes', async ({
    page,
}) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await rightClickOnNode(page, 'Pedro');
    await page.getByRole('menuitem', { name: 'Open Trace Log' }).click();

    await expect(page.getByRole('tab', { name: 'Trace Log' })).toBeVisible();
});

test('After context menu opens trace log, switching to Connections tab works', async ({
    page,
}) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
        'Edge ID ["Ben","Pedro","meets",1679356800000]',
    );
    await expect(benPedroEdge.locator('circle[r="7"]').first()).toHaveCSS(
        'fill-opacity',
        '0.5',
    );
});

test('Context menu find shortest path between two nodes', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });

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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
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
    const canvas = getInteractiveCanvas(page);
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
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });

    // Collapse the RHS panel to maximize canvas area
    await page.getByRole('button', { name: 'Collapse panel' }).click();
    await page.waitForTimeout(300);

    const canvas = getInteractiveCanvas(page);
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

// TODO: remove, this is redundant
test('Save As creates a new graph and navigates to it', async ({
    page,
    isolatedGraphs,
}) => {
    await isolatedGraphs.navigateToGraph(page, 'event');

    // Open Save As dialog
    // TODO: use saveAs util here
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save as ...').click();
    await expect(page.getByText('New Graph Name')).toBeVisible();

    // Enter new graph name within the same namespace
    const newGraphName = `${isolatedGraphs.namespace}/saved_test`;
    await page.getByLabel('New Graph Name').fill(newGraphName);

    // Confirm
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);

    // Verify URL changed to new graph path
    await expect(page).toHaveURL(new RegExp(`/graph/${newGraphName}`));
});

// TODO: remove, this is redundant
test('Save As preserves graph changes after reload', async ({
    page,
    isolatedGraphs,
}) => {
    await isolatedGraphs.navigateToGraph(page, 'event');

    // Delete a node to create an unsaved change
    await deleteNodes(page, ['Ben']);

    // Verify Ben is gone
    let state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).not.toContain('Ben');

    // Save As to a new graph
    await saveAsWithRandomName(page, isolatedGraphs.namespace);

    // Reload the page
    await page.reload();
    await waitForLayoutToFinish(page);

    // Verify Ben is still gone after reload
    state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).not.toContain('Ben');
});

test.describe('Unsaved-changes navigation blocker', () => {
    // Both paths share the same setup: land on /graph with one node, expand
    // it (which marks the view as having unsaved changes), then click the
    // Explorations navbar link to trigger the confirmation dialog.
    async function setupExpandedGraphAndOpenDialog(
        page: import('@playwright/test').Page,
    ) {
        await navigateToGraphPageBySearch(page, {
            type: 'node',
            nodeName: 'Pedro',
            nodeType: 'Person',
        });
        await doubleClickOnNode(page, 'Pedro');
        await waitForLayoutToFinish(page);
        // Sanity check: the expansion brought Ben and Hamza in. If this ever
        // changes, the post-cancel state assertion below also needs updating.
        const expandedIds = (await getGraphState(page)).nodes
            .map((n) => n.id)
            .sort();
        expect(expandedIds).toEqual(['Ben', 'Hamza', 'Pedro']);

        await page
            .getByRole('link', { name: 'Explorations', exact: true })
            .click();
        await expect(
            page.getByRole('heading', { name: 'Confirm Navigation' }),
        ).toBeVisible();
    }

    test('Cancel keeps the user on the graph with the expanded nodes intact', async ({
        page,
    }) => {
        await setupExpandedGraphAndOpenDialog(page);
        const graphUrlBeforeCancel = page.url();

        await page.getByRole('button', { name: 'Cancel' }).click();
        await expect(
            page.getByRole('heading', { name: 'Confirm Navigation' }),
        ).toBeHidden();

        // URL must not have moved, and the expanded graph state must still
        // be there. Regression guard for the bug where cancelling the
        // dialog tore down and rebuilt the G6 graph, leaving the canvas
        // blank.
        expect(page.url()).toBe(graphUrlBeforeCancel);
        const stateAfterCancel = (await getGraphState(page)).nodes
            .map((n) => n.id)
            .sort();
        expect(stateAfterCancel).toEqual(['Ben', 'Hamza', 'Pedro']);
    });

    test('Proceed navigates away to /saved-graphs', async ({ page }) => {
        await setupExpandedGraphAndOpenDialog(page);

        await page.getByRole('button', { name: 'Proceed' }).click();
        await expect(page).toHaveURL(/\/saved-graphs$/);
    });
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
        const canvas = getInteractiveCanvas(page);
        await canvas.hover({ position: pedroPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(pedroPos.x + 80, pedroPos.y + 80, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);

        // Save As to preserve positions
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await waitForLayoutToFinish(page);

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
// vanilla/filler has a Person icon set with ?color=white in the iconify URL.
// These tests verify the icon URL is plumbed all the way to the renderer
// and to the temporal view.

test.describe('icon colours', () => {
    test('graph view node icons carry the ?color=white iconify URL', async ({
        page,
    }) => {
        await navigateInSavedGraphs(page, {
            namespace: 'vanilla',
            graphName: 'filler',
        });
        await waitForLayoutToFinish(page);
        await fitView(page);
        await waitForLayoutToFinish(page);

        // Sigma's graphology stores the icon URL as the `image` node
        // attribute (set by SigmaScene's reconcile step from the styled iconSrc). Find
        // the Ben node by its sigma `label`, then check the `image` URL.
        interface SigmaInstance {
            graph: {
                nodes(): string[];
                getNodeAttribute(id: string, key: string): unknown;
            };
        }
        await page.waitForFunction(
            () => {
                const w = window as Window & {
                    __SIGMA__?: SigmaInstance;
                };
                const sigma = w.__SIGMA__;
                if (sigma === undefined) return false;
                const benId = sigma.graph
                    .nodes()
                    .find(
                        (id) =>
                            sigma.graph.getNodeAttribute(id, 'label') === 'Ben',
                    );
                if (benId === undefined) return false;
                const image = sigma.graph.getNodeAttribute(benId, 'image');
                return (
                    typeof image === 'string' && image.includes('color=white')
                );
            },
            { timeout: 15000 },
        );
    });

    test('temporal view Y-axis icons have ?color=white in their href', async ({
        page,
    }) => {
        await navigateInSavedGraphs(page, {
            namespace: 'vanilla',
            graphName: 'filler',
        });
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
        await navigateInSavedGraphs(page, {
            namespace: 'vanilla',
            graphName: 'filler',
        });
        await waitForLayoutToFinish(page);
        await fitView(page);
        await waitForLayoutToFinish(page);
        // waitForLayoutToFinish includes a 2s pause, which is enough for
        // useRecoloredIconSrcs canvas compositing to complete.
        const pos = await getNodePositions(page, ['Ben']);
        const box = await getInteractiveCanvas(page).boundingBox();
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
        await navigateInSavedGraphs(page, {
            namespace: 'vanilla',
            graphName: 'filler',
        });
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

// Regression tests for commit ab1ff59 (Persist node-type styles + positions on
// the saved-graph, not master). Two protections:
//
//   1. The type-styling panel's Save button now persists both styles AND
//      current node positions in one action. Earlier the panel only wrote
//      styles, so any drag the user performed before clicking Save was lost
//      on reload.
//
//   2. `onSaveGraph` (Save-As to the same path / navbar Save changes) takes a
//      "topology unchanged" branch that skips the destructive
//      `createSubgraph({ overwrite: true })`. Earlier every in-place save
//      rebuilt the saved-graph from master and wiped its `_style.node_types`
//      graph metadata + per-node `_pos` metadata, so a saved type colour
//      would silently revert to master defaults after the next reload.
test.describe('Regression: persist styles + positions on the saved-graph', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Type panel Save commits both the type colour and the current dragged positions', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        const positionsBefore = await getNodePositions(page, ['Pedro']);
        const pedroPos = positionsBefore['Pedro'];

        // Drag Pedro before saving — the type-panel Save handler is what
        // bundles the position-write into the same action.
        const canvas = getInteractiveCanvas(page);
        await canvas.hover({ position: pedroPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(pedroPos.x + 80, pedroPos.y + 80, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);

        // Set a Person-type colour and click Save in the styling panel.
        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'Person' }).click();
        await fillInStyling(page, { colourValue: 'BD10E0' });
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.waitForTimeout(2000);

        // Reload and verify both pieces of state survived.
        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        const stateAfter = await getGraphState(page);
        expect(stateAfter.nodes.find((n) => n.id === 'Pedro')?.colour).toEqual(
            '#bd10e0',
        );

        const positionsAfter = await getNodePositions(page, ['Pedro']);
        const dx = Math.abs(
            positionsAfter['Pedro'].x - positionsBefore['Pedro'].x,
        );
        const dy = Math.abs(
            positionsAfter['Pedro'].y - positionsBefore['Pedro'].y,
        );
        expect(dx + dy).toBeGreaterThan(10);
    });
});

test.describe("Regression: Save-As to current path doesn't wipe saved-graph metadata", () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('A second save over the same path preserves an earlier type-style write', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        // Step 1: persist a Person-type colour, then save as a new graph.
        await style(
            page,
            { kind: 'node-type', type: 'Person' },
            { colourValue: 'BD10E0' },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);

        // Sanity: the type colour is on the canvas before the second save.
        const stateBetween = await getGraphState(page);
        expect(
            stateBetween.nodes.find((n) => n.id === 'Pedro')?.colour,
        ).toEqual('#bd10e0');

        // Step 2: make a change (style Hamza individually) so the save button is enabled,
        // then save in-place. The second save must not wipe the type-style metadata.
        await style(
            page,
            { kind: 'node', name: 'Hamza' },
            { colourValue: '4A90D9' },
        );
        await save(page);
        await page.reload();
        await waitForLayoutToFinish(page);
        const stateAfter = await getGraphState(page);
        expect(stateAfter.nodes.find((n) => n.id === 'Pedro')?.colour).toEqual(
            '#bd10e0',
        );
    });

    test('A second save over the same path preserves an earlier dragged position', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        const positionsBefore = await getNodePositions(page, ['Pedro']);
        const pedroPos = positionsBefore['Pedro'];

        // Step 1: drag Pedro, then Save-As to the current path so the
        // dragged position lands on the saved-graph.
        const canvas = getInteractiveCanvas(page);
        await canvas.hover({ position: pedroPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(pedroPos.x + 80, pedroPos.y + 80, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);

        await saveAsWithRandomName(page, isolatedGraphs.namespace);

        // Step 2: drag a different node and Save-As to the current path
        // again. Without the fix, the second `createSubgraph` overwrite
        // would have wiped Pedro's `_pos` metadata.
        const positionsMid = await getNodePositions(page, ['Hamza']);
        const hamzaPos = positionsMid['Hamza'];
        await canvas.hover({ position: hamzaPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(hamzaPos.x + 60, hamzaPos.y - 60, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);
        await save(page);

        // Step 3: reload, fitView, and verify Pedro is no longer at his
        // pre-drag position. fitView normalises the viewport but the
        // relative displacement from the original layout output should
        // still be material.
        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        const positionsAfter = await getNodePositions(page, ['Pedro']);
        const dx = Math.abs(
            positionsAfter['Pedro'].x - positionsBefore['Pedro'].x,
        );
        const dy = Math.abs(
            positionsAfter['Pedro'].y - positionsBefore['Pedro'].y,
        );
        expect(dx + dy).toBeGreaterThan(10);
    });
});

test.describe('Regression: switching layouts after a saved layout actually applies', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Picking concentric after a save with persisted positions changes the layout', async ({
        page,
        isolatedGraphs,
    }) => {
        test.setTimeout(60000);
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        // Save dragged positions so the saved-graph has `_pos` metadata
        // for every node — this is the precondition that previously made
        // every subsequent layout switch a no-op (saved-graph positions
        // clobbered the layout output in the merge).
        const positionsBeforeDrag = await getNodePositions(page, ['Pedro']);
        const pedroPos = positionsBeforeDrag['Pedro'];
        const canvas = getInteractiveCanvas(page);
        await canvas.hover({ position: pedroPos });
        await page.mouse.down();
        await page.waitForTimeout(200);
        await page.mouse.move(pedroPos.x + 60, pedroPos.y + 60, { steps: 5 });
        await page.mouse.up();
        await page.waitForTimeout(500);

        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        const positionsBefore = await getNodePositions(page, [
            'Pedro',
            'Hamza',
        ]);

        // Pick a different layout. The user-visible bug was that this did
        // nothing — `persistedPositions` (saved-graph `_pos`) overrode the
        // freshly-computed layout output every render, so the canvas never
        // changed. The fix seeds saved positions into local state once and
        // lets `userDraggedPositions`'s clear-on-layout-change semantics
        // drop them when the user explicitly picks a layout.
        await selectLayout(page, 'Arrange nodes in concentric circles');
        await page.waitForTimeout(1000);
        await fitView(page);

        const positionsAfter = await getNodePositions(page, ['Pedro', 'Hamza']);

        // Concentric should reposition at least one of the nodes by more
        // than a fitView-tolerance amount. Loose threshold so we don't
        // overfit to specific layout output.
        const totalDelta =
            Math.abs(positionsAfter['Pedro'].x - positionsBefore['Pedro'].x) +
            Math.abs(positionsAfter['Pedro'].y - positionsBefore['Pedro'].y) +
            Math.abs(positionsAfter['Hamza'].x - positionsBefore['Hamza'].x) +
            Math.abs(positionsAfter['Hamza'].y - positionsBefore['Hamza'].y);
        expect(totalDelta).toBeGreaterThan(20);
    });
});

// Saved-graph layout persistence: picking a non-default layout, saving, and
// refreshing must keep that layout. Earlier the saved-graph stored only `_pos`
// and `_style`, so a refresh always reset to DEFAULT_LAYOUT (force) — and the
// next expansion redrew in force regardless of what the user had picked.
test.describe('Regression: saved layout type survives refresh', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/persistent'] });

    test('Concentric layout chosen + saved is still concentric after reload', async ({
        page,
        isolatedGraphs,
    }) => {
        test.setTimeout(60000);
        await isolatedGraphs.navigateToGraph(page, 'persistent');
        await fitView(page);

        await selectLayout(page, 'Arrange nodes in concentric circles');
        await waitForLayoutToFinish(page);
        await fitView(page);

        const positionsConcentric = await getNodePositions(page, [
            'Pedro',
            'Hamza',
        ]);

        await saveAsWithRandomName(page, isolatedGraphs.namespace);

        await page.reload();
        await waitForLayoutToFinish(page);
        await fitView(page);

        const positionsAfterReload = await getNodePositions(page, [
            'Pedro',
            'Hamza',
        ]);

        for (const node of Object.keys(positionsConcentric)) {
            const drift =
                Math.abs(
                    positionsAfterReload[node].x - positionsConcentric[node].x,
                ) +
                Math.abs(
                    positionsAfterReload[node].y - positionsConcentric[node].y,
                );
            expect(drift, `node "${node}" drifted too far`).toBeLessThan(20);
        }
    });
});
