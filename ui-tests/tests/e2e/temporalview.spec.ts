import assert from 'assert';

import { expect } from '@playwright/test';

import { test } from '../fixtures';
import {
    expectStylingHex,
    expectStylingHexInput,
    fillColorPickerHexInput,
    saveAsWithRandomName,
    setupGraphPage,
    style,
} from './graph.utils';
import { navigateInSavedGraphs } from './saved-graphs.utils';
import {
    getTemporalViewData,
    getYAxisLabelStripRightEdge,
    getYAxisRows,
    hoverEdgeAndExpectTooltip,
    openTimeline,
    pinYAxisNode,
    readRawEdges,
    selectYAxisNode,
    turnFilterOff,
    turnFilterOn,
} from './temporalview.utils';
import { waitForLayoutToFinish } from './utils';

test('Close temporal view button and open again', async ({ page }) => {
    await setupGraphPage(page);
    await page.getByRole('button', { name: 'Close timeline' }).click();
    await expect(page.locator('text="Ben"')).toBeHidden();
    await openTimeline(page);
    await expect(page.locator('text="Ben"')).toBeVisible();
});

test('Temporal view hover over edges', async ({ page }) => {
    await setupGraphPage(page);

    // The edge sensor (last child of each edge group) is the invisible
    // wide line that listens to mouse events. Use the edge group's
    // aria-label rather than g:nth-child(N) so the test does not rely on
    // the DOM order of edges, which depends on raphtory iteration order.
    const sensor = (id: string) => `[aria-label='Edge ID ${id}'] line:last-child`;

    await hoverEdgeAndExpectTooltip(page, sensor('["Ben","Hamza","meets",1671667200000]'), [
        'Ben → Hamza',
        'meets',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Ben","Pedro","meets",1679356800000]'), [
        'Ben → Pedro',
        'meets',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Hamza","Pometry","founds",1687132800000]'), [
        'Hamza → Pometry',
        'founds',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Hamza","Pedro","meets",1689734400000]'), [
        'Hamza → Pedro',
        'meets',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Hamza","Pedro","meets",1697424000000]'), [
        'Hamza → Pedro',
        'meets',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Hamza","Pedro","transfers",1705017600000]'), [
        'Hamza → Pedro',
        'transfers',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Pedro","Hamza","transfers",1707609600000]'), [
        'Pedro → Hamza',
        'transfers',
    ]);
    await hoverEdgeAndExpectTooltip(page, sensor('["Ben","Hamza","transfers",1710115200000]'), [
        'Ben → Hamza',
        'transfers',
    ]);
});

test('Pin node and highlight', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await openTimeline(page);

    await pinYAxisNode(page, 'Pometry');

    const pometryY = (await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .first()
        .boundingBox())!.y;
    const benY = (await page.locator('g').filter({ hasText: /^Ben$/ }).first().boundingBox())!.y;
    expect(pometryY).toBeLessThan(benY);

    await selectYAxisNode(page, 'Pometry');
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Pometry', exact: true })).toBeVisible();
});

test('Zoom into timeline view', async ({ page }) => {
    await setupGraphPage(page);
    await page.waitForSelector('text="Pometry"');

    const element = page.locator('#temporal-view');
    await expect(element).toBeVisible();
    const box = await element.boundingBox();
    assert(box !== null);
    const offsetX = box.width / 2;
    const offsetY = box.height / 2;

    await page.mouse.move(box.x + offsetX, box.y + offsetY);
    await page.mouse.wheel(0, -2000); // scroll up (zoom in)
    await expect(page.getByText('Pedro')).toBeHidden();
});

test('Temporal view renders off-screen overscan margin so panning does not reveal blanks', async ({
    page,
}) => {
    await setupGraphPage(page);
    await page.waitForSelector('text="Pometry"');

    const element = page.locator('#temporal-view');
    await expect(element).toBeVisible();
    const box = await element.boundingBox();
    assert(box !== null);

    // Zoom into the dense Hamza↔Pedro cluster (right of centre), then pan
    // left so its earliest edge slides just outside the visible window.
    // Before #966 that edge was culled and the strip it vacated rendered
    // blank until the pan committed; the overscan margin keeps it drawn so
    // the reveal is seamless.
    await page.mouse.move(box.x + box.width * 0.8, box.y + box.height / 2);
    await page.mouse.wheel(0, -300);

    const cx = box.x + box.width / 2;
    const cy = box.y + box.height / 2;
    await page.mouse.move(cx, cy);
    await page.mouse.down();
    await page.mouse.move(cx - 60, cy, { steps: 5 });
    await page.mouse.up();

    // The y-axis label strip sits entirely left of the visible plot, so an
    // edge drawn left of its right edge is unambiguously in the overscan
    // margin — off-screen, pre-rendered for a smooth pan.
    const labelStripRightEdge = await getYAxisLabelStripRightEdge(page);
    const rows = new Set((await getYAxisRows(page)).map((r) => r.name));
    const edges = await readRawEdges(page);

    const marginEdges = edges.filter((e) => e.screenX < labelStripRightEdge);
    expect(marginEdges.length).toBeGreaterThan(0);

    // Every margin edge must connect two on-axis rows: an overscan edge to
    // an off-window-only node has no row to attach to and must not render.
    for (const edge of marginEdges) {
        const [src, dst] = JSON.parse(edge.ariaLabel.replace(/^Edge ID /, '')) as [
            string,
            string,
            string,
            number,
        ];
        expect(rows.has(src)).toBe(true);
        expect(rows.has(dst)).toBe(true);
    }
});

test('Highlight node from timeline view', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await openTimeline(page);
    await selectYAxisNode(page, 'Ben');
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Ben', exact: true })).toBeVisible();
    await expect(page.getByText('PROPERTIES')).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
    await expect(page.getByText('30', { exact: true })).toBeVisible();
    await selectYAxisNode(page, 'Hamza', { modifiers: ['Shift'] });
    await expect(page.getByRole('heading', { name: 'Hamza', exact: true })).toBeVisible();
    await expect(page.getByText('PROPERTIES')).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
    await expect(page.getByText('30', { exact: true })).toBeVisible();
});

test('Click prop event selects its row node', async ({ page }) => {
    // Uses the `temporal_props` saved graph because it seeds explicit
    // temporal property updates per node — the other vanilla seeds set
    // properties only at node creation, which Raphtory exposes as
    // single events that don't always render as visible PropEventItems.
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'temporal_props',
    });
    await openTimeline(page);

    // First prop event on Ben's row (regex prefix-match — actual key
    // depends on the saved graph's prop names/timestamps/values).
    const benPropEvent = page.getByLabel(/^Select node via event trace: Ben-/).first();
    const hamzaPropEvent = page.getByLabel(/^Select node via event trace: Hamza-/).first();

    // Step 1: plain click selects the row's node.
    await benPropEvent.click();
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(page.getByRole('heading', { name: 'Ben', exact: true })).toBeVisible();

    // Step 2: shift-click adds another node to the selection.
    await hamzaPropEvent.click({ modifiers: ['Shift'] });
    await expect(page.getByRole('heading', { name: 'Hamza', exact: true })).toBeVisible();
    // Ben must still be selected. The Selected RHS panel should still
    // contain Ben's heading reachable via the panel's navigation; the
    // simplest cross-check is that the y-axis Ben circle is in its
    // selected (non-grey) colour. YAxis paints unselected circles
    // grey and selected circles in the node's own style fill.
    const benCircle = page
        .locator('#temporal-view g')
        .filter({ hasText: /^Ben$/ })
        .locator('circle')
        .first();
    const benFillAfterShift = await benCircle.evaluate((el) => window.getComputedStyle(el).fill);
    // Grey would be one of MUI's grey shades; the node's own fill
    // will be a non-grey colour. Assert it is *not* the unselected
    // grey palette colour. (rgb(158, 158, 158) is grey[500];
    // rgb(189, 189, 189) is grey[400].)
    expect(['rgb(158, 158, 158)', 'rgb(107, 103, 112)']).not.toContain(benFillAfterShift);

    const hamzaCircle = page
        .locator('#temporal-view g')
        .filter({ hasText: /^Hamza$/ })
        .locator('circle')
        .first();
    const fillOf = async (locator: typeof hamzaCircle) =>
        await locator.evaluate((el) => window.getComputedStyle(el).fill);
    // Unselected y-axis circles use the theme's defaultNodeColor:
    // grey[500] when nothing is selected anywhere, grey[400] when at
    // least one node is selected. MUI's default grey[500] is
    // rgb(158, 158, 158); Pometry's custom theme overrides grey[400]
    // to #6b6770 = rgb(107, 103, 112).
    const greys = ['rgb(158, 158, 158)', 'rgb(107, 103, 112)'];

    // Step 3: plain click on an already-selected node deselects only
    // it, leaving other selections intact. Use expect.poll because the
    // React re-render that updates the circle's fill is not awaited
    // by Playwright's click().
    await hamzaPropEvent.click();
    await expect
        .poll(() => fillOf(hamzaCircle))
        .toMatch(/rgb\(158, 158, 158\)|rgb\(107, 103, 112\)/);
    // Ben still selected.
    const benFillAfterHamzaDeselect = await fillOf(benCircle);
    expect(greys).not.toContain(benFillAfterHamzaDeselect);

    // Step 4: plain click on a different unselected node replaces
    // the selection (deselectAll then selectNodes).
    await hamzaPropEvent.click();
    await expect.poll(() => fillOf(benCircle)).toMatch(/rgb\(158, 158, 158\)|rgb\(107, 103, 112\)/);
    const hamzaFillAfterReplace = await fillOf(hamzaCircle);
    expect(greys).not.toContain(hamzaFillAfterReplace);
});

test('Filter selected hides non-neighbour nodes from y-axis', async ({ page }) => {
    await setupGraphPage(page);
    await page.waitForSelector('text="Pometry"');

    const yAxisName = (name: string) =>
        page.locator('#temporal-view').getByText(name, { exact: true });

    await expect(yAxisName('Ben')).toBeVisible();
    await expect(yAxisName('Hamza')).toBeVisible();
    await expect(yAxisName('Pedro')).toBeVisible();
    await expect(yAxisName('Pometry')).toBeVisible();
    await selectYAxisNode(page, 'Pometry');

    await turnFilterOn(page);

    await expect(yAxisName('Pometry')).toBeVisible();
    await expect(yAxisName('Ben')).toBeVisible();
    await expect(yAxisName('Hamza')).toBeVisible();
    await expect(yAxisName('Pedro')).toBeHidden();

    await turnFilterOff(page);

    await expect(yAxisName('Pedro')).toBeVisible();
});

test('Preview colour of edge on timeline view', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await openTimeline(page);

    await page.getByLabel('Edge ID ["Ben","Pedro","meets",1679356800000]').click();
    await page.getByRole('tab', { name: 'Styling' }).click();
    await fillColorPickerHexInput(page, 'F5A623');
    await page.waitForTimeout(2000);

    await expect(
        page.getByLabel('Edge ID ["Ben","Pedro","meets",1679356800000]').locator('path').first(),
    ).toHaveCSS('fill', 'rgb(245, 166, 35)');
});

test.describe('Change colour of edge on timeline view', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/filler'] });

    test('Change colour of edge on timeline view', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'filler');
        await openTimeline(page);

        await style(
            page,
            {
                kind: 'edge-instance',
                label: 'Edge ID ["Ben","Pedro","meets",50]',
            },
            { colourValue: 'F5A623' },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.waitForTimeout(2000);
        await expect(
            page.getByLabel('Edge ID ["Ben","Pedro","meets",50]').locator('path').first(),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
    });
});

test.describe('Change colour only of exploded edge persists after save', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/filler'] });

    test('Change colour only of exploded edge persists after save', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'filler');
        await openTimeline(page);

        await style(
            page,
            {
                kind: 'edge-instance',
                label: 'Edge ID ["Ben","Pedro","meets",50]',
            },
            { colourValue: 'F5A623' },
        );
        await saveAsWithRandomName(page, isolatedGraphs.namespace);
        await page.waitForTimeout(2000);
        await expect(
            page.getByLabel('Edge ID ["Ben","Pedro","meets",50]').locator('path').first(),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
        await expectStylingHexInput(page, 'F5A623');

        await page.reload();
        await waitForLayoutToFinish(page);
        await openTimeline(page);
        await expectStylingHex(
            page,
            {
                kind: 'edge-instance',
                label: 'Edge ID ["Ben","Pedro","meets",50]',
            },
            'F5A623',
        );
        await expect(
            page.getByLabel('Edge ID ["Ben","Pedro","meets",50]').locator('path').first(),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
    });
});

test('Property events render at the correct node row', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'temporal_props',
    });
    await openTimeline(page);

    const { properties } = await getTemporalViewData(page);

    const sorted = [...properties].sort(
        (a, b) => a.node.localeCompare(b.node) || a.time.getTime() - b.time.getTime(),
    );
    expect(sorted.map(({ node, key, value }) => ({ node, key, value }))).toEqual([
        { node: 'Ben', key: 'STATUS', value: 'joined' },
        { node: 'Ben', key: 'STATUS', value: 'active' },
        { node: 'Ben', key: 'STATUS', value: 'promoted' },
        { node: 'Hamza', key: 'STATUS', value: 'joined' },
        { node: 'Hamza', key: 'STATUS', value: 'active' },
    ]);

    // Each event's time is interpolated from its rendered x-position against the
    // axis ticks, so it validates where the event is drawn — not d3's internal
    // datum. Allow a few days' slack for pixel rounding; the smallest gap
    // between distinct event times here is ~89 days, so this stays unambiguous.
    const DAY_MS = 24 * 60 * 60 * 1000;
    const expectedTimes = [
        1648598400000, // Ben joined
        1656288000000, // Ben active
        1663977600000, // Ben promoted
        1648598400000, // Hamza joined
        1663977600000, // Hamza active
    ];
    sorted.forEach((p, i) => {
        expect(
            Math.abs(p.time.getTime() - expectedTimes[i]),
            `time mismatch for ${p.node}/${p.value}`,
        ).toBeLessThanOrEqual(5 * DAY_MS);
    });
});

test('Edges render with src/dst at the correct node rows', async ({ page }) => {
    await setupGraphPage(page);

    const { edges } = await getTemporalViewData(page);

    const sorted = [...edges].sort((a, b) => a.time.getTime() - b.time.getTime());
    expect(sorted.map(({ src, dst, layer }) => ({ src, dst, layer }))).toEqual([
        { src: 'Ben', dst: 'Hamza', layer: 'meets' },
        { src: 'Ben', dst: 'Pedro', layer: 'meets' },
        { src: 'Ben', dst: 'Pometry', layer: 'founds' },
        { src: 'Hamza', dst: 'Pometry', layer: 'founds' },
        { src: 'Hamza', dst: 'Pedro', layer: 'meets' },
        { src: 'Hamza', dst: 'Pedro', layer: 'meets' },
        { src: 'Hamza', dst: 'Pedro', layer: 'transfers' },
        { src: 'Pedro', dst: 'Hamza', layer: 'transfers' },
        { src: 'Ben', dst: 'Hamza', layer: 'transfers' },
    ]);
});
