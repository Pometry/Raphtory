import { expect } from '@playwright/test';
import assert from 'assert';
import { test } from '../fixtures';
import {
    expectStylingHex,
    expectStylingHexInput,
    fillColorPickerHexInput,
    hoverEdgeAndExpectTooltip,
    openTimeline,
    setupGraphPage,
    styleAndSave,
} from './graph.utils';
import { navigateInSavedGraphs } from './saved-graphs.utils';
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
    const sensor = (id: string) =>
        `[aria-label="Edge ID ${id}"] line:last-child`;

    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Ben->Hamza_meets_1671667200000'),
        ['Ben → Hamza', 'meets'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Ben->Pedro_meets_1679356800000'),
        ['Ben → Pedro', 'meets'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Hamza->Pometry_founds_1687132800000'),
        ['Hamza → Pometry', 'founds'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Hamza->Pedro_meets_1689734400000'),
        ['Hamza → Pedro', 'meets'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Hamza->Pedro_meets_1697424000000'),
        ['Hamza → Pedro', 'meets'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Hamza->Pedro_transfers_1705017600000'),
        ['Hamza → Pedro', 'transfers'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Pedro->Hamza_transfers_1707609600000'),
        ['Pedro → Hamza', 'transfers'],
    );
    await hoverEdgeAndExpectTooltip(
        page,
        sensor('Ben->Hamza_transfers_1710115200000'),
        ['Ben → Hamza', 'transfers'],
    );
});

test('Pin node and highlight', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await openTimeline(page);

    await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .locator('image')
        .click();

    const pometryY = (await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .first()
        .boundingBox())!.y;
    const benY = (await page
        .locator('g')
        .filter({ hasText: /^Ben$/ })
        .first()
        .boundingBox())!.y;
    expect(pometryY).toBeLessThan(benY);

    await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .locator('circle')
        .click();
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(
        page.getByRole('heading', { name: 'Pometry', exact: true }),
    ).toBeVisible();
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

test('Highlight node from timeline view', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'event',
    });
    await openTimeline(page);
    await page
        .locator('g')
        .filter({ hasText: /^Ben$/ })
        .locator('circle')
        .click();
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(
        page.getByRole('heading', { name: 'Ben', exact: true }),
    ).toBeVisible();
    await expect(page.getByText('PROPERTIES')).toBeVisible();
    await expect(page.getByText('Age', { exact: true })).toBeVisible();
    await expect(page.getByText('30', { exact: true })).toBeVisible();
    await page
        .locator('g')
        .filter({ hasText: /^Hamza$/ })
        .locator('circle')
        .click({
            modifiers: ['Shift'],
        });
    await expect(
        page.getByRole('heading', { name: 'Hamza', exact: true }),
    ).toBeVisible();
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
    const benPropEvent = page
        .getByLabel(/^Select node via event trace: Ben-/)
        .first();
    const hamzaPropEvent = page
        .getByLabel(/^Select node via event trace: Hamza-/)
        .first();

    // Step 1: plain click selects the row's node.
    await benPropEvent.click();
    await page.getByRole('tab', { name: 'Selected' }).click();
    await expect(
        page.getByRole('heading', { name: 'Ben', exact: true }),
    ).toBeVisible();

    // Step 2: shift-click adds another node to the selection.
    await hamzaPropEvent.click({ modifiers: ['Shift'] });
    await expect(
        page.getByRole('heading', { name: 'Hamza', exact: true }),
    ).toBeVisible();
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
    const benFillAfterShift = await benCircle.evaluate(
        (el) => window.getComputedStyle(el).fill,
    );
    // Grey would be one of MUI's grey shades; the node's own fill
    // will be a non-grey colour. Assert it is *not* the unselected
    // grey palette colour. (rgb(158, 158, 158) is grey[500];
    // rgb(189, 189, 189) is grey[400].)
    expect(['rgb(158, 158, 158)', 'rgb(107, 103, 112)']).not.toContain(
        benFillAfterShift,
    );

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
    await expect
        .poll(() => fillOf(benCircle))
        .toMatch(/rgb\(158, 158, 158\)|rgb\(107, 103, 112\)/);
    const hamzaFillAfterReplace = await fillOf(hamzaCircle);
    expect(greys).not.toContain(hamzaFillAfterReplace);
});

test('Filter selected hides non-neighbour nodes from y-axis', async ({
    page,
}) => {
    await setupGraphPage(page);
    await page.waitForSelector('text="Pometry"');

    const yAxisName = (name: string) =>
        page.locator('#temporal-view').getByText(name, { exact: true });

    await expect(yAxisName('Ben')).toBeVisible();
    await expect(yAxisName('Hamza')).toBeVisible();
    await expect(yAxisName('Pedro')).toBeVisible();
    await expect(yAxisName('Pometry')).toBeVisible();

    // Pometry only connects to Hamza, so it is not a neighbour of Ben.
    await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .locator('circle')
        .click();

    await page.getByRole('button', { name: 'Turn filter on' }).click();

    await expect(yAxisName('Pometry')).toBeVisible();
    await expect(yAxisName('Ben')).toBeVisible();
    await expect(yAxisName('Hamza')).toBeVisible();
    await expect(yAxisName('Pedro')).toBeHidden();

    await page.getByRole('button', { name: 'Turn filter off' }).click();

    await expect(yAxisName('Pedro')).toBeVisible();
});

test('Preview colour of edge on timeline view', async ({ page }) => {
    await navigateInSavedGraphs(page, {
        namespace: 'vanilla',
        graphName: 'persistent',
    });
    await openTimeline(page);

    await page.getByLabel('Edge ID Ben->Pedro_meets_1679356800000').click();
    await page.getByRole('tab', { name: 'Styling' }).click();
    await fillColorPickerHexInput(page, 'F5A623');
    await page.waitForTimeout(2000);

    await expect(
        page
            .getByLabel('Edge ID Ben->Pedro_meets_1679356800000')
            .locator('path')
            .first(),
    ).toHaveCSS('fill', 'rgb(245, 166, 35)');
});

test.describe('Change colour of edge on timeline view', () => {
    test.use({ isolatedGraphsConfig: ['vanilla/filler'] });

    test('Change colour of edge on timeline view', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'filler');
        await openTimeline(page);

        await styleAndSave(
            page,
            { kind: 'edge-instance', label: 'Edge ID Ben->Pedro_meets_50' },
            { colourValue: 'F5A623' },
            'Save edge styles',
        );
        await page.waitForTimeout(2000);
        await expect(
            page
                .getByLabel('Edge ID Ben->Pedro_meets_50')
                .locator('path')
                .first(),
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

        await styleAndSave(
            page,
            { kind: 'edge-instance', label: 'Edge ID Ben->Pedro_meets_50' },
            { colourValue: 'F5A623' },
            'Save edge styles',
        );
        await page.waitForTimeout(2000);
        await expect(
            page
                .getByLabel('Edge ID Ben->Pedro_meets_50')
                .locator('path')
                .first(),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
        await expectStylingHexInput(page, 'F5A623');

        await page.reload();
        await waitForLayoutToFinish(page);
        await openTimeline(page);
        await expectStylingHex(
            page,
            { kind: 'edge-instance', label: 'Edge ID Ben->Pedro_meets_50' },
            'F5A623',
        );
        await expect(
            page
                .getByLabel('Edge ID Ben->Pedro_meets_50')
                .locator('path')
                .first(),
        ).toHaveCSS('fill', 'rgb(245, 166, 35)');
    });
});
