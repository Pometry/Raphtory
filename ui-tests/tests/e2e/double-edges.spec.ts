import { expect, test } from '../fixtures';
import {
    changeTab,
    clickOnEdge,
    fillInStyling,
    fitView,
    getGraphState,
    waitForSigmaEdgeLabel,
} from './graph.utils';
import { waitForLayoutToFinish } from './utils';

test.describe('double-ended edges', () => {
    test.use({ isolatedGraphsConfig: ['new_folder/persistent_filler'] });

    test('each direction selects independently', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnEdge(page, 'Ben', 'Pedro');
        let state = await getGraphState(page);
        expect(state.selectedEdges).toEqual(['Ben->Pedro']);

        await clickOnEdge(page, 'Pedro', 'Ben');
        state = await getGraphState(page);
        expect(state.selectedEdges).toEqual(['Pedro->Ben']);
    });

    test('directions style independently and both render', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnEdge(page, 'Ben', 'Pedro');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: 'D0021B' });

        await clickOnEdge(page, 'Pedro', 'Ben');
        await changeTab(page, 'Styling');
        await fillInStyling(page, { colourValue: '4A90D9' });

        const state = await getGraphState(page);
        const forward = state.edges.find((e) => e.source === 'Ben' && e.target === 'Pedro');
        const backward = state.edges.find((e) => e.source === 'Pedro' && e.target === 'Ben');
        expect(forward?.colour?.toLowerCase()).toContain('d0021b');
        expect(backward?.colour?.toLowerCase()).toContain('4a90d9');
    });

    test('each direction carries its own label', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnEdge(page, 'Ben', 'Pedro');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('in {where}');
        await waitForSigmaEdgeLabel(page, 'Ben->Pedro', 'in Madrid');

        await clickOnEdge(page, 'Pedro', 'Ben');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('back in {where}');
        await waitForSigmaEdgeLabel(page, 'Pedro->Ben', 'back in Lisbon');
        // The forward label is untouched by the reverse edit.
        await waitForSigmaEdgeLabel(page, 'Ben->Pedro', 'in Madrid');
    });

    test('bidi pair visual snapshot', async ({ page }) => {
        // Read-only test, so use the source graph directly: an isolated
        // copy's random namespace would end up baked into the snapshot.
        await page.goto('/graph/new_folder/persistent_filler?initialNodes=%5B%5D');
        await waitForLayoutToFinish(page);
        await fitView(page);
        expect(await page.screenshot()).toMatchSnapshot('double-ended-pair.png');
    });
});
