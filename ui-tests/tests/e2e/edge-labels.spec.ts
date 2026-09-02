import { expect, test } from '../fixtures';
import { changeTab, clickOnEdge, fitView, saveAs, waitForSigmaEdgeLabel } from './graph.utils';
import { waitForLayoutToFinish } from './utils';

async function insertProperty(page: import('@playwright/test').Page, propertyLabel: string) {
    await page.getByRole('button', { name: 'Insert property' }).click();
    await page.getByPlaceholder('Search properties').fill(propertyLabel);
    await page.getByRole('button', { name: propertyLabel, exact: false }).click();
}

test.describe('edge label templates', () => {
    test.use({ isolatedGraphsConfig: ['new_folder/persistent_filler'] });

    test('edge label template resolves edge properties into the sigma label', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // Ben→Hamza (layer "meets") carries the property where=London
        await clickOnEdge(page, 'Ben', 'Hamza');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('in {where}');

        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'in London');
    });

    test('edge label template persists after save and reload', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnEdge(page, 'Ben', 'Hamza');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('{src} met {dst}');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'Ben met Hamza');

        // Save under a new name and reload
        const savedName = `${isolatedGraphs.namespace}/persistent_filler_edge_labeled`;
        await saveAs(page, savedName);

        await page.reload();
        await waitForLayoutToFinish(page);

        // Label must survive the round-trip
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'Ben met Hamza');
    });

    test('property browser inserts a token into the label template field', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnEdge(page, 'Ben', 'Hamza');
        await changeTab(page, 'Styling');
        await insertProperty(page, 'Source id');

        await expect(page.getByLabel('Label template')).toHaveValue(/\{src\}/);
    });

    test('layer template labels all edges on the layer; per-edge overrides it', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // Graph-wide template on the "meets" layer labels both meets edges
        await changeTab(page, 'Styling');
        await page.getByRole('button', { name: 'Edge Layers' }).click();
        await page.getByRole('combobox', { name: 'Style Layer' }).click();
        await page.getByRole('option', { name: 'meets', exact: true }).click();
        await page.getByLabel('Label template').fill('in {where}');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'in London');
        await waitForSigmaEdgeLabel(page, 'Ben->Pedro', 'in Madrid');

        // A per-edge template overrides the layer template on that edge only
        await clickOnEdge(page, 'Ben', 'Hamza');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('{src} met {dst}');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'Ben met Hamza');
        await waitForSigmaEdgeLabel(page, 'Ben->Pedro', 'in Madrid');
    });

    test('default edge label template labels all edges; layer overrides it', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // The customizable default labels every edge, whatever its layer
        await changeTab(page, 'Styling');
        await page.getByRole('button', { name: 'Edge Layers' }).click();
        await page.getByRole('combobox', { name: 'Style Layer' }).click();
        await page.getByRole('option', { name: 'Default (all edges)' }).click();
        await page.getByLabel('Label template').fill('{src}-{dst}');
        await waitForSigmaEdgeLabel(page, 'Ben->Pometry', 'Ben-Pometry');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'Ben-Hamza');

        // A layer template overrides the default for that layer's edges only
        await page.getByRole('combobox', { name: 'Style Layer' }).click();
        await page.getByRole('option', { name: 'meets', exact: true }).click();
        await page.getByLabel('Label template').fill('in {where}');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'in London');
        await waitForSigmaEdgeLabel(page, 'Ben->Pometry', 'Ben-Pometry');

        // Both tiers survive a save round-trip
        const savedName = `${isolatedGraphs.namespace}/persistent_filler_default_labeled`;
        await saveAs(page, savedName);
        await page.reload();
        await waitForLayoutToFinish(page);
        await waitForSigmaEdgeLabel(page, 'Ben->Pometry', 'Ben-Pometry');
        await waitForSigmaEdgeLabel(page, 'Ben->Hamza', 'in London');
    });
});
