import { expect, test } from '../fixtures';
import { changeTab, clickOnNode, fitView, saveAs } from './graph.utils';
import { waitForLayoutToFinish } from './utils';

// Inline __SIGMA__ shape — avoids importing the non-exported internal type.
interface SigmaGraph {
    nodes(): string[];
    getNodeAttribute(id: string, key: string): unknown;
}
type BrowserWindow = Window & { __SIGMA__?: { graph: SigmaGraph } };

/** Wait until sigma's `label` attribute for the node with the given id equals `expected`. */
async function waitForSigmaLabel(
    page: import('@playwright/test').Page,
    nodeId: string,
    expected: string,
) {
    await page.waitForFunction(
        ({ id, label }: { id: string; label: string }) => {
            const sigma = (window as BrowserWindow).__SIGMA__;
            if (!sigma) return false;
            return sigma.graph.getNodeAttribute(id, 'label') === label;
        },
        { id: nodeId, label: expected },
        { timeout: 10000 },
    );
}

async function insertProperty(page: import('@playwright/test').Page, propertyLabel: string) {
    await page.getByRole('button', { name: 'Insert property' }).click();
    await page.getByPlaceholder('Search properties').fill(propertyLabel);
    await page.getByRole('button', { name: propertyLabel, exact: false }).click();
}

test.describe('node label templates', () => {
    test.use({ isolatedGraphsConfig: ['new_folder/persistent_filler'] });

    test('type label template updates sigma label for all nodes of that type', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // Open the Styling tab → node-type panel; select the None type (Pedro's type)
        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'None' }).click();

        // Fill in the label template
        await page.getByLabel('Label template').fill('Node {id}');

        // Pedro (a None-type node) should now show the resolved label
        await waitForSigmaLabel(page, 'Pedro', 'Node Pedro');
    });

    test('individual node label template overrides type template', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // Set a type-level template for None type
        await changeTab(page, 'Styling');
        await page.getByText('Select Node Type').click();
        await page.getByRole('option', { name: 'None' }).click();
        await page.getByLabel('Label template').fill('Type {id}');
        await waitForSigmaLabel(page, 'Pedro', 'Type Pedro');

        // Now select Pedro individually and override with a node-level template
        await clickOnNode(page, 'Type Pedro');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('Node {id}');

        // Node-level template takes precedence over type-level
        await waitForSigmaLabel(page, 'Pedro', 'Node Pedro');
    });

    test('node label template persists after save and reload', async ({ page, isolatedGraphs }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        // Click Pedro → go to Styling tab → set a node-level label template
        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        await page.getByLabel('Label template').fill('Node {id}');
        await waitForSigmaLabel(page, 'Pedro', 'Node Pedro');

        // Save under a new name and reload
        const savedName = `${isolatedGraphs.namespace}/persistent_filler_labeled`;
        await saveAs(page, savedName);

        await page.reload();
        await waitForLayoutToFinish(page);

        // Label must survive the round-trip
        await waitForSigmaLabel(page, 'Pedro', 'Node Pedro');
    });

    test('property browser inserts a token into the label template field', async ({
        page,
        isolatedGraphs,
    }) => {
        await isolatedGraphs.navigateToGraph(page, 'persistent_filler');
        await fitView(page);

        await clickOnNode(page, 'Pedro');
        await changeTab(page, 'Styling');
        await insertProperty(page, 'Node id');

        await expect(page.getByLabel('Label template')).toHaveValue(/\{id\}/);
    });
});
