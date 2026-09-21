import { Page } from '@playwright/test';

import { clickAfterPaginating, waitForLayoutToFinish } from './utils';

export async function clickSavedGraphsGraph(page: Page, graphName: string) {
    const target = page.getByRole('button', {
        name: new RegExp(`^${graphName} GRAPH`),
    });
    await clickAfterPaginating(page, target, `Graph "${graphName}"`);
}

export async function clickSavedGraphsFolder(page: Page, folderName: string) {
    const target = page.getByRole('button', {
        name: new RegExp(`^${folderName} FOLDER Click to browse`),
    });
    await clickAfterPaginating(page, target, `Folder "${folderName}"`);
}

export type OpenSavedGraphMethod = 'split-button' | 'double-click' | 'preview-graph';

export const OPEN_SAVED_GRAPH_METHODS: OpenSavedGraphMethod[] = [
    'split-button',
    'double-click',
    'preview-graph',
];

/**
 * Navigate into a saved-graphs namespace, optionally opening a specific graph.
 *
 * - When `path.graphName` is omitted, ends on the namespace's folder page;
 *   the optional `view` is applied to switch to card/table view.
 * - When `path.graphName` is provided, opens that graph using `method`
 *   (default 'split-button') and waits for the graph layout to finish. The
 *   `view` option is ignored because each method controls its own view
 *   internally (e.g. 'double-click' switches to the table view).
 */
export async function navigateInSavedGraphs(
    page: Page,
    path: { namespace: string; graphName?: string },
    options: {
        method?: OpenSavedGraphMethod;
        view?: 'card' | 'table';
    } = {},
) {
    const { namespace, graphName } = path;
    const { method = 'split-button', view } = options;
    await page.goto('/saved-graphs');
    await page.waitForLoadState('networkidle');
    if (await page.getByText('Welcome to Explorations').isVisible()) {
        throw new Error('No saved graphs exist!');
    }
    await clickSavedGraphsFolder(page, namespace);

    if (graphName === undefined) {
        if (view === 'table') {
            await page.getByRole('button', { name: 'Table view', exact: true }).click();
        }
        return;
    }

    switch (method) {
        case 'double-click':
            await page.getByRole('button', { name: 'Table view', exact: true }).click();
            await page.getByRole('cell', { name: graphName, exact: true }).dblclick();
            break;
        case 'split-button':
            await clickSavedGraphsGraph(page, graphName);
            await page.getByRole('link', { name: 'Open' }).first().click();
            break;
        case 'preview-graph':
            await clickSavedGraphsGraph(page, graphName);
            await page
                .getByRole('region', { name: 'Graph preview' })
                .getByRole('link', { name: 'Open' })
                .click();
            break;
    }
    await waitForLayoutToFinish(page);
}
