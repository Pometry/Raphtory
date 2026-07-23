import { test as base, Page } from '@playwright/test';

interface PerfFixtures {
    /**
     * Navigates to the saved "big graph" at /graph/big%20graph and
     * waits for the graph to load and stabilise.
     *
     * Requires the dev server to be running with ENABLE_TESTING=true
     * (exposes window.__SIGMA__).
     */
    loadBigGraph(page: Page): Promise<void>;

    /**
     * Loads a base graph with a seed-node initialNodes URL parameter. Lands
     * on a graph view showing only the seed node. Subsequent UI actions
     * (e.g., expand-neighbours) bring in more nodes.
     *
     * Requires ENABLE_TESTING=true.
     */
    loadGraphWithSeedNode(
        page: Page,
        opts: { baseGraph: string; seedNodeId: string },
    ): Promise<void>;

    /**
     * Returns the viewport-relative (x, y) of a node identified by id,
     * suitable for `page.mouse.move(...)` or for the `position` field of
     * `canvas.click({ position })`. Reads sigma's live render position via
     * `getNodeDisplayData` + `framedGraphToViewport`.
     */
    getNodeViewportPosition(
        page: Page,
        nodeId: string,
    ): Promise<{ x: number; y: number }>;
}

async function waitForGraphReady(page: Page, timeout: number): Promise<void> {
    // Wait for sigma to be attached to window and for at least one node
    // to be present. More reliable than aria-busy (which can flip back
    // and forth during async loading paths).
    await page.waitForFunction(
        () => {
            const w = window as unknown as {
                __SIGMA__?: { graph: { nodes(): string[] } };
            };
            const sigma = w.__SIGMA__;
            if (sigma === undefined) return false;
            return sigma.graph.nodes().length > 0;
        },
        undefined,
        { timeout },
    );
}

export const test = base.extend<PerfFixtures>({
    // eslint-disable-next-line no-empty-pattern
    loadBigGraph: async ({}, use) => {
        await use(async (page) => {
            // The space in "big graph" must be URL-encoded.
            await page.goto('/graph/big%20graph');
            // Loading the big graph can be slow; allow 3 minutes.
            await waitForGraphReady(page, 180_000);
        });
    },
    // eslint-disable-next-line no-empty-pattern
    loadGraphWithSeedNode: async ({}, use) => {
        await use(async (page, { baseGraph, seedNodeId }) => {
            const initialNodes = encodeURIComponent(
                JSON.stringify([seedNodeId]),
            );
            await page.goto(
                `/graph?baseGraph=${baseGraph}&initialNodes=${initialNodes}`,
            );
            await waitForGraphReady(page, 30_000);
        });
    },
    // eslint-disable-next-line no-empty-pattern
    getNodeViewportPosition: async ({}, use) => {
        await use(async (page, nodeId) => {
            const position = await page.evaluate((id) => {
                const w = window as unknown as {
                    __SIGMA__?: {
                        sigma: {
                            getNodeDisplayData(
                                node: string,
                            ): { x: number; y: number } | undefined;
                            framedGraphToViewport(p: {
                                x: number;
                                y: number;
                            }): { x: number; y: number };
                        };
                    };
                };

                const sigma = w.__SIGMA__;
                if (sigma === undefined) return null;
                const data = sigma.sigma.getNodeDisplayData(id);
                if (data === undefined) return null;
                return sigma.sigma.framedGraphToViewport({
                    x: data.x,
                    y: data.y,
                });
            }, nodeId);
            if (position === null) {
                throw new Error(
                    `Failed to get viewport position for node "${nodeId}". ` +
                        `Ensure ENABLE_TESTING=true and the node exists in the graph.`,
                );
            }
            return position;
        });
    },
});

export { expect } from '@playwright/test';
