import { test as base, Page } from '@playwright/test';
import { randomUUID } from 'crypto';
import { mkdir, writeFile } from 'fs/promises';
import path from 'path';
import { copyGraph, deleteNamespace } from './e2e/api';
import { waitForLayoutToFinish } from './e2e/utils';

const ENABLE_COVERAGE = process.env.ENABLE_COVERAGE === 'true';
const COVERAGE_DIR = path.join(process.cwd(), '.nyc_output');

type CoverageWindow = Window & {
    __coverage__?: Record<string, unknown>;
};

interface IsolatedGraphs {
    /** The unique namespace for this test's cloned graphs */
    namespace: string;
    /** Build a graph page URL for a cloned graph */
    graphUrl: (graphName: string, params?: string) => string;
    /** Navigate to a cloned graph's page and wait for layout */
    navigateToGraph: (
        page: Page,
        graphName: string,
        params?: string,
    ) => Promise<void>;
}

interface MyFixtures {
    isolatedGraphs: IsolatedGraphs;
    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Playwright fixtures use `void` for side-effect-only fixtures
    collectCoverage: void;
}

// Default graphs to copy if none specified. Override via isolatedGraphsConfig option.
interface MyOptions {
    isolatedGraphsConfig: string[];
}

export const test = base.extend<MyFixtures & MyOptions>({
    isolatedGraphsConfig: [['vanilla/event'], { option: true }],

    collectCoverage: [
        async ({ page }, use, testInfo) => {
            await use();
            if (!ENABLE_COVERAGE) return;
            try {
                const coverage = await page.evaluate(
                    () => (window as CoverageWindow).__coverage__,
                );
                if (!coverage) return;
                await mkdir(COVERAGE_DIR, { recursive: true });
                const filename = `${testInfo.testId}-${randomUUID()}.json`;
                await writeFile(
                    path.join(COVERAGE_DIR, filename),
                    JSON.stringify(coverage),
                );
            } catch {
                // Page may already be closed; skip silently
            }
        },
        { auto: true },
    ],

    isolatedGraphs: async ({ isolatedGraphsConfig }, use) => {
        const namespace = `test_${randomUUID().slice(0, 8)}`;

        for (const graphPath of isolatedGraphsConfig) {
            const graphName = graphPath.split('/').pop()!;
            await copyGraph(graphPath, `${namespace}/${graphName}`);
        }

        const isolatedGraphs: IsolatedGraphs = {
            namespace,
            graphUrl: (graphName: string, params?: string) => {
                const urlBase = `/graph/${namespace}/${graphName}?initialNodes=%5B%5D`;
                return params ? `${urlBase}&${params}` : urlBase;
            },
            navigateToGraph: async (
                navigatePage: Page,
                graphName: string,
                params?: string,
            ) => {
                await navigatePage.goto(
                    isolatedGraphs.graphUrl(graphName, params),
                );
                await waitForLayoutToFinish(navigatePage);
            },
        };

        await use(isolatedGraphs);

        try {
            await deleteNamespace(namespace);
        } catch {
            // Best effort — don't fail the test on cleanup errors
        }
    },
});
export { expect } from '@playwright/test';
