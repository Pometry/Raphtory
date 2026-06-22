import { test as base, Page } from '@playwright/test';
import { randomUUID } from 'crypto';
import { mkdir, rm, writeFile } from 'fs/promises';
import path from 'path';
import { copyGraph, deleteGraph } from './e2e/api';
import { waitForLayoutToFinish } from './e2e/utils';

const RAPHTORY_WORK_DIR =
    process.env.RAPHTORY_WORK_DIR ?? '/tmp/vanilla-graphs';

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
    /** Track a graph path for cleanup (e.g. graphs created via Save As) */
    trackForCleanup: (graphPath: string) => void;
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
        const copiedGraphs: string[] = [];

        for (const graphPath of isolatedGraphsConfig) {
            const graphName = graphPath.split('/').pop()!;
            const newPath = `${namespace}/${graphName}`;
            await copyGraph(graphPath, newPath);
            copiedGraphs.push(newPath);
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
            trackForCleanup: (graphPath: string) => {
                copiedGraphs.push(graphPath);
            },
        };

        await use(isolatedGraphs);

        // Cleanup: delete all copied and tracked graphs
        for (const graphPath of copiedGraphs) {
            try {
                await deleteGraph(graphPath);
            } catch {
                // Best effort cleanup — don't fail the test
            }
        }

        // Best-effort: remove the namespace directory left behind after graph
        // deletion. Raphtory has no deleteNamespace mutation, and namespaces
        // map to directories on disk. Only works when the test runner has
        // filesystem access to the server's work dir (i.e. local macOS runs).
        try {
            await rm(path.join(RAPHTORY_WORK_DIR, namespace), {
                recursive: true,
                force: true,
            });
        } catch {
            // Ignore — Docker/Linux runs can't reach the host's work dir
        }
    },
});
export { expect } from '@playwright/test';
