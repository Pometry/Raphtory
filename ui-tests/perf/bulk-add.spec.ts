import * as fs from 'node:fs';
import * as path from 'node:path';
import { expect, test } from './fixtures';

const BASELINE_DIR = path.join(
    __dirname,
    'baselines',
    new Date().toISOString().slice(0, 10),
);

test.describe('bulk-add perf', () => {
    test('expand CMP-000 (~2.7k neighbours) and time to layout-stable', async ({
        page,
        loadGraphWithSeedNode,
    }) => {
        await loadGraphWithSeedNode(page, {
            baseGraph: 'master',
            seedNodeId: 'CMP-000',
        });

        // Wait for the initial single-node render to settle.
        await page.waitForTimeout(2000);

        const samples: { durationMs: number; nodeCount: number }[] = [];

        // 5 runs; drop first as warm-up.
        for (let i = 0; i < 5; i++) {
            await page.evaluate(() => window.__PERF__?.clear());

            // Synthesize sigma's `clickNode` event so the selection
            // doesn't depend on where CMP-000 is currently rendered —
            // routes through the same React handler as a real canvas
            // click but skips hit-testing entirely.
            await page.evaluate(() => {
                const w = window as unknown as {
                    __SIGMA__?: {
                        sigma: {
                            emit: (
                                event: 'clickNode',
                                payload: {
                                    node: string;
                                    event: {
                                        original: {
                                            shiftKey: boolean;
                                            ctrlKey: boolean;
                                            metaKey: boolean;
                                        };
                                    };
                                },
                            ) => void;
                        };
                    };
                };
                w.__SIGMA__?.sigma.emit('clickNode', {
                    node: 'CMP-000',
                    event: {
                        original: {
                            shiftKey: false,
                            ctrlKey: false,
                            metaKey: false,
                        },
                    },
                });
            });
            // Give React a tick to flush the setState from selectNode.
            await page.waitForTimeout(100);

            await page.getByRole('button', { name: 'Explore' }).click();
            // The "Expand" menu item has a Tooltip whose text overrides the
            // accessible name, so `getByRole('menuitem', { name: 'Expand' })`
            // doesn't match. Target by visible inner text instead.
            await page
                .getByRole('menuitem')
                .filter({ hasText: /^Expand$/ })
                .click();

            await page.waitForFunction(
                () =>
                    window.__PERF__
                        ?.dump()
                        .some(
                            (s) =>
                                s.operation === 'bulk-add' &&
                                s.endedAt > s.startedAt,
                        ) ?? false,
                undefined,
                // Single expand of ~2700 nodes on G6 can take well over a
                // minute. Allow generous headroom per iteration.
                { timeout: 3 * 60 * 1000 },
            );

            const sample = await page.evaluate(() =>
                window.__PERF__
                    ?.dump()
                    .filter((s) => s.operation === 'bulk-add')
                    .at(-1),
            );
            if (
                sample !== undefined &&
                sample.metadata?.nodeCount !== undefined
            ) {
                samples.push({
                    durationMs: sample.durationMs,
                    nodeCount: sample.metadata.nodeCount,
                });
            }

            // Reset to seed via undo.
            await page.keyboard.press(
                process.platform === 'darwin' ? 'Meta+z' : 'Control+z',
            );
            await page.waitForTimeout(1000);
        }

        const warmedUp = samples.slice(1);
        const sortedDur = [...warmedUp]
            .map((s) => s.durationMs)
            .sort((a, b) => a - b);
        const median = sortedDur[Math.floor(sortedDur.length / 2)] ?? 0;
        const p95Index = Math.min(
            Math.floor(sortedDur.length * 0.95),
            sortedDur.length - 1,
        );
        const p95 = sortedDur[p95Index] ?? 0;

        fs.mkdirSync(BASELINE_DIR, { recursive: true });
        fs.writeFileSync(
            path.join(BASELINE_DIR, 'bulk-add.json'),
            JSON.stringify({ samples, medianMs: median, p95Ms: p95 }, null, 2),
        );

        console.log(
            `[bulk-add perf] median ${median.toFixed(0)} ms, p95 ${p95.toFixed(0)} ms`,
        );

        expect(samples.length).toBeGreaterThan(0);
    });
});
