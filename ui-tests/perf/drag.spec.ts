import * as fs from 'node:fs';
import * as path from 'node:path';
import { expect, test } from './fixtures';

const BASELINE_DIR = path.join(
    __dirname,
    'baselines',
    new Date().toISOString().slice(0, 10),
);

test.describe('drag perf', () => {
    test('drag a node for ~2s on big graph', async ({
        page,
        loadBigGraph,
        getNodeViewportPosition,
    }) => {
        await loadBigGraph(page);

        // Wait an extra moment for layout to stabilise before measuring.
        await page.waitForTimeout(2000);

        // Pick the highest-degree node — moving a heavily-connected node
        // forces the renderer to redraw the most edges per frame, exposing
        // the slow path. Picking `nodes[0]` is unreliable; it can be a leaf
        // node with no edges, making the per-frame work trivial.
        const targetNodeId = await page.evaluate(() => {
            const w = window as unknown as {
                __SIGMA__?: {
                    graph: {
                        nodes(): string[];
                        edges(): string[];
                        source(edge: string): string;
                        target(edge: string): string;
                    };
                };
            };
            const sigma = w.__SIGMA__;
            if (sigma === undefined) return null;
            const nodes = sigma.graph.nodes();
            const edges = sigma.graph.edges();
            if (nodes.length === 0) return null;
            const degree = new Map<string, number>();
            for (const edge of edges) {
                const src = sigma.graph.source(edge);
                const tgt = sigma.graph.target(edge);
                degree.set(src, (degree.get(src) ?? 0) + 1);
                degree.set(tgt, (degree.get(tgt) ?? 0) + 1);
            }
            let best = nodes[0];
            let bestDegree = -1;
            for (const node of nodes) {
                const d = degree.get(node) ?? 0;
                if (d > bestDegree) {
                    best = node;
                    bestDegree = d;
                }
            }
            return best;
        });
        if (targetNodeId === null) {
            throw new Error(
                'No nodes in graph — ENABLE_TESTING=true required, or graph failed to load.',
            );
        }
        // Sigma stacks several canvases on top of each other; only
        // `sigma-mouse` accepts pointer events.
        const canvas = page.locator('canvas.sigma-mouse');
        const canvasBox = await canvas.boundingBox();
        if (canvasBox === null) throw new Error('canvas not visible');

        const samples: { fps: number; durationMs: number }[] = [];

        // 5 runs; drop the first as warm-up. Each iteration re-reads the
        // target node's viewport position because previous drag iterations
        // move the node.
        for (let i = 0; i < 5; i++) {
            const nodePos = await getNodeViewportPosition(page, targetNodeId);
            const startX = canvasBox.x + nodePos.x;
            const startY = canvasBox.y + nodePos.y;

            await page.evaluate(() => window.__PERF__?.clear());
            await page.mouse.move(startX, startY);
            await page.mouse.down();
            // Fast continuous drag in a circular path. `steps` generates many
            // intermediate pointermove events per call to saturate the
            // renderer; a real user dragging quickly fires only ~50-100
            // pointermoves/second, which can hide the slow path.
            const RADIUS = 80;
            const LAPS = 1;
            const SEGMENTS_PER_LAP = 24;
            const STEPS_PER_SEGMENT = 8;
            for (let lap = 0; lap < LAPS; lap++) {
                for (let seg = 0; seg < SEGMENTS_PER_LAP; seg++) {
                    const angle = (2 * Math.PI * seg) / SEGMENTS_PER_LAP;
                    const tx = startX + Math.cos(angle) * RADIUS;
                    const ty = startY + Math.sin(angle) * RADIUS;
                    await page.mouse.move(tx, ty, {
                        steps: STEPS_PER_SEGMENT,
                    });
                }
            }
            await page.mouse.up();
            await page.waitForTimeout(200);

            const sample = await page.evaluate(() =>
                window.__PERF__?.dump().find((s) => s.operation === 'drag'),
            );
            if (sample !== undefined && sample.metadata?.fps !== undefined) {
                samples.push({
                    fps: sample.metadata.fps,
                    durationMs: sample.durationMs,
                });
            }
        }

        const warmedUp = samples.slice(1);
        const sortedFps = [...warmedUp].map((s) => s.fps).sort((a, b) => a - b);
        const median = sortedFps[Math.floor(sortedFps.length / 2)] ?? 0;
        const p95Index = Math.min(
            Math.floor(sortedFps.length * 0.95),
            sortedFps.length - 1,
        );
        const p95 = sortedFps[p95Index] ?? 0;

        fs.mkdirSync(BASELINE_DIR, { recursive: true });
        fs.writeFileSync(
            path.join(BASELINE_DIR, 'drag.json'),
            JSON.stringify({ samples, median, p95 }, null, 2),
        );

        console.log(
            `[drag perf] median ${median.toFixed(1)} FPS, p95 ${p95.toFixed(1)} FPS`,
        );

        expect(samples.length).toBeGreaterThan(0);
    });
});
