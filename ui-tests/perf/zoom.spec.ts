import * as fs from 'node:fs';
import * as path from 'node:path';

import { expect, test } from './fixtures';

const BASELINE_DIR = path.join(__dirname, 'baselines', new Date().toISOString().slice(0, 10));

// Sweep zoom-in from a fully-zoomed-out start ratio then back out in
// equal ticks. The midpoint is well past the edge-label fontSize
// threshold so the sweep crosses the label-render-on transition in
// both directions. Smaller per-tick delta + more ticks approximates a
// trackpad pinch rather than a wheel notch — more frames per unit zoom,
// more likely to surface subtle lag.
const START_RATIO = 3;
const WHEEL_TICKS_PER_DIRECTION = 60;
const WHEEL_DELTA_Y_PX = 25;
// Offset applied to the focal node's viewport position. Zooming at the
// hub node itself frames just the hub at max ratio (the layout enforces
// empty space around it); zooming slightly off lands the focal point in
// the dense edge tangle next to the hub.
const FOCAL_OFFSET_X_PX = -10;
const FOCAL_OFFSET_Y_PX = 0;

test.describe('zoom perf', () => {
    test('zoom in then out over big graph', async ({
        page,
        loadBigGraph,
        getNodeViewportPosition,
    }) => {
        await loadBigGraph(page);

        // Let the initial layout settle before any measurement.
        await page.waitForTimeout(2000);

        // Anchor the zoom on the highest-degree node, then nudge the
        // focal pixel off it slightly (see FOCAL_OFFSET_*). That puts
        // the cursor in the dense edge tangle right next to the hub
        // rather than on the hub itself. Centring the mouse on the
        // canvas instead would leave the hub off the focal point
        // entirely and miss the slow path.
        const focalNodeId = await page.evaluate(() => {
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
            if (nodes.length === 0) return null;
            const degree = new Map<string, number>();
            for (const edge of sigma.graph.edges()) {
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
        if (focalNodeId === null) {
            throw new Error(
                'No nodes in graph — ENABLE_TESTING=true required, or graph failed to load.',
            );
        }

        // canvas.first() — any sigma canvas has the same bounding box; the
        // wheel listener is attached to the parent container, so wheel
        // events from anywhere over the canvas reach the handler.
        const canvas = page.locator('canvas').first();
        const canvasBox = await canvas.boundingBox();
        if (canvasBox === null) throw new Error('canvas not visible');

        const samples: { fps: number; durationMs: number }[] = [];

        // 5 runs; drop the first as warm-up.
        for (let i = 0; i < 5; i++) {
            // Reset sigma's camera to a known zoomed-out state so each
            // iteration's measured zoom-in covers the same ratio range.
            await page.evaluate((startRatio) => {
                const w = window as unknown as {
                    __SIGMA__?: {
                        sigma: {
                            getCamera(): {
                                setState(s: {
                                    x: number;
                                    y: number;
                                    ratio: number;
                                    angle: number;
                                }): void;
                            };
                        };
                    };
                };
                w.__SIGMA__?.sigma.getCamera().setState({
                    x: 0.5,
                    y: 0.5,
                    ratio: startRatio,
                    angle: 0,
                });
            }, START_RATIO);
            // Let the camera-reset render settle before clearing PERF.
            await page.waitForTimeout(300);

            await page.evaluate(() => window.__PERF__?.clear());

            // Start an explicit zoom frame-rate span and stash the handle
            // on `window` so the matching stop call below can retrieve it.
            // Test-side bracketing (rather than source-side as drag does)
            // because wheel events have no natural start/end boundary.
            await page.evaluate(() => {
                const w = window as unknown as {
                    __PERF__?: {
                        recordFrameRate: (op: 'zoom') => {
                            stop: () => void;
                        };
                    };
                    __PERF_ZOOM_HANDLE__?: { stop: () => void };
                };
                if (w.__PERF__ === undefined) return;
                w.__PERF_ZOOM_HANDLE__ = w.__PERF__.recordFrameRate('zoom');
            });

            // Park the mouse near the focal node (offset by
            // FOCAL_OFFSET_*) — wheel events use the cursor position as
            // the zoom focal point. Read the node's viewport position
            // AFTER the camera reset; the framed position depends on
            // the camera state.
            const nodePos = await getNodeViewportPosition(page, focalNodeId);
            await page.mouse.move(
                canvasBox.x + nodePos.x + FOCAL_OFFSET_X_PX,
                canvasBox.y + nodePos.y + FOCAL_OFFSET_Y_PX,
            );

            // Drive zoom-in then zoom-out. Each `page.mouse.wheel` call
            // yields to the event loop, giving RAF a chance to render
            // between ticks. Negative deltaY = zoom in under our handler.
            for (let j = 0; j < WHEEL_TICKS_PER_DIRECTION; j++) {
                await page.mouse.wheel(0, -WHEEL_DELTA_Y_PX);
            }
            for (let j = 0; j < WHEEL_TICKS_PER_DIRECTION; j++) {
                await page.mouse.wheel(0, WHEEL_DELTA_Y_PX);
            }
            // Let the last frame render and the camera settle before
            // closing the recording span.
            await page.waitForTimeout(300);

            await page.evaluate(() => {
                const w = window as unknown as {
                    __PERF_ZOOM_HANDLE__?: { stop: () => void };
                };
                w.__PERF_ZOOM_HANDLE__?.stop();
            });

            const sample = await page.evaluate(() =>
                window.__PERF__
                    ?.dump()
                    .filter((s) => s.operation === 'zoom')
                    .at(-1),
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
        const p95Index = Math.min(Math.floor(sortedFps.length * 0.95), sortedFps.length - 1);
        const p95 = sortedFps[p95Index] ?? 0;

        fs.mkdirSync(BASELINE_DIR, { recursive: true });
        fs.writeFileSync(
            path.join(BASELINE_DIR, 'zoom.json'),
            JSON.stringify({ samples, median, p95 }, null, 2),
        );

        console.log(`[zoom perf] median ${median.toFixed(1)} FPS, p95 ${p95.toFixed(1)} FPS`);

        expect(samples.length).toBeGreaterThan(0);
    });
});
