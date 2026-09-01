import { expect, type Page } from '@playwright/test';

// ── Temporal view y-axis functions ─────────────────────────────────────────

export interface YAxisRow {
    name: string;
    y: number; // page-coordinate Y of the row's centre
}
/**
 * Returns all y-axis node labels and their page-coordinate Y centres,
 * sorted top-to-bottom. Reads from `#yaxis-nodes text` elements.
 */
export async function getYAxisRows(page: Page): Promise<YAxisRow[]> {
    await page.locator('#yaxis-nodes text').first().waitFor();
    const rows = await page.locator('#yaxis-nodes text').evaluateAll((els) =>
        els.map((el) => {
            const rect = el.getBoundingClientRect();
            return {
                name: el.textContent?.trim() ?? '',
                y: rect.top + rect.height / 2,
            };
        }),
    );
    return rows.filter(({ name }) => name.length > 0).sort((a, b) => a.y - b.y);
}

/**
 * Returns the name of the y-axis row whose centre Y is closest to `y`.
 * Row height is inferred from the gaps between consecutive labels, so this
 * works even when rows are unevenly spaced due to zooming or pinning.
 */
export function labelAtY(rows: YAxisRow[], y: number): string {
    return rows.reduce((closest, row) =>
        Math.abs(row.y - y) < Math.abs(closest.y - y) ? row : closest,
    ).name;
}

/**
 * Clicks the colour icon on the y-axis row. Clicking a selected
 * icon again deselects it. Pass `modifiers` (e.g. `['Shift']`) to
 * extend the existing selection instead of replacing it.
 */
export async function selectYAxisNode(
    page: Page,
    name: string,
    options?: { modifiers?: ('Shift' | 'Alt' | 'Control' | 'Meta')[] },
): Promise<void> {
    await page
        .locator('#yaxis-nodes > g')
        .filter({ hasText: name })
        .locator('circle')
        .first()
        .click({ force: true, modifiers: options?.modifiers });
}

/**
 * Clicks the pin icon on the y-axis row. Pinned rows sort to the top of
 * the y-axis (see useTemporalData.ts). Clicking a pinned icon again
 * unpins. The pin <image> is the last <image> in the row group — the
 * sibling-of-text <g> that toggles `pinnedNodes` (see YAxis.tsx).
 */
export async function pinYAxisNode(page: Page, name: string): Promise<void> {
    await page
        .locator('#yaxis-nodes > g')
        .filter({ hasText: name })
        .locator('image')
        .last()
        .click({ force: true });
}

/**
 * Returns the names of y-axis rows whose <text> renders with `font-weight`
 * equal to `weight`. Selected rows render at 500, unselected at 400
 * (see YAxis.tsx).
 */
async function getYAxisNodesByFontWeight(
    page: Page,
    weight: '400' | '500',
): Promise<string[]> {
    return page
        .locator(`#yaxis-nodes > g text[font-weight="${weight}"]`)
        .evaluateAll((els) =>
            els.flatMap((el) => {
                const name = el.textContent?.trim() ?? '';
                return name ? [name] : [];
            }),
        );
}

/** Y-axis rows whose label is rendered bold (font-weight 500 = selected). */
export function getBoldYAxisNodes(page: Page): Promise<string[]> {
    return getYAxisNodesByFontWeight(page, '500');
}

/** Y-axis rows whose label is rendered regular (font-weight 400 = unselected). */
export function getRegularYAxisNodes(page: Page): Promise<string[]> {
    return getYAxisNodesByFontWeight(page, '400');
}

// ── Toolbar controls ───────────────────────────────────────────────────────

/**
 * Clicks the toolbar "Hide edges" button. The button is only present when
 * edges are currently shown — call `showEdges` to flip back. Hiding removes
 * edge <g> elements from the DOM entirely (see useTemporalData.ts:
 * `sourceEdgeEvents = hideEdges ? [] : data.edgeEvents`); prop events stay.
 */
export async function hideEdges(page: Page): Promise<void> {
    await page.getByRole('button', { name: 'Hide edges' }).click();
}

/** Clicks the toolbar "Show edges" button. Only present while edges are hidden. */
export async function showEdges(page: Page): Promise<void> {
    await page.getByRole('button', { name: 'Show edges' }).click();
}

/**
 * Clicks the toolbar "Turn filter on" button. With nodes selected, enabling
 * the filter restricts the y-axis to the selected nodes plus their direct
 * neighbours, and edges/props to those involving the visible nodes (see
 * useTemporalData.ts). Default state is off.
 */
export async function turnFilterOn(page: Page): Promise<void> {
    await page.getByRole('button', { name: 'Turn filter on' }).click();
}

/** Clicks the toolbar "Turn filter off" button. Only present while the filter is on. */
export async function turnFilterOff(page: Page): Promise<void> {
    await page.getByRole('button', { name: 'Turn filter off' }).click();
}

// ── Temporal view data reader ─────────────────────────────────────────

export interface TemporalEdge {
    src: string;
    dst: string;
    /** Time interpolated from the rendered x-position against the axis ticks. */
    time: Date;
    /** Exact event time decoded from the aria-label (the data time the product
     *  used to position the element). */
    labelTime: Date | null;
    /** How far, in pixels, the element is drawn from where `labelTime` maps on
     *  the axis. ~0 when the element sits at the x-position matching its own
     *  timestamp; large when a coordinate-frame bug misplaces it. `null` if the
     *  aria-label carried no time. Domain-agnostic, unlike a time delta. */
    labelTimeOffsetPx: number | null;
    layer: string;
    tickLabel: string;
}
export interface TemporalProperty {
    node: string;
    /** Time interpolated from the rendered x-position against the axis ticks. */
    time: Date;
    /** Exact event time decoded from the aria-label (see TemporalEdge). */
    labelTime: Date | null;
    /** Pixel offset between the element and its `labelTime` (see TemporalEdge). */
    labelTimeOffsetPx: number | null;
    key: string;
    value: string;
    tickLabel: string;
}

export interface TemporalViewData {
    edges: TemporalEdge[];
    properties: TemporalProperty[];
}

interface XTick {
    time: number; // epoch ms
    screenX: number; // page-coordinate X of the tick line
}

const UPPER_X_AXIS_TICK_SELECTOR =
    '#temporal-view svg > g:first-child > g:first-child .tick';

interface XAxisTick {
    time: number | null;
    label: string;
    screenX: number;
}

/**
 * Single source of truth for the upper x-axis ticks: one DOM read returning,
 * per tick, both d3's internal datum (`time`, for pixel↔time interpolation) and
 * the rendered `label` (for view-anchored checks). The upper axis is the first
 * <g> child of the SVG. Ticks with neither a bound Date nor a label (e.g. the
 * domain path) are skipped.
 */
async function getXAxisTicks(page: Page): Promise<XAxisTick[]> {
    await page.locator(UPPER_X_AXIS_TICK_SELECTOR).first().waitFor();
    return page.locator(UPPER_X_AXIS_TICK_SELECTOR).evaluateAll((ticks) =>
        ticks.flatMap((tick) => {
            const datum = (tick as unknown as { __data__: unknown }).__data__;
            const time = datum instanceof Date ? datum.getTime() : null;
            const label = tick.querySelector('text')?.textContent?.trim() ?? '';
            if (time === null && label === '') return [];
            const anchor = tick.querySelector('line') ?? tick;
            const rect = anchor.getBoundingClientRect();
            return [{ time, label, screenX: rect.left + rect.width / 2 }];
        }),
    );
}

export interface XAxisTickLabel {
    /** The text the user actually sees on the tick, e.g. "2024". */
    label: string;
    /** Page-coordinate X of the tick line centre. */
    screenX: number;
}

/** View-only projection of the ticks: labelled ones, sorted left-to-right. */
function visibleTicksOf(ticks: XAxisTick[]): XAxisTickLabel[] {
    return ticks
        .filter((t) => t.label !== '')
        .map(({ label, screenX }) => ({ label, screenX }))
        .sort((a, b) => a.screenX - b.screenX);
}

/**
 * The *visible* upper x-axis ticks — the labels rendered to the user — and
 * their screen X positions, sorted left-to-right. Ticks with no label (d3 draws
 * some mark-only ticks) are omitted, since the user sees nothing there. A thin
 * projection of `getXAxisTicks`, so it shares the same single DOM read.
 */
export async function getVisibleXAxisTicks(
    page: Page,
): Promise<XAxisTickLabel[]> {
    return visibleTicksOf(await getXAxisTicks(page));
}

/**
 * Returns the label of the visible tick whose centre X is closest to `screenX`
 * — i.e. the tick label the user would read an event as sitting under. Pass a
 * page-coordinate event X; ticks and events already share that frame (see
 * `interpolateTime`), so no adjustment is needed.
 */
export function labelAtX(ticks: XAxisTickLabel[], screenX: number): string {
    if (ticks.length === 0) return '';
    return ticks.reduce((closest, tick) =>
        Math.abs(tick.screenX - screenX) < Math.abs(closest.screenX - screenX)
            ? tick
            : closest,
    ).label;
}

/**
 * Linearly interpolates an epoch-ms timestamp from a page-coordinate screen X.
 * Event circles and upper-axis ticks share the same page frame — the ticks use
 * a scale spanning the full SVG width (range [0, svgWidth]) while events sit in
 * a plot group translated by the y-axis width, so an event and a tick at the
 * same time land at the same page X (see TemporalView.tsx / XAxis.tsx). No
 * y-axis correction is therefore applied.
 */
function interpolateTime(screenX: number, ticks: XTick[]): number {
    if (ticks.length < 2) return NaN;
    const sorted = [...ticks].sort((a, b) => a.screenX - b.screenX);
    let lo = sorted[0];
    let hi = sorted[sorted.length - 1];
    for (let i = 0; i < sorted.length - 1; i++) {
        if (sorted[i].screenX <= screenX && screenX <= sorted[i + 1].screenX) {
            lo = sorted[i];
            hi = sorted[i + 1];
            break;
        }
    }
    const fraction = (screenX - lo.screenX) / (hi.screenX - lo.screenX);
    return Math.round(lo.time + fraction * (hi.time - lo.time));
}

/**
 * Inverse of `interpolateTime`: the screen X an epoch-ms timestamp maps to,
 * against the same axis ticks. Used to measure, in pixels, how far an element
 * is drawn from where its own timestamp says it should be — a domain-agnostic
 * check (a fixed pixel skew doesn't inflate into large time errors on wide
 * time ranges the way an absolute-time tolerance would).
 */
function timeToScreenX(timeMs: number, ticks: XTick[]): number {
    if (ticks.length < 2) return NaN;
    const sorted = [...ticks].sort((a, b) => a.time - b.time);
    let lo = sorted[0];
    let hi = sorted[sorted.length - 1];
    for (let i = 0; i < sorted.length - 1; i++) {
        if (sorted[i].time <= timeMs && timeMs <= sorted[i + 1].time) {
            lo = sorted[i];
            hi = sorted[i + 1];
            break;
        }
    }
    const fraction = (timeMs - lo.time) / (hi.time - lo.time);
    return lo.screenX + fraction * (hi.screenX - lo.screenX);
}

interface RawEdge {
    screenX: number;
    srcY: number;
    dstY: number;
    ariaLabel: string;
}

async function readRawEdges(page: Page): Promise<RawEdge[]> {
    return page
        .locator('#temporal-view g:has(> defs):has(> circle)')
        .evaluateAll((groups) =>
            groups.flatMap((group) => {
                const circles = group.querySelectorAll('circle');
                const n = circles.length;
                if (n < 2) return [];
                // Hover/selection prepends highlight circles; the last
                // two are always the solid src then dst endpoints.
                const srcRect = circles[n - 2].getBoundingClientRect();
                const dstRect = circles[n - 1].getBoundingClientRect();
                return [
                    {
                        screenX: srcRect.left + srcRect.width / 2,
                        srcY: srcRect.top + srcRect.height / 2,
                        dstY: dstRect.top + dstRect.height / 2,
                        ariaLabel: group.getAttribute('aria-label') ?? '',
                    },
                ];
            }),
        );
}

/**
 * Edge layer and time have no visual representation beyond the element's
 * position — they only appear in the aria-label, which encodes the edge id as a
 * JSON tuple (see Edges.tsx):
 *   `Edge ID ["${src}","${dst}","${layer}",${epochMs}]`
 * Returns the layer ('' if missing/malformed) and the exact event time
 * (`null` if absent), the latter for asserting the element is drawn at the
 * x-position matching its own timestamp.
 */
function parseEdgeAria(ariaLabel: string): {
    layer: string;
    time: number | null;
} {
    const prefix = 'Edge ID ';
    if (!ariaLabel.startsWith(prefix)) return { layer: '', time: null };
    try {
        const parsed = JSON.parse(ariaLabel.slice(prefix.length));
        if (!Array.isArray(parsed)) return { layer: '', time: null };
        return {
            layer: typeof parsed[2] === 'string' ? parsed[2] : '',
            time: typeof parsed[3] === 'number' ? parsed[3] : null,
        };
    } catch {
        return { layer: '', time: null };
    }
}

interface RawProp {
    screenX: number;
    screenY: number;
    /** Sensor <rect>'s aria-label, if the group has one. Format:
     *   "Select node via event trace: ${nodeId}-${prop}-${epochMs}-${value}"
     * Empty string when the group has no <rect> (caller treats key/value
     * as unknown rather than dropping the entry). */
    ariaLabel: string;
}

async function readRawProps(page: Page): Promise<RawProp[]> {
    // Prop event groups have a direct child <circle cx="…">: prop-event
    // circles always carry an explicit cx (= xScale(time)), whereas edge
    // and y-axis circles only carry cy. Some groups render without the
    // sensor <rect> (e.g. nodes with no edges); we still surface them so
    // the caller can see every event circle that was drawn.
    return page
        .locator('#temporal-view g:has(> circle[cx])')
        .evaluateAll((groups) =>
            groups.flatMap((group) => {
                const circle = group.querySelector(':scope > circle[cx]');
                if (!circle) return [];
                const box = circle.getBoundingClientRect();
                const rect = group.querySelector(':scope > rect');
                return [
                    {
                        screenX: box.left + box.width / 2,
                        screenY: box.top + box.height / 2,
                        ariaLabel: rect?.getAttribute('aria-label') ?? '',
                    },
                ];
            }),
        );
}

/**
 * Extracts `{ key, value }` from a prop sensor's aria-label given the node
 * id (resolved from the y-coordinate). The aria-label format is
 *   "Select node via event trace: ${nodeId}-${prop}-${epochMs}-${value}"
 * Both nodeId and value may contain hyphens, so we anchor on the only
 * all-digit segment (epochMs) to delimit them. Returns empty strings when
 * the aria-label is missing or doesn't match the expected shape.
 *
 * The UI uppercases the prop name via CSS `text-transform`; we mirror that
 * here so callers see the same string they'd read off the tooltip.
 */
function parsePropAriaLabel(
    ariaLabel: string,
    node: string,
): { key: string; value: string; time: number | null } {
    if (!ariaLabel) return { key: '', value: '', time: null };
    const prefix = 'Select node via event trace: ';
    let body = ariaLabel.startsWith(prefix)
        ? ariaLabel.slice(prefix.length)
        : ariaLabel;
    if (body.startsWith(`${node}-`)) body = body.slice(node.length + 1);
    const m = body.match(/^(.+)-(\d+)-(.+)$/);
    if (!m) return { key: '', value: '', time: null };
    return { key: m[1].toUpperCase(), value: m[3], time: Number(m[2]) };
}

/**
 * Reads the temporal view SVG and returns all rendered edges and property
 * events as structured data.
 *
 * Node identity (src/dst/node) and time are derived strictly from rendered
 * coordinates. Prop key/value come from the sensor <rect>'s aria-label (CSS-uppercased
 * to match what the tooltip would show); groups missing that rect still appear in `properties`
 * with empty key/value rather than being dropped. Edge layer also reads from the aria-label.
 */
export async function getTemporalViewData(
    page: Page,
): Promise<TemporalViewData> {
    const [yAxisRows, allTicks] = await Promise.all([
        getYAxisRows(page),
        getXAxisTicks(page),
    ]);
    // Two projections of one tick read: time anchors for interpolation, and the
    // labelled subset for view-anchored tick labels.
    const timeTicks = allTicks.flatMap((t) =>
        t.time !== null ? [{ time: t.time, screenX: t.screenX }] : [],
    );
    const visibleTicks = visibleTicksOf(allTicks);

    const rawEdges = await readRawEdges(page);
    const edges: TemporalEdge[] = rawEdges.map(
        ({ screenX, srcY, dstY, ariaLabel }) => {
            const src = labelAtY(yAxisRows, srcY);
            const dst = labelAtY(yAxisRows, dstY);
            const { layer, time: labelTimeMs } = parseEdgeAria(ariaLabel);
            return {
                src,
                dst,
                time: new Date(interpolateTime(screenX, timeTicks)),
                labelTime: labelTimeMs !== null ? new Date(labelTimeMs) : null,
                labelTimeOffsetPx:
                    labelTimeMs !== null
                        ? Math.abs(
                              screenX - timeToScreenX(labelTimeMs, timeTicks),
                          )
                        : null,
                layer,
                tickLabel: labelAtX(visibleTicks, screenX),
            };
        },
    );

    const rawProps = await readRawProps(page);
    const properties: TemporalProperty[] = rawProps.map(
        ({ screenX, screenY, ariaLabel }) => {
            const node = labelAtY(yAxisRows, screenY);
            const {
                key,
                value,
                time: labelTimeMs,
            } = parsePropAriaLabel(ariaLabel, node);
            return {
                node,
                time: new Date(interpolateTime(screenX, timeTicks)),
                labelTime: labelTimeMs !== null ? new Date(labelTimeMs) : null,
                labelTimeOffsetPx:
                    labelTimeMs !== null
                        ? Math.abs(
                              screenX - timeToScreenX(labelTimeMs, timeTicks),
                          )
                        : null,
                key,
                value,
                tickLabel: labelAtX(visibleTicks, screenX),
            };
        },
    );

    return { edges, properties };
}

export async function openTimeline(page: Page) {
    await page.getByRole('button', { name: 'Open timeline' }).click();
    // wait for animation to finish
    await page.waitForTimeout(300);
}

export async function hoverEdgeAndExpectTooltip(
    page: Page,
    selector: string,
    expectedTexts: string[],
) {
    const temporalViewIsHidden = await page
        .locator('#temporal-view')
        .isHidden();
    if (temporalViewIsHidden) {
        await openTimeline(page);
        await page.waitForTimeout(500);
    }

    const line = page.locator(selector).first();
    await expect(line).toHaveCount(1);

    // Dispatch the enter event directly rather than moving the cursor: edges
    // with identical timestamps render at the same X with the shorter line's
    // vertical range entirely contained within the longer one, so a positional
    // hit-test on the overlap lands on whichever is rendered last in DOM
    // order — and raphtory's edge iteration order is non-deterministic.
    // React polyfills onMouseEnter/onMouseLeave from native mouseover/
    // mouseout via root-level delegation, so dispatch mouseover/mouseout
    // (which bubble) rather than mouseenter/mouseleave (which don't).
    await line.dispatchEvent('mouseover');
    for (const text of expectedTexts) {
        await expect(
            page.getByText(text, { exact: true }).first(),
        ).toBeVisible();
    }
    await line.dispatchEvent('mouseout');
}
