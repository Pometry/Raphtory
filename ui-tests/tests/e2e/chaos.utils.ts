import { expect, type Page } from '@playwright/test';

import { changeTab, clickOnNode, getGraphState } from './graph.utils';
import {
    closeTimeline,
    getYAxisRows,
    openTimeline,
    pinYAxisNode,
    selectYAxisNode,
} from './temporalview.utils';
import { waitForLayoutToFinish } from './utils';

/**
 * Chaos-engineering building blocks: small, reusable actions for driving a
 * graph page through a random sequence of user interactions and asserting the
 * app behaves as predicted after each one.
 *
 * Each action is exposed as a `ChaosCommand` — a triple of:
 *   - `check`     : is this action applicable to the current model?
 *   - `transition`: pure prediction of the model after the action runs;
 *   - `run`       : drive the real UI (returns a human-readable label).
 *
 * The caller (a fast-check model-based run) keeps a `ChaosModel` — a small,
 * reliably-readable slice of the real graph — in lockstep with the UI, calling
 * `transition` to predict and `assertModel` to confirm. Randomness is a single
 * selector `r` in [0, 1) that each action maps onto live state, so a failing
 * sequence is fully reproducible. An action whose control is absent is a no-op:
 * an absent control is a valid UI state, not a failure.
 */

export type ChaosActionType =
    | 'deleteNode'
    | 'selectNode'
    | 'highlightLayer'
    | 'switchLayout'
    | 'switchTab'
    | 'tvSelectYAxis'
    | 'tvPinYAxis'
    | 'tvToggleEdges'
    | 'tvToggleFilter';

export interface ChaosOptions {
    /** Node-deleting actions never take the graph below this many nodes. */
    minNodes: number;
}

/**
 * The abstract state we predict and assert after every action. Deliberately a
 * small, reliably-readable slice of the real graph — currently just the sorted
 * node-id set. Grow this (selection, active tab, timeline flags) as each new
 * modeled field is proven stable to read back.
 */
export interface ChaosModel {
    /** Sorted node-id set. */
    nodes: string[];
}

export interface ChaosCommand {
    type: ChaosActionType;
    /** Whether this action applies to the current model (fast-check `check`). */
    check(model: ChaosModel, opts: ChaosOptions): boolean;
    /** Pure prediction: the model after this action runs with selector `r`. */
    transition(model: ChaosModel, r: number, opts: ChaosOptions): ChaosModel;
    /** Drive the real UI with selector `r`. Returns a human-readable label. */
    run(page: Page, r: number, opts: ChaosOptions): Promise<string>;
}

// Menu items are named by their layout description (see DEFAULT_LAYOUT_MENU_CFG
// in raphtory-base-ui/lib/features/graph/layout.ts).
const LAYOUT_OPTIONS = [
    'Physics-based layout with natural clustering',
    'Arrange nodes in concentric circles',
    'Force-directed layout algorithm',
    'Top-to-bottom hierarchical tree',
    'Left-to-right hierarchical tree',
];

const TAB_OPTIONS = ['Connections', 'Pinned', 'Selected', 'Styling', 'Trace Log'];

/** Pick an element by an [0, 1) selector, clamped so r === 1 stays in range. */
function pick<T>(items: T[], r: number): T | undefined {
    if (items.length === 0) return undefined;
    return items[Math.min(items.length - 1, Math.floor(r * items.length))];
}

/** The modeled slice of live graph state: the sorted node-id set. */
function sortedIds(state: { nodes: { id: string }[] }): string[] {
    return state.nodes.map((n) => n.id).sort();
}

async function temporalViewVisible(page: Page): Promise<boolean> {
    return page
        .locator('#temporal-view')
        .first()
        .isVisible()
        .catch(() => false);
}

/** Put the temporal view into the requested open/closed state, only acting if
 *  it isn't already there; returns whether it is open afterwards. */
async function setTemporalView(page: Page, open: boolean): Promise<boolean> {
    const visible = await temporalViewVisible(page);
    if (open === visible) return visible;
    if (open) {
        const openBtn = page.getByRole('button', { name: 'Open timeline' });
        if (await openBtn.count()) await openTimeline(page);
    } else {
        await closeTimeline(page);
    }
    return temporalViewVisible(page);
}

/** Assert the live graph matches the predicted model. */
export async function assertModel(page: Page, model: ChaosModel): Promise<void> {
    expect(sortedIds(await getGraphState(page))).toEqual(model.nodes);
}

/** The real-UI drivers, keyed by action type. */
const RUN: Record<ChaosActionType, (page: Page, r: number, opts: ChaosOptions) => Promise<string>> =
    {
        deleteNode: async (page, r, opts) => {
            // Node actions click the graph canvas, which the temporal view overlays
            // (its SVG intercepts pointer events). Close the timeline first so the
            // canvas is reachable.
            await setTemporalView(page, false);
            // Pick from the sorted id list so this stays in lockstep with the
            // model's own `pick(model.nodes, r)` in the transition below.
            const state = await getGraphState(page);
            const ids = sortedIds(state);
            if (ids.length <= opts.minNodes) return 'deleteNode: skip (at floor)';
            const id = pick(ids, r)!;
            // node needs to be selected to delete, if not selected click it first
            if (!state.selected.includes(id)) {
                await clickOnNode(page, id);
            }
            await page.getByRole('button', { name: 'Delete selected (⌫)' }).click();
            await page.mouse.move(0, 0);
            await waitForLayoutToFinish(page);
            return `deleteNode: ${id}`;
        },
        selectNode: async (page, r) => {
            // Clear the temporal-view overlay so the node is clickable on canvas.
            await setTemporalView(page, false);
            const { nodes } = await getGraphState(page);
            const node = pick(nodes, r);
            if (!node) return 'selectNode: skip (no nodes)';
            await clickOnNode(page, node.id);
            return `selectNode: ${node.id}`;
        },
        highlightLayer: async (page, r) => {
            // Highlight a relationship/risk on the graph (changes which nodes are
            // highlighted, never which nodes exist). Toggle one off if all are on.
            const highlight = page.getByRole('button', {
                name: 'Highlight on graph',
            });
            const count = await highlight.count();
            if (count > 0) {
                const idx = Math.min(count - 1, Math.floor(r * count));
                await highlight.nth(idx).click();
                await waitForLayoutToFinish(page);
                return `highlightLayer: on #${idx}`;
            }
            const remove = page.getByRole('button', { name: 'Remove highlight' });
            if (await remove.count()) {
                await remove.first().click();
                await waitForLayoutToFinish(page);
                return 'highlightLayer: off';
            }
            return 'highlightLayer: skip (no highlight controls)';
        },
        switchLayout: async (page, r) => {
            const layout = pick(LAYOUT_OPTIONS, r)!;
            const layoutBtn = page.getByRole('button', { name: 'Layout' });
            if (!(await layoutBtn.count())) {
                return 'switchLayout: skip (no Layout button)';
            }
            await layoutBtn.click();
            // Wait for the menu to actually render before deciding an item is
            // absent, otherwise the count below races the portal mount.
            await page.locator('[data-menubar-dropdown]').waitFor({ state: 'visible' });
            // The currently-selected layout's menu item is relabelled "Re-run
            // layout", so its description is absent — re-selecting the active
            // layout is a no-op we skip rather than hang waiting for it.
            const item = page.getByRole('menuitem', { name: layout, exact: true });
            if (!(await item.count())) {
                // No Escape handler on this menu; toggle the button to close it.
                await layoutBtn.click();
                return `switchLayout: skip (${layout} already active)`;
            }
            await item.click();
            await waitForLayoutToFinish(page);
            return `switchLayout: ${layout}`;
        },
        switchTab: async (page, r) => {
            const name = pick(TAB_OPTIONS, r)!;
            if (!(await page.getByRole('tab', { name, exact: true }).count())) {
                return `switchTab: skip (${name} absent)`;
            }
            await changeTab(page, name);
            return `switchTab: ${name}`;
        },
        tvSelectYAxis: async (page, r) => {
            if (!(await setTemporalView(page, true))) return 'tvSelectYAxis: skip (no timeline)';
            const row = pick(await getYAxisRows(page), r);
            if (!row) return 'tvSelectYAxis: skip (no rows)';
            await selectYAxisNode(page, row.name);
            return `tvSelectYAxis: ${row.name}`;
        },
        tvPinYAxis: async (page, r) => {
            if (!(await setTemporalView(page, true))) return 'tvPinYAxis: skip (no timeline)';
            const row = pick(await getYAxisRows(page), r);
            if (!row) return 'tvPinYAxis: skip (no rows)';
            await pinYAxisNode(page, row.name);
            return `tvPinYAxis: ${row.name}`;
        },
        tvToggleEdges: async (page) => {
            if (!(await setTemporalView(page, true))) return 'tvToggleEdges: skip (no timeline)';
            const hide = page.getByRole('button', { name: 'Hide edges' });
            if (await hide.count()) {
                await hide.click();
                return 'tvToggleEdges: hide';
            }
            const show = page.getByRole('button', { name: 'Show edges' });
            if (await show.count()) {
                await show.click();
                return 'tvToggleEdges: show';
            }
            return 'tvToggleEdges: skip (no button)';
        },
        tvToggleFilter: async (page) => {
            if (!(await setTemporalView(page, true))) return 'tvToggleFilter: skip (no timeline)';
            const on = page.getByRole('button', { name: 'Turn filter on' });
            const off = page.getByRole('button', { name: 'Turn filter off' });
            let label: string;
            if (await on.count()) {
                await on.click();
                label = 'tvToggleFilter: on';
            } else if (await off.count()) {
                await off.click();
                label = 'tvToggleFilter: off';
            } else {
                return 'tvToggleFilter: skip (no button)';
            }
            return label;
        },
    };

/**
 * Per-action model transitions. Only actions that change the modeled state need
 * an entry; everything else is identity on the node set (they may change
 * selection, layout or view chrome, but never which nodes exist).
 */
const TRANSITIONS: Partial<Record<ChaosActionType, Pick<ChaosCommand, 'check' | 'transition'>>> = {
    deleteNode: {
        check: (model, opts) => model.nodes.length > opts.minNodes,
        transition: (model, r) => {
            const id = pick(model.nodes, r);
            return id === undefined
                ? model
                : { ...model, nodes: model.nodes.filter((n) => n !== id) };
        },
    },
};

export const CHAOS_COMMANDS: ChaosCommand[] = (Object.keys(RUN) as ChaosActionType[]).map(
    (type) => ({
        type,
        run: RUN[type],
        // Default: always applicable, and leaves the node-set model untouched.
        check: TRANSITIONS[type]?.check ?? (() => true),
        transition: TRANSITIONS[type]?.transition ?? ((model) => model),
    }),
);

export const CHAOS_ACTION_TYPES = CHAOS_COMMANDS.map((c) => c.type);

/**
 * Assert the page is still healthy after a chaos step: the graph state still
 * resolves above the node floor, a graph or temporal-view surface is still
 * mounted, and no error alert has surfaced.
 */
export async function assertChaosInvariants(page: Page, opts: ChaosOptions): Promise<void> {
    const { nodes } = await getGraphState(page);
    expect(nodes.length).toBeGreaterThanOrEqual(opts.minNodes);

    const graphAlive = await page
        .locator('canvas')
        .first()
        .isVisible()
        .catch(() => false);
    expect(graphAlive || (await temporalViewVisible(page))).toBeTruthy();

    await expect(
        page.getByRole('alert').filter({ hasText: /error|something went wrong/i }),
    ).toHaveCount(0);
}
