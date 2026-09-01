import { expect, Locator, Page } from '@playwright/test';
import { searchForEntity } from './search.utils';
import { openTimeline } from './temporalview.utils';
import { waitForLayoutToFinish } from './utils';

/**
 * The canvas that receives pointer events. Sigma stacks several canvases
 * (`sigma-nodes`, `sigma-edges`, `sigma-edgeLabels`, `sigma-mouse`,
 * `sigma-hovers`); only `sigma-mouse` accepts pointer events.
 */
export function getInteractiveCanvas(page: Page): Locator {
    return page.locator('canvas.sigma-mouse');
}

interface SigmaGraphShape {
    nodes(): string[];
    edges(): string[];
    hasNode(id: string): boolean;
    getNodeAttribute(id: string, key: string): unknown;
    getEdgeAttribute(id: string, key: string): unknown;
    source(edge: string): string;
    target(edge: string): string;
}
interface SigmaInstanceShape {
    graph: SigmaGraphShape;
    sigma: {
        graphToViewport(p: { x: number; y: number }): { x: number; y: number };
    };
}
type BrowserWindow = Window & {
    __TESTING_ENABLED__?: boolean;
    __SIGMA__?: SigmaInstanceShape;
};

// Mirror the truncation rule applied by useGraphContext.ts's
// `truncateDisplayName` so a >20-char displayName matches its rendered
// labelText. Short names pass through unchanged.
function expectedLabelText(displayName: string): string {
    return displayName.length > 20
        ? displayName.slice(0, 20) + '...'
        : displayName;
}

export async function fitView(page: Page) {
    await page
        .getByRole('button', { name: 'Fit all nodes within visible region' })
        .click();
    await waitForLayoutToFinish(page);
}

async function getNodePosition(
    page: Page,
    displayName: string,
): Promise<{ x: number; y: number }> {
    const labelText = expectedLabelText(displayName);
    // Match by id or label — BTS overrides label to "<id>\nES: …LS: …"
    // which won't match the bare id callers pass.
    await page.waitForFunction(
        (name) => {
            const sigma = (window as BrowserWindow).__SIGMA__;
            return !!(
                sigma &&
                sigma.graph
                    .nodes()
                    .some(
                        (id) =>
                            id === name ||
                            sigma.graph.getNodeAttribute(id, 'label') === name,
                    )
            );
        },
        displayName,
        { timeout: 10000 },
    );

    const position = await page.evaluate(
        ({ name, label }) => {
            const sigma = (window as BrowserWindow).__SIGMA__;
            if (!sigma) return null;
            const id = sigma.graph
                .nodes()
                .find(
                    (nid) =>
                        nid === name ||
                        sigma.graph.getNodeAttribute(nid, 'label') === label,
                );
            if (id === undefined) return null;
            const x = sigma.graph.getNodeAttribute(id, 'x') as number;
            const y = sigma.graph.getNodeAttribute(id, 'y') as number;
            return sigma.sigma.graphToViewport({ x, y });
        },
        { name: displayName, label: labelText },
    );
    if (!position) {
        throw new Error(
            `Failed to get canvas position for node "${displayName}"`,
        );
    }
    return position;
}

export async function getNodePositions(
    page: Page,
    displayNames: string[],
): Promise<Record<string, { x: number; y: number }>> {
    const positions: Record<string, { x: number; y: number }> = {};
    for (const name of displayNames) {
        positions[name] = await getNodePosition(page, name);
    }
    return positions;
}

export async function clickOnNode(
    page: Page,
    displayName: string,
    options?: { modifiers?: ('Shift' | 'Control' | 'Meta' | 'Alt')[] },
) {
    const position = await getNodePosition(page, displayName);
    await getInteractiveCanvas(page).click({
        position,
        modifiers: options?.modifiers,
    });
}

export async function doubleClickOnNode(page: Page, displayName: string) {
    const position = await getNodePosition(page, displayName);
    await getInteractiveCanvas(page).dblclick({ position });
}

/** Click the first node normally, then Shift-click the rest to multi-select. */
export async function clickOnNodes(page: Page, displayNames: string[]) {
    if (displayNames.length === 0) return;
    await clickOnNode(page, displayNames[0]);
    for (const name of displayNames.slice(1)) {
        await clickOnNode(page, name, { modifiers: ['Shift'] });
    }
}

export async function rightClickOnNode(page: Page, displayName: string) {
    const position = await getNodePosition(page, displayName);
    await getInteractiveCanvas(page).click({ position, button: 'right' });
}

/**
 * Simulate macOS ctrl+click — on macOS this fires a native `contextmenu`
 * event without going through sigma's pointer pipeline. Caller is responsible
 * for skipping on non-webkit browsers.
 */
export async function ctrlClickOnNode(page: Page, displayName: string) {
    const position = await getNodePosition(page, displayName);
    await getInteractiveCanvas(page).click({
        position,
        modifiers: ['Control'],
    });
}

/** Click a point along an edge identified by its src and dst node display names.
 *  Tries several positions along the line in case the midpoint sits on top of a
 *  node or its label. After each click we re-query graph state and stop as soon
 *  as the edge appears selected. */
export async function clickOnEdge(
    page: Page,
    srcDisplayName: string,
    dstDisplayName: string,
    options?: { modifiers?: ('Shift' | 'Control' | 'Meta' | 'Alt')[] },
) {
    const [src, dst] = await Promise.all([
        getNodePosition(page, srcDisplayName),
        getNodePosition(page, dstDisplayName),
    ]);
    const canvas = getInteractiveCanvas(page);
    // Order tried: midpoint first (preserves existing behaviour for the common
    // case), then progressively closer to each endpoint where curves and label
    // overlaps deviate less from the straight line.
    const ratios = [0.5, 0.35, 0.65, 0.25, 0.75];
    for (const t of ratios) {
        const position = {
            x: src.x + (dst.x - src.x) * t,
            y: src.y + (dst.y - src.y) * t,
        };
        await canvas.click({ position, modifiers: options?.modifiers });
        const state = await getGraphState(page);
        if (state.selectedEdges.length > 0) return;
    }
}

export async function navigateToGraphPageBySearch(
    page: Page,
    entity:
        | {
              type: 'node';
              nodeName: string;
              nodeType: string;
          }
        | {
              type: 'edge';
              src: string;
              dst: string;
              layers: string[];
          },
) {
    await searchForEntity(page, entity);

    if (entity.type === 'node') {
        if (entity.nodeType === 'Person') {
            await page
                .getByRole('button', {
                    name: `${entity.nodeName} PERSON Age`,
                })
                .dblclick();
        } else if (entity.nodeType === 'Company') {
            await page
                .getByRole('button', {
                    name: `${entity.nodeName} COMPANY ID: ${entity.nodeName}`,
                })
                .dblclick();
        } else if (entity.nodeType === 'None') {
            await page
                .getByRole('button', {
                    name: `${entity.nodeName} ID: ${entity.nodeName}`,
                })
                .dblclick();
        }
    } else if (entity.type === 'edge') {
        await page
            .getByRole('button', {
                name: `${entity.src} - ${entity.dst} EDGE ${entity.layers.join('·')}`,
            })
            .dblclick();
    }

    await waitForLayoutToFinish(page);
}

export async function selectLayout(
    page: Page,
    layoutName: string,
    layoutTimeout?: number,
) {
    await page.getByRole('button', { name: 'Layout' }).click();
    await page
        .getByRole('menuitem', {
            name: layoutName,
            exact: true,
        })
        .click();
    await waitForLayoutToFinish(page, undefined, layoutTimeout);
}

export async function changeTab(page: Page, tabName: string) {
    await page.getByRole('tab', { name: tabName, exact: true }).click();
    // This is to wait for the animation for changing tabs to finish
    await page.waitForTimeout(500);
}

export async function setupGraphPage(
    page: Page,
    relativePath = 'graph/vanilla/event?initialNodes=%5B%5D',
) {
    await page.goto(`/${relativePath}`);
    await waitForLayoutToFinish(page);
    await openTimeline(page);
    return page;
}

export async function dragSlider({
    page,
    slider,
    root,
    sliderPosition,
}: {
    page: Page;
    slider: Locator;
    root: Locator;
    sliderPosition: number;
}) {
    const rootOffsetWidth = (await root.boundingBox())?.width;
    if (rootOffsetWidth === undefined) {
        throw Error('Could not get slider root offset!');
    }
    if (sliderPosition < 0 || sliderPosition > 1) {
        throw Error(
            'Provide a position to drag the slider to between 0 and 1.',
        );
    }
    await slider.dragTo(root, {
        force: true,
        targetPosition: { x: rootOffsetWidth * sliderPosition, y: 0 },
    });
    await slider.dragTo(root, {
        force: true,
        targetPosition: { x: rootOffsetWidth * sliderPosition, y: 0 },
    });
    // sometimes the slider label stays and blocks elements below it, this tries
    // to make it go away
    await page.mouse.move(0, 0);
}

export type StyleTarget =
    | { kind: 'node'; name: string }
    | { kind: 'node-type'; type: string }
    | { kind: 'edge'; src: string; dst: string; layer: string }
    | { kind: 'edge-instance'; label: string };

async function selectStylingTarget(page: Page, target: StyleTarget) {
    switch (target.kind) {
        case 'node':
            await clickOnNode(page, target.name);
            await changeTab(page, 'Styling');
            break;
        case 'node-type':
            await changeTab(page, 'Styling');
            await page.getByText('Select Node Type').click();
            await page.getByRole('option', { name: target.type }).click();
            break;
        case 'edge':
            await clickOnEdge(page, target.src, target.dst);
            await changeTab(page, 'Styling');
            // TODO: as soon as there are instanced where an edge has two layers, this will need to change
            await expect(
                page.getByRole('combobox', { name: 'Edge Layer' }),
            ).toContainText(target.layer);
            break;
        case 'edge-instance':
            await page.getByLabel(target.label).click();
            await changeTab(page, 'Styling');
            break;
    }
}

export async function style(
    page: Page,
    target: StyleTarget,
    styling: { colourValue?: string; size?: number },
) {
    await selectStylingTarget(page, target);
    await fillInStyling(page, styling);
}

export async function saveAs(page: Page, newName: string) {
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save as ...').click();
    await page.getByLabel('New Graph Name').fill(newName);
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);
}

export async function saveAsWithRandomName(
    page: Page,
    namespace: string,
): Promise<string> {
    const name = `${namespace}/test_${Math.random().toString(36).slice(2, 8)}`;
    await saveAs(page, name);
    return name;
}

export async function save(page: Page) {
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save changes').waitFor();
    await page.getByText('Save changes').click();
    // The save runs async behind the menu click. Wait for the Save button's
    // unsaved-changes dot to clear — it only does once the mutation has
    // completed AND the refetched server state matches the local one.
    // Returning earlier lets callers (e.g. page.reload()) abort the save
    // mid-flight, silently losing it.
    await expect(
        page.getByRole('button', { name: 'Save' }).locator('.MuiBadge-badge'),
    ).toHaveClass(/MuiBadge-invisible/, { timeout: 15000 });
}

// TODO: remove styleAndSave and styleAndSaveAs, we should above instead
export async function styleAndSaveAs(
    page: Page,
    target: StyleTarget,
    styling: { colourValue?: string; size?: number },
    newName: string,
) {
    await selectStylingTarget(page, target);
    await fillInStyling(page, styling);
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save as ...').click();
    await page.getByLabel('New Graph Name').fill(newName);
    await page.getByRole('button', { name: 'Confirm' }).click();
    await waitForLayoutToFinish(page);
}

export async function styleAndSave(
    page: Page,
    target: StyleTarget,
    styling: { colourValue?: string; size?: number },
    _saveButtonText: string, // FIXME: remove this? or maybe styleAndSave and  styleAndSaveAs entirely
) {
    await selectStylingTarget(page, target);
    await fillInStyling(page, styling);
    await page.getByRole('button', { name: 'Save' }).click();
    await page.getByText('Save changes').waitFor();
    await page.getByText('Save changes').click();
}

// Asserts the styling tab's hex input shows expectedHex. Assumes the styling
// tab is already showing the relevant target.
export async function expectStylingHexInput(page: Page, expectedHex: string) {
    // Poll instead of reading once: after a page reload the saved style loads
    // asynchronously, so the controlled colour picker can briefly show a stale
    // value before the query settles. toHaveValue retries until it matches (or
    // times out), which waits that race out — a one-shot inputValue() read was
    // flaky on faster runs.
    const hexInput = page
        .locator('div')
        .filter({ hasText: /^Hex$/ })
        .getByRole('textbox');
    await expect(hexInput).toHaveValue(new RegExp(`^${expectedHex}$`, 'i'));
}

// Selects the target on the styling tab and asserts the hex input matches
// expectedHex. The caller is responsible for reloading + waitForLayoutToFinish
// (and openTimeline for edge-instance targets) beforehand, so that any
// intermediate assertions about graph state can be checked between reload and
// re-select.
export async function expectStylingHex(
    page: Page,
    target: StyleTarget,
    expectedHex: string,
) {
    await selectStylingTarget(page, target);
    await expectStylingHexInput(page, expectedHex);
}

export async function deleteNodes(page: Page, nodeNames: string[]) {
    await clickOnNodes(page, nodeNames);
    await page.getByRole('button', { name: 'Delete selected (⌫)' }).click();
    // Defocus the delete tooltip so the ⌫ symbol does not interfere with
    // subsequent interactions or snapshots
    await page.mouse.move(0, 0);
    await waitForLayoutToFinish(page);
}

// Assumes you have RHS open and are on the styling tab of a particular entity
// (i.e. that entity is selected already or you are editing the styles of a node type)
export async function fillInStyling(
    page: Page,
    { colourValue, size }: { colourValue?: string; size?: number },
) {
    // in Chromium, the input needs to be cleared first or it will append the
    // value to the end of the existing value, which will then be ignored.
    if (colourValue !== undefined) {
        const colourInput = page
            .locator('div')
            .filter({ hasText: /^Hex$/ })
            .getByRole('textbox');
        await colourInput.click();
        await colourInput.fill('');
        await colourInput.fill(colourValue);
        // DebouncedSketch debounces onChange by 100ms before the typed hex
        // is written into temporaryStyles. If size is filled next it
        // synchronously enables the Save button, so a click can land before
        // the colour debounce fires and persist a style without the fill.
        await page.waitForTimeout(150);
    }

    if (size !== undefined) {
        const sizeInput = page.getByPlaceholder('Enter size');
        await sizeInput.fill('');
        await sizeInput.fill(size.toString());
    }
}

interface GraphState {
    highlighted: { id: string }[];
    selected: string[];
    selectedEdges: string[];
    nodes: {
        id: string;
        colour: string | undefined;
        size: number | undefined;
        badgeText: string | undefined;
    }[];
}

export async function getGraphState(
    page: Page,
    options: { allowEmpty?: boolean } = {},
): Promise<GraphState> {
    const allowEmpty = options.allowEmpty ?? false;
    const handle = await page.waitForFunction(
        (allowEmpty) => {
            const sigma = (window as BrowserWindow).__SIGMA__;
            if (!sigma) return undefined;
            const sg = sigma.graph;
            const nodeIds = sg.nodes();
            // Default: wait until the graph has loaded. allowEmpty for the
            // rare test that expects a deliberately blank canvas.
            if (nodeIds.length === 0 && !allowEmpty) return undefined;

            // SigmaScene's reconcile step writes `disabled` / `selected` as node and
            // edge attributes on sigma's graphology graph.
            const anyDisabled = nodeIds.some(
                (id) => sg.getNodeAttribute(id, 'disabled') === true,
            );
            const highlighted = anyDisabled
                ? nodeIds
                      .filter(
                          (id) => sg.getNodeAttribute(id, 'disabled') !== true,
                      )
                      .map((id) => ({ id }))
                : [];
            const selected = nodeIds.filter(
                (id) => sg.getNodeAttribute(id, 'selected') === true,
            );
            const selectedEdges = sg
                .edges()
                .filter((id) => sg.getEdgeAttribute(id, 'selected') === true);

            // Sigma stores size as size/1.6; multiply back so callers see
            // the same pre-divisor value the store used to expose.
            const SIGMA_NODE_SIZE_DIVISOR = 1.6;

            return {
                highlighted,
                selected,
                selectedEdges,
                nodes: nodeIds.map((id) => {
                    let badgeText: string | undefined;
                    const badges = sg.hasNode(id)
                        ? sg.getNodeAttribute(id, 'badges')
                        : undefined;
                    if (Array.isArray(badges)) {
                        const textBadge = badges.find(
                            (b: Record<string, unknown>) => 'text' in b,
                        );
                        badgeText = textBadge?.text?.toString();
                    }
                    const rawSize = sg.getNodeAttribute(id, 'size') as
                        | number
                        | undefined;
                    return {
                        id,
                        colour: sg.getNodeAttribute(id, 'color') as
                            | string
                            | undefined,
                        size:
                            rawSize !== undefined
                                ? rawSize * SIGMA_NODE_SIZE_DIVISOR
                                : undefined,
                        badgeText,
                    };
                }),
            };
        },
        allowEmpty,
        { timeout: 10000 },
    );
    return handle.jsonValue() as Promise<GraphState>;
}

// The current color picker's hex input box needs special handling for
// chromium because in chromium, not clearing or clicking it before calling
// fill will cause the fill to append onto the previous existing color,
// rather than clearing and filling in the new contents.
export async function fillColorPickerHexInput(page: Page, newValue: string) {
    const colorTextbox = page
        .locator('div')
        .filter({ hasText: /^Hex$/ })
        .getByRole('textbox');
    await colorTextbox.click();
    await colorTextbox.fill(newValue);
}
