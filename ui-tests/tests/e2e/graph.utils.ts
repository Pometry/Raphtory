import { expect, Locator, Page } from '@playwright/test';
import { searchForEntity } from './search.utils';
import { waitForLayoutToFinish } from './utils';

interface G6NodeData {
    id: string;
    displayName: string;
    states?: string[];
    style?: {
        fill?: string;
        size?: number;
    };
}
interface G6EdgeData {
    id?: string;
    source: string;
    target: string;
}
type BrowserWindow = Window & {
    __TESTING_ENABLED__?: boolean;
    __G6_GRAPH__?: {
        getData(): { nodes: G6NodeData[]; edges: G6EdgeData[] };
        getElementPosition(id: string): [number, number];
        getViewportByCanvas(point: [number, number]): [number, number];
        getElementRenderStyle(id: string): Record<string, unknown>;
    };
};

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
    await page.waitForFunction(
        (name) => {
            const graph = (window as BrowserWindow).__G6_GRAPH__;
            return !!(
                graph &&
                graph.getData().nodes.some((n) => n.displayName === name)
            );
        },
        displayName,
        { timeout: 10000 },
    );

    const position = await page.evaluate((name) => {
        const graph = (window as BrowserWindow).__G6_GRAPH__;
        const node = graph?.getData().nodes.find((n) => n.displayName === name);
        if (!node || !graph) return null;
        const canvasPoint = graph.getElementPosition(node.id);
        const vp = graph.getViewportByCanvas(canvasPoint);
        return { x: vp[0], y: vp[1] };
    }, displayName);
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
    await page
        .locator('canvas')
        .nth(1)
        .click({ position, modifiers: options?.modifiers });
}

export async function doubleClickOnNode(page: Page, displayName: string) {
    const position = await getNodePosition(page, displayName);
    await page.locator('canvas').nth(1).dblclick({ position });
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
    await page.locator('canvas').nth(1).click({ position, button: 'right' });
}

/**
 * Simulate macOS ctrl+click — on macOS this fires a native `contextmenu`
 * event without going through G6's pointer pipeline. Caller is responsible
 * for skipping on non-webkit browsers.
 */
export async function ctrlClickOnNode(page: Page, displayName: string) {
    const position = await getNodePosition(page, displayName);
    await page
        .locator('canvas')
        .nth(1)
        .click({ position, modifiers: ['Control'] });
}

/** Click the midpoint of an edge identified by its src and dst node display names. */
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
    const position = { x: (src.x + dst.x) / 2, y: (src.y + dst.y) / 2 };
    await page
        .locator('canvas')
        .nth(1)
        .click({ position, modifiers: options?.modifiers });
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

export async function openTimeline(page: Page) {
    await page.getByRole('button', { name: 'Open timeline' }).click();
    // wait for animation to finish
    await page.waitForTimeout(300);
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
            await page.waitForTimeout(100);
            await page.getByText('Select Edge Layer').click();
            await page.getByRole('option', { name: target.layer }).click();
            break;
        case 'edge-instance':
            await page.getByLabel(target.label).click();
            await changeTab(page, 'Styling');
            break;
    }
}

export async function styleAndSave(
    page: Page,
    target: StyleTarget,
    styling: { colourValue?: string; size?: number },
    saveButtonText: string,
) {
    await selectStylingTarget(page, target);
    await fillInStyling(page, styling);
    await page
        .getByRole('button', { name: saveButtonText, exact: true })
        .click();
    await expect(page.getByText('Styling updated')).toBeVisible({
        timeout: 5000,
    });
}

// Asserts the styling tab's hex input shows expectedHex. Assumes the styling
// tab is already showing the relevant target.
export async function expectStylingHexInput(page: Page, expectedHex: string) {
    const hex = await page
        .locator('div')
        .filter({ hasText: /^Hex$/ })
        .getByRole('textbox')
        .inputValue();
    expect(hex.toLowerCase()).toBe(expectedHex.toLowerCase());
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
    nodes: {
        id: string;
        colour: string | undefined;
        size: number | undefined;
        badgeText: string | undefined;
    }[];
}

export async function getGraphState(page: Page): Promise<GraphState> {
    const handle = await page.waitForFunction(
        () => {
            const graph = (window as BrowserWindow).__G6_GRAPH__;
            if (!graph) return undefined;
            const data = graph.getData();
            const anyDisabled = data.nodes.some((n) =>
                n.states?.includes('disabled'),
            );
            return {
                highlighted: anyDisabled
                    ? data.nodes
                          .filter((n) => !n.states?.includes('disabled'))
                          .map((n) => ({ id: n.id }))
                    : [],
                selected: data.nodes
                    .filter((n) => n.states?.includes('selected'))
                    .map((n) => n.id),
                nodes: data.nodes.map((n) => {
                    let badgeText: string | undefined;
                    try {
                        const style = graph.getElementRenderStyle(n.id);
                        const badges = style?.badges;
                        if (Array.isArray(badges)) {
                            const textBadge = badges.find(
                                (b: Record<string, unknown>) => 'text' in b,
                            );
                            badgeText = textBadge?.text?.toString();
                        }
                    } catch {
                        // getElementRenderStyle may not be available
                    }
                    return {
                        id: n.id,
                        colour: n.style?.fill,
                        size: n.style?.size,
                        badgeText,
                    };
                }),
            };
        },
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
