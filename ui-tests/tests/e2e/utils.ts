import { expect, Locator, Page } from '@playwright/test';

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

const NAVBAR_STORAGE_KEY = 'pometry-navbar-expanded';

/**
 * Seed the navbar expanded state in localStorage before the app mounts. The
 * init script runs before every page script on every navigation in the
 * context, but is guarded so it only writes when the key is unset — once the
 * app (or a click) has stored a value, the script becomes a no-op and reloads
 * preserve the real state. Caller is responsible for the subsequent navigation.
 */
export async function setNavbarExpanded(
    page: Page,
    expanded: boolean,
): Promise<void> {
    await page.addInitScript(
        ({ key, value }) => {
            if (window.localStorage.getItem(key) !== null) return;
            window.localStorage.setItem(key, value);
        },
        { key: NAVBAR_STORAGE_KEY, value: JSON.stringify(expanded) },
    );
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

/**
 * Get a clip rectangle for screenshotting the region around a node.
 * Returns a square region centered on the node's viewport position.
 */
export async function getNodeScreenshotClip(
    page: Page,
    displayName: string,
    padding = 40,
): Promise<{ x: number; y: number; width: number; height: number }> {
    const position = await getNodePosition(page, displayName);
    return {
        x: Math.max(0, position.x - padding),
        y: Math.max(0, position.y - padding),
        width: padding * 2,
        height: padding * 2,
    };
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

export async function fillInCondition(
    page: Page,
    condition: {
        name?: string;
        op?: { current: string; new: string };
        value?: string;
        blur?: boolean;
    },
) {
    if (condition.name !== undefined) {
        await page.getByRole('button', { name: 'Add' }).click();
        await page.getByRole('menuitem', { name: condition.name }).click();
        await expect(page.locator('.MuiMenu-root')).toBeHidden();
    }
    if (condition.op !== undefined) {
        await page.getByText(condition.op.current).click();
        await expect(
            page.getByRole('option', { name: condition.op.new }),
        ).toBeVisible();
        await page.getByRole('option', { name: condition.op.new }).click();
        // Wait for condition dropdown to close
        await expect(page.locator('.MuiMenu-root')).toBeHidden();
        await expect(page.getByText(condition.op.new)).toBeVisible();
    }
    if (condition.value !== undefined) {
        const input = page.getByPlaceholder('Value').last();
        await input.fill(condition.value);
        if (condition.blur) {
            await input.blur();
        }
        await page.waitForTimeout(1000);
    }
}

export async function searchForEntity(
    page: Page,
    entity:
        | {
              type: 'node';
              nodeType: string;
              graph?: string;
              conditions?: {
                  name: string;
                  op?: { current: string; new: string };
                  value?: string;
                  blur?: boolean;
              }[];
          }
        | {
              type: 'edge';
              graph?: string;
              src?: string;
              dst?: string;
              layers?: string[];
          },
    options?: { search?: boolean },
) {
    const graph = entity.graph ?? 'event';
    await page.goto('/search');
    await selectGraphInQueryBuilder(page, {
        namespace: 'vanilla',
        graphName: graph,
    });
    await page
        .getByRole('button', {
            name: 'Confirm',
        })
        .click();
    if (entity.type === 'node') {
        await page.getByRole('combobox', { name: 'Select type' }).click();
        await page.getByRole('option', { name: entity.nodeType }).click();
        await expect(page.getByText(entity.nodeType).first()).toBeVisible();
        for (const condition of entity.conditions ?? []) {
            await fillInCondition(page, condition);
        }
    } else if (entity.type === 'edge') {
        await page.getByRole('combobox').filter({ hasText: 'Entity' }).click();
        await page.getByRole('option', { name: 'Relationship' }).click();
        if (entity.src !== undefined) {
            await page.getByRole('textbox', { name: 'Source ID' }).click();
            await page
                .getByRole('textbox', { name: 'Source ID' })
                .fill(entity.src);
        }
        if (entity.dst !== undefined) {
            await page.getByRole('textbox', { name: 'Destination ID' }).click();
            await page
                .getByRole('textbox', { name: 'Destination ID' })
                .fill(entity.dst);
        }
        for (const layer of entity.layers ?? []) {
            await page.getByRole('combobox', { name: 'Layers' }).click();
            await page.getByRole('option', { name: layer }).click();
        }
    }
    if (options?.search !== false) {
        await page.getByRole('button', { name: 'Search', exact: true }).click();
        await expect(page.getByText('Start Your Search')).toBeHidden();
        await expect(page.getByRole('progressbar')).toBeHidden();
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

/**
 * Walk through the pages of a paginated table until `target` is found, then
 * click it. Throws if not found after exhausting all pages. The next-page
 * button defaults to the saved-graphs table's "Next page" button; pass a
 * different `nextPageButton` for tables that label theirs differently (e.g.
 * the QueryBuilder graph picker uses lowercase "next page").
 */
async function clickAfterPaginating(
    page: Page,
    target: Locator,
    description: string,
    nextPageButtonProvided?: Locator,
): Promise<void> {
    const nextPageButton =
        nextPageButtonProvided ??
        page.getByRole('button', { name: 'Next page', exact: true });
    const MAX_PAGES = 100;
    for (let i = 0; i < MAX_PAGES; i++) {
        await page.waitForTimeout(200);
        await nextPageButton.isVisible();
        if (await target.isVisible()) {
            await target.click();
            return;
        }
        if (await nextPageButton.isDisabled()) {
            throw new Error(`${description} not found after ${i + 1} pages`);
        }
        await nextPageButton.click();
    }
    throw new Error(`${description} not found after ${MAX_PAGES} pages`);
}

/**
 * Open the QueryBuilder graph picker, navigate into the given namespace, and
 * click the named graph. Paginates through both the namespace list and the
 * graph list when test fixtures push the target onto a later page. Does NOT
 * click "Confirm" — leave that to the caller.
 */
export async function selectGraphInQueryBuilder(
    page: Page,
    { namespace, graphName }: { namespace: string; graphName: string },
): Promise<void> {
    const nextPageButton = page.getByRole('button', { name: 'next page' });
    await page.getByRole('button', { name: 'Select a graph' }).click();
    const namespaceRow = page.getByRole('row', {
        name: new RegExp(`^${namespace}$`),
    });
    await clickAfterPaginating(
        page,
        namespaceRow,
        `Namespace "${namespace}"`,
        nextPageButton,
    );
    const graphCell = page.getByRole('cell', {
        name: graphName,
        exact: true,
    });
    await clickAfterPaginating(
        page,
        graphCell,
        `Graph "${graphName}"`,
        nextPageButton,
    );
}

export async function clickSavedGraphsGraph(page: Page, graphName: string) {
    const target = page.getByRole('button', {
        name: new RegExp(`^${graphName} GRAPH`),
    });
    await clickAfterPaginating(page, target, `Graph "${graphName}"`);
}

export async function clickSavedGraphsFolder(page: Page, folderName: string) {
    const target = page.getByRole('button', {
        name: new RegExp(`^${folderName} FOLDER Click to browse`),
    });
    await clickAfterPaginating(page, target, `Folder "${folderName}"`);
}

export async function navigateToSavedGraphBySavedGraphsTable(
    page: Page,
    folderName: string,
    graphName: string,
) {
    await page.goto('/saved-graphs');
    await page.waitForLoadState('networkidle');
    if (await page.getByText('Welcome to Explorations').isVisible()) {
        throw new Error('No saved graphs exist!');
    }
    await clickSavedGraphsFolder(page, folderName);
    await clickSavedGraphsGraph(page, graphName);
    await page.getByRole('link', { name: 'Open' }).click();
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

export async function waitForLayoutToFinish(
    page: Page,
    queryTimeout?: number,
    layoutTimeout?: number,
) {
    await expect(
        page.getByRole('progressbar', { name: 'Querying for graph...' }),
    ).toBeHidden({
        timeout: queryTimeout,
    });
    await expect(
        page.getByRole('progressbar', { name: 'Computing layout...' }),
    ).toBeHidden({
        timeout: layoutTimeout,
    });
    // this extra timeout is to account for the animation
    await page.waitForTimeout(2000);
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

export async function toggleAccordion(page: Page, name: string) {
    await page.getByRole('button', { name, exact: true }).click();
    await page.waitForTimeout(500); // Accordion animation
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

const GRAPHQL_URL = process.env.GRAPHQL_URL ?? 'http://localhost:1736';

export async function graphqlMutation(
    query: string,
    variables?: Record<string, unknown>,
): Promise<unknown> {
    const response = await fetch(GRAPHQL_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, variables }),
    });
    if (!response.ok) {
        throw new Error(`GraphQL request failed: ${response.statusText}`);
    }
    const result = await response.json();
    if (result.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
    }
    return result.data;
}

export async function copyGraph(path: string, newPath: string): Promise<void> {
    await graphqlMutation(
        'mutation($path: String!, $newPath: String!) { copyGraph(path: $path, newPath: $newPath) }',
        { path, newPath },
    );
}

export async function deleteGraph(path: string): Promise<void> {
    await graphqlMutation(
        'mutation($path: String!) { deleteGraph(path: $path) }',
        { path },
    );
}
