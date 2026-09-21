import { expect, Page, test } from '@playwright/test';

import { setNavbarExpanded } from './utils';

// Cmd-click (mac) / ctrl-click (elsewhere) jumps to the schema type;
// multi-cursor lives on alt-click
const JUMP_MODIFIER = process.platform === 'darwin' ? ('Meta' as const) : ('Control' as const);

const TABS_STORAGE_KEY = 'pometry-playground-tabs';

/**
 * Seed the playground's persisted tab state before the app mounts, so tests
 * get exact editor content without fighting CodeMirror's auto-closing
 * brackets. Restoring from this state is itself part of what's under test.
 */
async function seedQuery(page: Page, query: string, variables = '{}'): Promise<void> {
    await page.addInitScript(
        ({ key, q, v }) => {
            window.localStorage.setItem(
                key,
                JSON.stringify({
                    tabs: [
                        {
                            id: 'tab-1',
                            title: 'Query',
                            query: q,
                            variables: v,
                            result: '',
                            headers: [],
                        },
                    ],
                    activeId: 'tab-1',
                }),
            );
        },
        { key: TABS_STORAGE_KEY, q: query, v: variables },
    );
}

function queryEditor(page: Page) {
    return page.locator('[aria-label="Query editor"]');
}

async function editorText(page: Page): Promise<string> {
    return await queryEditor(page).locator('.cm-content').innerText();
}

function breadcrumbs(page: Page) {
    return page.getByRole('navigation', { name: 'Schema breadcrumbs' });
}

async function waitForSchema(page: Page): Promise<void> {
    await expect(page.getByRole('tab', { name: 'Schema', exact: true })).toBeVisible();
}

test('Tab inserts at the cursor instead of indenting the line', async ({ page }) => {
    await seedQuery(page, '{\n  timestamp\n}');
    await page.goto('/playground');

    await queryEditor(page).getByText('timestamp').click();
    await page.keyboard.press('End');
    await page.keyboard.press('Tab');

    expect(await editorText(page)).toContain('timestamp\t');
    // And focus must remain in the editor — Tab typed, not navigated
    await expect(queryEditor(page).locator('.cm-content')).toBeFocused();
});

test('Tab moves focus out of the read-only results editor', async ({ page }) => {
    await page.goto('/playground');

    const results = page.locator('[aria-label="Results editor"]');
    await results.locator('.cm-content').click();
    await expect(results.locator('.cm-content')).toBeFocused();

    await page.keyboard.press('Tab');
    await expect(results.locator('.cm-content')).not.toBeFocused();
});

test('Cmd/ctrl-click on a field jumps to it without adding a cursor', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    await expect(breadcrumbs(page)).toBeHidden();
    await queryEditor(page)
        .getByText('namespaces')
        .click({ modifiers: [JUMP_MODIFIER] });
    await expect(breadcrumbs(page)).toBeVisible();
    await expect(queryEditor(page).locator('.cm-cursor')).toHaveCount(1);
});

test('Alt-click adds a cursor (multi-cursor gesture)', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n  root\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const editor = queryEditor(page);
    await editor.getByText('namespaces').click();
    await editor.getByText('root').click({ modifiers: ['Alt'] });

    await expect(editor.locator('.cm-cursor')).toHaveCount(2);
    await expect(breadcrumbs(page)).toBeHidden();
});

test('Variables editor shares the gestures: Tab at cursor, alt-click multi-cursor', async ({
    page,
}) => {
    await seedQuery(page, '{\n  namespaces\n}', '{\n  "path": "a",\n  "name": "b"\n}');
    await page.goto('/playground');

    await page.getByRole('button', { name: 'Variables' }).click();
    const variables = page.locator('[aria-label="Variables editor"]');

    await variables.getByText('path').click();
    await page.keyboard.press('End');
    await page.keyboard.press('Tab');
    expect(await variables.locator('.cm-content').innerText()).toContain('"path": "a",\t');

    await variables.getByText('name').click({ modifiers: ['Alt'] });
    await expect(variables.locator('.cm-cursor')).toHaveCount(2);
});

test('The jump underline clears when the pointer leaves the editor', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const editor = queryEditor(page);
    await page.keyboard.down(JUMP_MODIFIER);
    await editor.getByText('namespaces').hover();
    await expect(editor.locator('.cm-jumpTarget')).toHaveText('namespaces');

    // Still holding the modifier, leave the editor entirely
    await page.locator('[aria-label="Results editor"]').hover();
    await expect(editor.locator('.cm-jumpTarget')).toHaveCount(0);
    await page.keyboard.up(JUMP_MODIFIER);
});

test('Holding the jump modifier underlines the target under the pointer', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const editor = queryEditor(page);
    // Hovering without the modifier shows nothing
    await editor.getByText('namespaces').hover();
    await expect(editor.locator('.cm-jumpTarget')).toHaveCount(0);

    await page.keyboard.down(JUMP_MODIFIER);
    await editor.getByText('namespaces').hover();
    const target = editor.locator('.cm-jumpTarget');
    await expect(target).toHaveText('namespaces');
    // Link-styled: brand pink, not the syntax-highlight color
    await expect(target).toHaveCSS('color', 'rgb(227, 6, 122)');

    // Releasing the modifier clears the affordance without mouse movement
    await page.keyboard.up(JUMP_MODIFIER);
    await expect(editor.locator('.cm-jumpTarget')).toHaveCount(0);
});

test('Cmd/ctrl-click on empty space after a line does not jump', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const line = queryEditor(page).locator('.cm-line', { hasText: 'namespaces' });
    const box = await line.boundingBox();
    if (!box) throw new Error('editor line not rendered');
    await line.click({
        position: { x: box.width - 10, y: box.height / 2 },
        modifiers: [JUMP_MODIFIER],
    });

    await expect(breadcrumbs(page)).toBeHidden();
});

test('The jump underline does not appear past the end of a line', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const editor = queryEditor(page);
    const line = editor.locator('.cm-line', { hasText: 'namespaces' });
    const box = await line.boundingBox();
    if (!box) throw new Error('editor line not rendered');

    await page.keyboard.down(JUMP_MODIFIER);
    // Hover the empty space after the text: posAtCoords clamps this to the
    // nearest position, which must not underline the trailing token
    await line.hover({ position: { x: box.width - 10, y: box.height / 2 } });
    await expect(editor.locator('.cm-jumpTarget')).toHaveCount(0);

    // Sanity check that the same hover on the text itself does underline —
    // otherwise the assertion above passes vacuously
    await editor.getByText('namespaces').hover();
    await expect(editor.locator('.cm-jumpTarget')).toHaveText('namespaces');
    await page.keyboard.up(JUMP_MODIFIER);
});

test('A consumed schema jump does not replay when the panel remounts', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    await queryEditor(page)
        .getByText('namespaces')
        .click({ modifiers: [JUMP_MODIFIER] });
    await expect(breadcrumbs(page)).toBeVisible();

    // Return to the root, then remount the explorer via an Examples round-trip
    await page.getByRole('button', { name: 'Go back' }).click();
    await expect(breadcrumbs(page)).toBeHidden();
    await page.getByRole('tab', { name: 'Examples' }).click();
    await page.getByRole('tab', { name: 'Schema', exact: true }).click();

    // The old jump must not re-navigate the explorer
    await expect(breadcrumbs(page)).toBeHidden();
});

test('Repeated jumps rewind the breadcrumb trail; back and crumbs walk it', async ({ page }) => {
    await seedQuery(page, '{\n  namespaces\n  root\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    const editor = queryEditor(page);
    const crumbs = async () =>
        ((await breadcrumbs(page).textContent()) ?? '')
            .split('›')
            // The nav's text also contains the back button's ← arrow
            .map((s) => s.replace('←', '').trim())
            .filter(Boolean);

    await editor.getByText('namespaces').click({ modifiers: [JUMP_MODIFIER] });
    await expect(breadcrumbs(page)).toBeVisible();
    const afterFirstJump = await crumbs();
    expect(afterFirstJump).toHaveLength(2);

    // A second jump to a new type extends the trail
    await editor.getByText('root').click({ modifiers: [JUMP_MODIFIER] });
    await expect(async () => expect(await crumbs()).toHaveLength(3)).toPass();
    const afterSecondJump = await crumbs();
    expect(new Set(afterSecondJump).size).toBe(3);

    // Jumping back to an already-visited type rewinds the trail instead of
    // appending — repeated jumps must never grow duplicates
    await editor.getByText('namespaces').click({ modifiers: [JUMP_MODIFIER] });
    await expect(async () => expect(await crumbs()).toEqual(afterFirstJump)).toPass();
    await editor.getByText('namespaces').click({ modifiers: [JUMP_MODIFIER] });
    expect(await crumbs()).toEqual(afterFirstJump);

    // Back button pops one level at a time down to the root
    await editor.getByText('root').click({ modifiers: [JUMP_MODIFIER] });
    await expect(async () => expect(await crumbs()).toEqual(afterSecondJump)).toPass();
    await page.getByRole('button', { name: 'Go back' }).click();
    expect(await crumbs()).toEqual(afterFirstJump);

    // Clicking an earlier crumb truncates the trail to it (root hides the nav)
    await editor.getByText('root').click({ modifiers: [JUMP_MODIFIER] });
    await expect(async () => expect(await crumbs()).toHaveLength(3)).toPass();
    const rootCrumb = afterSecondJump[0];
    await breadcrumbs(page).getByText(rootCrumb, { exact: true }).click();
    await expect(breadcrumbs(page)).toBeHidden();
});

test('Clicking a self-referential field highlights it instead of dead-clicking', async ({
    page,
}) => {
    await page.goto('/playground');
    await waitForSchema(page);

    // Jump to Edge, whose at/before/window fields return Edge itself
    await page.getByRole('textbox', { name: 'Search types and fields' }).fill('Edge');
    await page.getByRole('option', { name: 'Edge', exact: true }).click();
    await expect(breadcrumbs(page)).toContainText('Edge');
    const trail = (await breadcrumbs(page).textContent()) ?? '';

    const atRow = page.getByRole('button', { name: /^at\b/ }).first();
    await atRow.click();
    // Park the pointer elsewhere so the hover style can't mask the highlight
    await page.mouse.move(0, 0);
    await expect(breadcrumbs(page)).toHaveText(trail);
    await expect(atRow).toHaveCSS('background-color', 'rgba(227, 6, 122, 0.08)');
});

test('Prettify keeps a comment on the later of two same-named fields', async ({ page }) => {
    await seedQuery(
        page,
        '{\n  nodes { name }\n  edges {\n    # count them\n    nodes { list }\n  }\n}',
    );
    await page.goto('/playground');

    await page.getByRole('button', { name: 'Prettify query' }).click();

    const lines = (await editorText(page)).split('\n').map((l) => l.trim());
    const commentAt = lines.indexOf('# count them');
    expect(commentAt).toBeGreaterThan(-1);
    expect(lines[commentAt - 1]).toBe('edges {');
    expect(lines[commentAt + 1]).toBe('nodes {');
});

test('Typed query content survives a reload', async ({ page }) => {
    await page.goto('/playground');

    await queryEditor(page).locator('.cm-content').click();
    await page.keyboard.press('ControlOrMeta+a');
    await page.keyboard.type('# my scratch note');
    // Content saves are debounced at 500ms
    await page.waitForTimeout(800);

    await page.reload();
    expect(await editorText(page)).toContain('# my scratch note');
});

test('Argument suggestions pop up right after typing the opening paren', async ({ page }) => {
    await seedQuery(page, '{\n  graph\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    await queryEditor(page).getByText('graph').click();
    await page.keyboard.press('End');
    await page.keyboard.type('(');

    const tooltip = page.locator('.cm-tooltip-autocomplete');
    await expect(tooltip).toBeVisible();
    await expect(tooltip.getByRole('option', { name: /^path/ })).toBeVisible();
});

test('Suggestions survive a space after the opener', async ({ page }) => {
    await seedQuery(page, '{\n  graph\n}');
    await page.goto('/playground');
    await waitForSchema(page);

    await queryEditor(page).getByText('graph').click();
    await page.keyboard.press('End');
    // The space would dismiss the popup the paren opened; the trigger
    // must look back across it and reopen
    await page.keyboard.type('( ');

    const tooltip = page.locator('.cm-tooltip-autocomplete');
    await expect(tooltip).toBeVisible();
    await expect(tooltip.getByRole('option', { name: /^path/ })).toBeVisible();
});

test('Expanded navbar sits beside the playground instead of covering it', async ({ page }) => {
    await setNavbarExpanded(page, true);
    await page.goto('/playground');

    // Stays expanded here: links keep their labels
    const searchLink = page.getByRole('link', { name: 'Search', exact: true });
    await expect(searchLink).toHaveText('Search');

    // The playground's left panel starts where the navbar ends
    const navBox = await page.getByRole('navigation').boundingBox();
    const schemaTab = page.getByRole('tab', { name: /Schema/ });
    await expect(schemaTab).toBeVisible();
    const tabBox = await schemaTab.boundingBox();
    expect(tabBox!.x).toBeGreaterThanOrEqual(navBox!.x + navBox!.width);

    // Toggling on the playground persists like anywhere else
    await page.getByRole('button', { name: 'Collapse', exact: true }).click();
    await expect(searchLink).not.toHaveText('Search');
    await page.goto('/');
    await expect(searchLink).not.toHaveText('Search');
});
