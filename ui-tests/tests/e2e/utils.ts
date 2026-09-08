import { expect, Locator, Page } from '@playwright/test';

const NAVBAR_STORAGE_KEY = 'pometry-navbar-expanded';

/**
 * Seed the navbar expanded state in localStorage before the app mounts. The
 * init script runs before every page script on every navigation in the
 * context, but is guarded so it only writes when the key is unset — once the
 * app (or a click) has stored a value, the script becomes a no-op and reloads
 * preserve the real state. Caller is responsible for the subsequent navigation.
 */
export async function setNavbarExpanded(page: Page, expanded: boolean): Promise<void> {
    await page.addInitScript(
        ({ key, value }) => {
            if (window.localStorage.getItem(key) !== null) return;
            window.localStorage.setItem(key, value);
        },
        { key: NAVBAR_STORAGE_KEY, value: JSON.stringify(expanded) },
    );
}

export async function waitForLayoutToFinish(
    page: Page,
    queryTimeout?: number,
    layoutTimeout?: number,
) {
    await expect(page.getByRole('progressbar', { name: 'Querying for graph...' })).toBeHidden({
        timeout: queryTimeout,
    });
    await expect(page.getByRole('progressbar', { name: 'Computing layout...' })).toBeHidden({
        timeout: layoutTimeout,
    });
    // this extra timeout is to account for the animation
    await page.waitForTimeout(2000);
}

/**
 * Walk through the pages of a paginated table until `target` is found, then
 * click it. Throws if not found after exhausting all pages. The next-page
 * button defaults to the saved-graphs table's "Next page" button; pass a
 * different `nextPageButton` for tables that label theirs differently (e.g.
 * the QueryBuilder graph picker uses lowercase "next page").
 */
export async function clickAfterPaginating(
    page: Page,
    target: Locator,
    description: string,
    nextPageButtonProvided?: Locator,
): Promise<void> {
    const nextPageButton =
        nextPageButtonProvided ?? page.getByRole('button', { name: 'Next page', exact: true });
    const MAX_PAGES = 100;
    const PAGE_LOAD_TIMEOUT = 5000;
    for (let i = 0; i < MAX_PAGES; i++) {
        // Wait for the target to appear on this page or, failing that, for the
        // next-page button to become reliably interactive. The picker is async
        // and starts with rows hidden + next-page disabled while data loads;
        // checking immediately would incorrectly conclude "not found".
        const found = await target
            .waitFor({ state: 'visible', timeout: PAGE_LOAD_TIMEOUT })
            .then(() => true)
            .catch(() => false);
        if (found) {
            await target.click();
            return;
        }
        // Target didn't appear on this page within the timeout. Wait briefly
        // for the next-page button to settle before deciding whether more
        // pages are available.
        const nextPageVisible = await nextPageButton
            .waitFor({ state: 'visible', timeout: PAGE_LOAD_TIMEOUT })
            .then(() => true)
            .catch(() => false);
        if (!nextPageVisible || (await nextPageButton.isDisabled())) {
            throw new Error(`${description} not found after ${i + 1} pages`);
        }
        await nextPageButton.click();
    }
    throw new Error(`${description} not found after ${MAX_PAGES} pages`);
}
