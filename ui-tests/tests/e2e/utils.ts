import { expect, Locator, Page } from '@playwright/test';

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
