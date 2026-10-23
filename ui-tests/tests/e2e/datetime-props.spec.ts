import { expect, test } from '../fixtures';
import { changeTab, clickOnEdge, clickOnNode, fitView } from './graph.utils';
import { waitForLayoutToFinish } from './utils';

// West of UTC, so a datetime near midnight UTC lands on a different local day,
// and a naive one would too if it were wrongly treated as UTC.
test.use({ timezoneId: 'America/New_York' });

test.beforeEach(async ({ page }) => {
    await page.goto('/graph/datetimes/props');
    await waitForLayoutToFinish(page);
    await fitView(page);
});

test('node datetime properties render as local datetimes', async ({ page }) => {
    await changeTab(page, 'Selected');
    await clickOnNode(page, 'Alice');
    await expect(page.getByRole('region', { name: 'Entity details' })).toMatchAriaSnapshot(`
        - paragraph: Aware At
        - paragraph: 31 Dec 2023, 22:00:00
        - paragraph: Naive At
        - paragraph: 1 Feb 2024, 00:30:00
        - paragraph: Aware List
        - paragraph: 1 Mar 2024, 07:00:00, 1 Apr 2024, 08:00:00
        - paragraph: Epoch Ms
        - paragraph: "1717200000000"
    `);
});

test('edge datetime properties render as datetimes', async ({ page }) => {
    await changeTab(page, 'Selected');
    await clickOnEdge(page, 'Alice', 'Bob');
    await expect(page.getByRole('region', { name: 'Entity details' })).toMatchAriaSnapshot(`
        - paragraph: Sent At
        - paragraph: 1 May 2024, 08:00:00
    `);
});

test('graph datetime metadata renders as datetimes', async ({ page }) => {
    await changeTab(page, 'Overview');
    await expect(page.locator('body')).toMatchAriaSnapshot(`
        - paragraph: Created On
        - paragraph: 1 Jun 2024, 08:00:00
    `);
});
