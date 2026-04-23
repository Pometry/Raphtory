import { expect, Page, test } from '@playwright/test';
import { navigateToSavedGraphBySavedGraphsTable } from './utils';

async function setupGraphPage(
    page: Page,
    relativePath = 'graph?graphSource=vanilla/event&initialNodes=%5B%5D',
) {
    await page.goto(`/${relativePath}`);
    await page.waitForSelector('text=Ben');
    return page;
}

async function hoverEdgeAndExpectTooltip(
    page: Page,
    selector: string,
    expectedText: string,
) {
    const line = page.locator(selector).first();
    await expect(line).toHaveCount(1);

    const box = await line.boundingBox();
    if (!box) throw new Error(`Element ${selector} is not visible or rendered`);
    await page.mouse.move(250, 200);
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await expect(page.getByText(expectedText)).toBeVisible();
}

test('Close temporal view button and open again', async ({ page }) => {
    await setupGraphPage(page);
    await page.waitForSelector('text=Ben');
    await page
        .locator(
            '.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary',
        )
        .nth(6)
        .click();

    await expect(page.locator('text="Ben"')).toBeHidden();

    await page
        .locator(
            '.MuiButtonBase-root.MuiButton-root.MuiButton-text.MuiButton-textPrimary',
        )
        .nth(6)
        .click();
    await page.waitForSelector('text=Ben');
    await expect(page.locator('text="Ben"')).toBeVisible();
});

test('Temporal view hover over edges', async ({ page }) => {
    await setupGraphPage(page);
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(20) > line:nth-child(6)',
        'Ben -> meets -> Hamza',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(19) > line:nth-child(6)',
        'Ben -> meets -> Pedro',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(22) > line:nth-child(6)',
        'Hamza -> founds -> Pometry',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(23) > line:nth-child(6)',
        'Hamza -> meets -> Pedro',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(24) > line:nth-child(6)',
        'Hamza -> meets -> Pedro',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(25) > line:nth-child(6)',
        'Hamza -> transfers -> Pedro',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(18) > line:nth-child(6)',
        'Pedro -> transfers -> Hamza',
    );
    await hoverEdgeAndExpectTooltip(
        page,
        'g:nth-child(21) > line:nth-child(6)',
        'Ben -> transfers -> Hamza',
    );
});

test('Pin node and highlight', async ({ page }) => {
    navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');
    await page.waitForSelector('text=Pedro');
    await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .locator('image')
        .click();
    expect(await page.screenshot()).toMatchSnapshot('pinnode.png', {
        maxDiffPixels: 3500,
    });
    await page
        .locator('g')
        .filter({ hasText: /^Pometry$/ })
        .locator('circle')
        .click();
    expect(await page.screenshot()).toMatchSnapshot(
        'highlightedandpinned.png',
        {
            maxDiffPixels: 3500,
        },
    );
});

test('Zoom into timeline view', async ({ page }) => {
    await setupGraphPage(page);
    await page.waitForSelector('text="Pometry"');
    const element = page.locator(
        '.MuiPaper-root.MuiPaper-elevation.MuiPaper-rounded.MuiPaper-elevation1.css-gnkdhv-MuiPaper-root',
    );
    await expect(element).toBeVisible();
    const box = await element.boundingBox();
    if (box !== null) {
        const offsetX = box.width / 2;
        const offsetY = box.height / 2;

        await page.mouse.move(box.x + offsetX, box.y + offsetY);
        await page.mouse.wheel(0, -2000); // scroll up (zoom in)
        await expect(page.getByText('Pedro')).toBeHidden();
    }
});

test('Highlight node from timeline view', async ({ page }) => {
    navigateToSavedGraphBySavedGraphsTable(page, 'vanilla', 'event');
    await page
        .locator('g')
        .filter({ hasText: /^Ben$/ })
        .locator('circle')
        .click();
    await page.getByRole('tab', { name: 'Selected' }).click();
    await page.getByRole('button', { name: 'Node Statistics' }).click();
    await page.getByRole('row', { name: 'Node Type Person' }).isVisible();
    await page
        .locator('g')
        .filter({ hasText: /^Hamza$/ })
        .locator('circle')
        .click({
            modifiers: ['Shift'],
        });
    await page.getByRole('button', { name: 'Node Statistics' }).click();
    await page.getByRole('row', { name: 'Node Type Person' }).isVisible();
});
