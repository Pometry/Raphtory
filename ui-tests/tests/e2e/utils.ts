import { expect, Page } from '@playwright/test';

export async function fillInCondition(
    page: Page,
    condition: {
        op?: { current: string; new: string };
        value?: string;
    },
) {
    if (condition.op !== undefined) {
        await page
            .getByRole('combobox', { name: condition.op.current })
            .click();
        await expect(
            page.getByRole('option', { name: condition.op.new }),
        ).toBeVisible();
        await page.getByRole('option', { name: condition.op.new }).click();
        await expect(
            page.getByRole('combobox', { name: condition.op.new }),
        ).toBeVisible();
    }
    if (condition.value !== undefined) {
        const input = page.getByPlaceholder('Value');
        await input.focus();
        await input.fill(condition.value);
        await input.press('Tab'); // tab is required for blur events
    }
}

export async function searchForEntity(
    page: Page,
    entity:
        | {
              type: 'node';
              nodeType: string;
              conditions?: {
                  name: string;
                  op?: { current: string; new: string };
                  value?: string;
              }[];
          }
        | {
              type: 'edge';
              src?: string;
              dst?: string;
              layers?: string[];
          },
) {
    await page.goto('/search');
    await page
        .getByRole('button', {
            name: 'Select a graph',
        })
        .click();
    await page.waitForSelector('text="vanilla"');
    await page.getByRole('row', { name: /^vanilla$/ }).click();
    await page.waitForSelector('text="event"');
    await page.getByRole('row', { name: /^event$/ }).click();
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
            await page
                .locator('div')
                .filter({
                    hasText: new RegExp(
                        `^With the following conditions:${entity.nodeType}$`,
                    ),
                })
                .getByRole('button')
                .click();
            await page.getByRole('menuitem', { name: condition.name }).click();
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
    await page.getByRole('button', { name: 'Search', exact: true }).click();
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
                .getByRole('button', { name: `${entity.nodeName} Age` })
                .dblclick();
        } else if (entity.nodeType === 'Company') {
            await page
                .getByRole('button', {
                    name: `${entity.nodeName} No properties found`,
                })
                .dblclick();
        }
    } else if (entity.type === 'edge') {
        await page
            .getByRole('button', {
                name: `${entity.src} - ${entity.dst} Layers: ${entity.layers.join(',')}`,
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
    await waitForLayoutToFinish(page, layoutTimeout);
}

export async function waitForLayoutToFinish(
    page: Page,
    layoutTimeout?: number,
) {
    await expect(page.getByRole('progressbar')).toBeHidden({
        timeout: layoutTimeout,
    });
    // this extra timeout is to account for the animation
    await page.waitForTimeout(2000);
}

export async function navigateToSavedGraphBySavedGraphsTable(
    page: Page,
    folderName: string,
    graphName: string,
) {
    await page.goto('/saved-graphs');
    await page
        .getByRole('row', { name: new RegExp(`^${folderName}$`) })
        .click();
    await page.getByRole('cell', { name: graphName }).dblclick();
    await page.waitForSelector('text=Ben');
}
