import { expect, Page } from '@playwright/test';

import { clickAfterPaginating } from './utils';

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
        await expect(page.getByRole('option', { name: condition.op.new })).toBeVisible();
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
    await clickAfterPaginating(page, namespaceRow, `Namespace "${namespace}"`, nextPageButton);
    const graphCell = page.getByRole('cell', {
        name: graphName,
        exact: true,
    });
    await clickAfterPaginating(page, graphCell, `Graph "${graphName}"`, nextPageButton);
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
            await page.getByRole('textbox', { name: 'Source ID' }).fill(entity.src);
        }
        if (entity.dst !== undefined) {
            await page.getByRole('textbox', { name: 'Destination ID' }).click();
            await page.getByRole('textbox', { name: 'Destination ID' }).fill(entity.dst);
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
