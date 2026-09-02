import { expect } from '@playwright/test';

import { test } from '../fixtures';
import { getGraphState } from './graph.utils';
import { waitForLayoutToFinish } from './utils';

const MAX_U64 = '18446744073709551615';
const MAX_U64_MINUS_1 = '18446744073709551614';

test('numerical graph renders integer-ID nodes, including u64 max', async ({ page }) => {
    // initialNodes=[] loads the whole graph (no node filter).
    await page.goto('/graph/vanilla/numerical?initialNodes=%5B%5D');

    // Both progress bars ("Querying for graph...", "Computing layout...") must
    // clear — the pre-fix bug rejected the layout worker message and this
    // never completed.
    await waitForLayoutToFinish(page);

    // getGraphState resolves only once nodes are present in the scene.
    const state = await getGraphState(page);
    const ids = new Set(state.nodes.map((n) => n.id));

    expect(state.nodes).toHaveLength(6);
    // The two u64 nodes must be distinct (they collapse to one double pre-fix).
    expect(ids).toEqual(new Set(['1', '2', '3', '4', MAX_U64, MAX_U64_MINUS_1]));
});

test('can render graph with numerical ID node', async ({ page }) => {
    await page.goto(`/graph/vanilla/numerical?initialNodes=%5B%22${MAX_U64}%22%5D`);
    await waitForLayoutToFinish(page);

    const state = await getGraphState(page);
    expect(state.nodes.map((n) => n.id)).toEqual([MAX_U64]);
});
