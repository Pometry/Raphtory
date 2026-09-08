import { expect, Page, test } from '@playwright/test';

import { graphqlMutation } from './api';

// Exercises the playground example queries end-to-end: open the Examples
// panel, click each example (which loads it into a new tab), press Run, and
// assert the response pane shows data backed by the `my_graph` fixture in
// test_server.py.
//
// Not covered:
// - "Searches": this server build has no search index — every query returns
//   "Indexing not supported".
// - "Permissions (RBAC)": closed-source servers only; the field does not
//   exist in this schema and the examples deliberately stub the admin JWT.
//
// Tests share one server across browser projects, so all runnable mutation
// examples are value-idempotent: they re-add existing nodes/edges or set the
// same values, keeping the counts and filter matches other tests assert on
// stable. Tests in this file run in order in one worker per project (mode
// "default") so the mutation tests come last within each project.
test.describe.configure({ mode: 'default' });

interface ExampleCase {
    title: string;
    /** Substring of the JSON response proving the data is real, not just an envelope. */
    marker: string;
    /** Replaces the example's variables JSON through the UI before running. */
    variables?: string;
}

async function openExamples(page: Page) {
    await page.goto('/playground');
    await page.getByRole('tab', { name: 'Examples' }).click();
}

async function runExamples(page: Page, folder: string, examples: ExampleCase[]) {
    await openExamples(page);
    const panel = page.getByRole('list');
    await panel.getByText(folder, { exact: true }).click();
    for (const example of examples) {
        await panel.getByText(example.title, { exact: true }).click();
        if (example.variables !== undefined) {
            // insertText, not type: typing would fight CodeMirror's
            // auto-closing brackets.
            await page.locator('[aria-label="Variables editor"] .cm-content').click();
            await page.keyboard.press('ControlOrMeta+a');
            await page.keyboard.insertText(example.variables);
        }
        await page.getByRole('button', { name: '▶ Run' }).click();
        const results = page.locator('[aria-label="Results editor"]');
        await expect(results).toContainText('"data"', { timeout: 15000 });
        await expect(results).toContainText(example.marker);
        await expect(results).not.toContainText('"errors"');
    }
}

test('Getting Started examples return data', async ({ page }) => {
    await runExamples(page, 'Getting Started', [
        { title: 'Hello', marker: 'Hello world from raphtory-graphql' },
        { title: 'Version', marker: '"version"' },
        { title: 'Available Graphs', marker: '"my_graph"' },
        { title: 'Graph Counts', marker: '"count": 4' },
    ]);
});

test('Queries examples return data', async ({ page }) => {
    await runExamples(page, 'Queries', [
        { title: 'Graph Info', marker: '"path": "my_graph"' },
        { title: 'Node Count', marker: '"count": 4' },
        { title: 'List All Nodes', marker: '"node_1"' },
        { title: 'List Nodes (Paginated)', marker: '"node_1"' },
        { title: 'Get Node', marker: '"degree": 2' },
        { title: 'Node Properties', marker: '"score"' },
        { title: 'Node History', marker: '"timestamp": 1' },
        { title: 'Node Out-Component', marker: '"node_2"' },
        { title: 'List All Edges', marker: '"knows"' },
        { title: 'Get Edge', marker: '"knows"' },
        { title: 'Time Window Query', marker: '"countNodes": 4' },
        { title: 'Graph Properties', marker: '"version"' },
    ]);
});

test('Filters examples return data', async ({ page }) => {
    await runExamples(page, 'Filters', [
        { title: 'Filter Nodes by Type', marker: '"node_1"' },
        { title: 'Filter Nodes by Property', marker: '"node_2"' },
        { title: 'Filter Nodes (AND Composite)', marker: '"countNodes": 3' },
        { title: 'Filter Edges by Source Node', marker: '"node_2"' },
        { title: 'Filter Edges by Property', marker: '"countEdges": 2' },
        { title: 'Select Filter Nodes', marker: '"node_1"' },
        { title: 'Select Filter Edges', marker: '"node_2"' },
        { title: 'Select Filter by Time Window', marker: '"node_1"' },
    ]);
});

test('Apply Views examples return data', async ({ page }) => {
    await runExamples(page, 'Apply Views', [
        { title: 'Windowed Node Filter', marker: '"node_1"' },
        { title: 'Subgraph', marker: '"countNodes": 3' },
        { title: 'Layer Restriction', marker: '"knows"' },
        { title: 'Latest Snapshot', marker: '"countNodes": 4' },
        { title: 'Edge Filter View', marker: '"countEdges": 2' },
    ]);
});

test('Algorithms examples return data', async ({ page }) => {
    await runExamples(page, 'Algorithms', [
        { title: 'PageRank', marker: '"pagerank_score"' },
        { title: 'Shortest Path', marker: '"node_2"' },
    ]);
});

test('Namespaces examples return data', async ({ page }) => {
    await runExamples(page, 'Namespaces', [
        { title: 'List Root', marker: '"my_graph"' },
        { title: 'List All Namespaces', marker: '"my_namespace"' },
        { title: 'Get Specific Namespace', marker: '"my_namespace/demo"' },
    ]);
});

test('Data Mutations examples succeed against the fixture graph', async ({ page }) => {
    await runExamples(page, 'Data Mutations', [
        { title: 'Add Node', marker: '"success": true' },
        { title: 'Add Node with Properties', marker: '"node_1"' },
        { title: 'Add Edge', marker: '"success": true' },
        { title: 'Add Edge with Properties', marker: '"success": true' },
        { title: 'Add Temporal Properties to Node', marker: '"addUpdates": true' },
        { title: 'Add Graph Metadata', marker: '"addMetadata": true' },
        { title: 'Add Node Metadata', marker: '"addMetadata": true' },
    ]);
});

test('Graph Management examples create and delete graphs', async ({ page }, testInfo) => {
    // These examples target my_graph — the fixture every other test queries —
    // so running them verbatim would either fail (create: already exists) or
    // destroy the fixture (delete). Override the path variable through the
    // UI so the test creates and deletes its own graphs, unique per browser
    // project since all projects share one server.
    const eventPath = `pw_mgmt_event_${testInfo.project.name}`;
    const persistentPath = `pw_mgmt_persistent_${testInfo.project.name}`;
    // A failed earlier attempt can leave the graphs behind; deleteGraph
    // errors on a missing path — that's fine here.
    for (const path of [eventPath, persistentPath]) {
        await graphqlMutation('mutation($path: String!) { deleteGraph(path: $path) }', {
            path,
        }).catch(() => {});
    }
    await runExamples(page, 'Graph Management', [
        {
            title: 'New Graph (Event)',
            marker: '"newGraph": true',
            variables: `{ "path": "${eventPath}" }`,
        },
        {
            title: 'New Graph (Persistent)',
            marker: '"newGraph": true',
            variables: `{ "path": "${persistentPath}" }`,
        },
        {
            title: 'Delete Graph',
            marker: '"deleteGraph": true',
            variables: `{ "path": "${eventPath}" }`,
        },
    ]);
    // Remove the leftover persistent graph so this test is repeatable.
    await graphqlMutation('mutation($path: String!) { deleteGraph(path: $path) }', {
        path: persistentPath,
    });
});
