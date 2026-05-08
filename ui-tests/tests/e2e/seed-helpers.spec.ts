import { expect, test } from '@playwright/test';
import { randomUUID } from 'crypto';
import { loadSpec, seedGraphFromSpec } from './specs';
import { deleteGraph } from './utils';

const SPEC_NAMES = [
    'event',
    'persistent',
    'filler',
    'persistent_filler',
    'second_filler',
    'persistent_second_filler',
    'variant_test',
] as const;

test.describe('graph spec helpers', () => {
    for (const name of SPEC_NAMES) {
        test(`loadSpec parses ${name}`, () => {
            const spec = loadSpec(name);
            expect(['EVENT', 'PERSISTENT']).toContain(spec.graphType);
            expect(Array.isArray(spec.nodes)).toBe(true);
            expect(Array.isArray(spec.edges)).toBe(true);
            expect(spec.nodes.length).toBeGreaterThan(0);
        });
    }

    test('event spec has the expected key nodes', () => {
        const spec = loadSpec('event');
        const names = spec.nodes.map((n) => n.name);
        expect(names).toEqual(
            expect.arrayContaining([
                'None',
                'Pedro',
                'Ben',
                'Hamza',
                'Pometry',
            ]),
        );
    });

    test('persistent spec includes the expected deletion', () => {
        const spec = loadSpec('persistent');
        expect(spec.deletions).toBeDefined();
        expect(spec.deletions).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    src: 'Ben',
                    dst: 'Pedro',
                    layer: 'meets',
                }),
            ]),
        );
    });
});

const GRAPHQL_URL = process.env.GRAPHQL_URL ?? 'http://localhost:1736';

async function gql<T>(query: string, variables?: Record<string, unknown>) {
    const response = await fetch(GRAPHQL_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, variables }),
    });
    const result = await response.json();
    if (result.errors) {
        throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
    }
    return result.data as T;
}

test.describe('seedGraphFromSpec', () => {
    test('seeds an event-graph spec into Raphtory and exposes the right counts', async () => {
        const namespace = `test_seeder_${randomUUID().slice(0, 8)}`;
        const target = `${namespace}/event`;
        const spec = loadSpec('event');
        try {
            await seedGraphFromSpec(target, spec);
            const data = await gql<{
                graph: {
                    nodes: { list: { name: string }[] };
                    countTemporalEdges: number;
                };
            }>(
                `query($path: String!) {
                    graph(path: $path) {
                        nodes { list { name } }
                        countTemporalEdges
                    }
                }`,
                { path: target },
            );
            const nodeNames = data.graph.nodes.list.map((n) => n.name).sort();
            expect(nodeNames).toEqual(
                ['Ben', 'Hamza', 'None', 'Pedro', 'Pometry'].sort(),
            );
            expect(data.graph.countTemporalEdges).toBe(spec.edges.length);
        } finally {
            await deleteGraph(target).catch(() => {});
        }
    });
});
