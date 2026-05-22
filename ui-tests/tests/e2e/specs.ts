import { readFileSync } from 'fs';
import path from 'path';

import { graphqlMutation } from './utils';

export type GraphSpecValue = string | number | boolean;

export interface GraphSpecNode {
    time: number;
    name: string;
    nodeType?: string;
    properties?: Record<string, GraphSpecValue>;
}

export interface GraphSpecEdge {
    time: number;
    src: string;
    dst: string;
    layer?: string;
    properties?: Record<string, GraphSpecValue>;
}

export interface GraphSpecDeletion {
    time: number;
    src: string;
    dst: string;
    layer?: string;
}

export interface GraphSpec {
    graphType: 'EVENT' | 'PERSISTENT';
    nodes: GraphSpecNode[];
    edges: GraphSpecEdge[];
    deletions?: GraphSpecDeletion[];
}

const SPECS_DIR = path.resolve(__dirname, '..', '..', 'graph-specs');

function isPlainObject(v: unknown): v is Record<string, unknown> {
    return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function validateProperties(
    raw: unknown,
    where: string,
): Record<string, GraphSpecValue> | undefined {
    if (raw === undefined) return undefined;
    if (!isPlainObject(raw)) {
        throw new Error(`${where}: expected properties to be an object`);
    }
    const out: Record<string, GraphSpecValue> = {};
    for (const [key, value] of Object.entries(raw)) {
        if (
            typeof value !== 'string' &&
            typeof value !== 'number' &&
            typeof value !== 'boolean'
        ) {
            throw new Error(
                `${where}: properties.${key} must be string|number|boolean, got ${typeof value}`,
            );
        }
        out[key] = value;
    }
    return out;
}

function validateSpec(raw: unknown, name: string): GraphSpec {
    if (!isPlainObject(raw)) {
        throw new Error(`spec ${name}: expected top-level object`);
    }
    if (raw.graphType !== 'EVENT' && raw.graphType !== 'PERSISTENT') {
        throw new Error(
            `spec ${name}: graphType must be "EVENT" or "PERSISTENT"`,
        );
    }
    if (!Array.isArray(raw.nodes)) {
        throw new Error(`spec ${name}: nodes must be an array`);
    }
    if (!Array.isArray(raw.edges)) {
        throw new Error(`spec ${name}: edges must be an array`);
    }
    const nodes: GraphSpecNode[] = raw.nodes.map((n: unknown, i: number) => {
        if (!isPlainObject(n)) {
            throw new Error(`spec ${name}: nodes[${i}] must be an object`);
        }
        if (typeof n.time !== 'number') {
            throw new Error(`spec ${name}: nodes[${i}].time must be a number`);
        }
        if (typeof n.name !== 'string') {
            throw new Error(`spec ${name}: nodes[${i}].name must be a string`);
        }
        if (n.nodeType !== undefined && typeof n.nodeType !== 'string') {
            throw new Error(
                `spec ${name}: nodes[${i}].nodeType must be a string`,
            );
        }
        return {
            time: n.time,
            name: n.name,
            nodeType: n.nodeType as string | undefined,
            properties: validateProperties(
                n.properties,
                `spec ${name}: nodes[${i}]`,
            ),
        };
    });
    const edges: GraphSpecEdge[] = raw.edges.map((e: unknown, i: number) => {
        if (!isPlainObject(e)) {
            throw new Error(`spec ${name}: edges[${i}] must be an object`);
        }
        if (typeof e.time !== 'number') {
            throw new Error(`spec ${name}: edges[${i}].time must be a number`);
        }
        if (typeof e.src !== 'string') {
            throw new Error(`spec ${name}: edges[${i}].src must be a string`);
        }
        if (typeof e.dst !== 'string') {
            throw new Error(`spec ${name}: edges[${i}].dst must be a string`);
        }
        if (e.layer !== undefined && typeof e.layer !== 'string') {
            throw new Error(`spec ${name}: edges[${i}].layer must be a string`);
        }
        return {
            time: e.time,
            src: e.src,
            dst: e.dst,
            layer: e.layer as string | undefined,
            properties: validateProperties(
                e.properties,
                `spec ${name}: edges[${i}]`,
            ),
        };
    });
    let deletions: GraphSpecDeletion[] | undefined;
    if (raw.deletions !== undefined) {
        if (!Array.isArray(raw.deletions)) {
            throw new Error(`spec ${name}: deletions must be an array`);
        }
        if (raw.graphType !== 'PERSISTENT') {
            throw new Error(
                `spec ${name}: deletions only allowed on PERSISTENT graphs`,
            );
        }
        deletions = raw.deletions.map((d: unknown, i: number) => {
            if (!isPlainObject(d)) {
                throw new Error(
                    `spec ${name}: deletions[${i}] must be an object`,
                );
            }
            if (typeof d.time !== 'number') {
                throw new Error(
                    `spec ${name}: deletions[${i}].time must be a number`,
                );
            }
            if (typeof d.src !== 'string' || typeof d.dst !== 'string') {
                throw new Error(
                    `spec ${name}: deletions[${i}] must have string src/dst`,
                );
            }
            if (d.layer !== undefined && typeof d.layer !== 'string') {
                throw new Error(
                    `spec ${name}: deletions[${i}].layer must be a string`,
                );
            }
            return {
                time: d.time,
                src: d.src,
                dst: d.dst,
                layer: d.layer as string | undefined,
            };
        });
    }
    return { graphType: raw.graphType, nodes, edges, deletions };
}

export function loadSpec(name: string): GraphSpec {
    const filePath = path.join(SPECS_DIR, `${name}.json`);
    const raw = JSON.parse(readFileSync(filePath, 'utf-8'));
    return validateSpec(raw, name);
}

type ValueInput =
    | { str: string }
    | { i64: number }
    | { f64: number }
    | { bool: boolean };

function valueFor(v: GraphSpecValue): ValueInput {
    if (typeof v === 'string') return { str: v };
    if (typeof v === 'boolean') return { bool: v };
    return Number.isInteger(v) ? { i64: v } : { f64: v };
}

function propertyInputs(
    props: Record<string, GraphSpecValue> | undefined,
): { key: string; value: ValueInput }[] | undefined {
    if (props === undefined) return undefined;
    const entries = Object.entries(props);
    if (entries.length === 0) return undefined;
    return entries.map(([key, value]) => ({ key, value: valueFor(value) }));
}

export async function seedGraphFromSpec(
    targetPath: string,
    spec: GraphSpec,
): Promise<void> {
    await graphqlMutation(
        'mutation($path: String!, $type: GraphType!) { newGraph(path: $path, graphType: $type) }',
        { path: targetPath, type: spec.graphType },
    );

    const nodes = spec.nodes.map((n) => ({
        name: n.name,
        nodeType: n.nodeType,
        updates: [
            {
                time: n.time,
                properties: propertyInputs(n.properties),
            },
        ],
    }));
    const edges = spec.edges.map((e) => ({
        src: e.src,
        dst: e.dst,
        layer: e.layer,
        updates: [
            {
                time: e.time,
                properties: propertyInputs(e.properties),
            },
        ],
    }));

    await graphqlMutation(
        `query($path: String!, $nodes: [NodeAddition!]!) {
            updateGraph(path: $path) { addNodes(nodes: $nodes) }
        }`,
        { path: targetPath, nodes },
    );

    if (edges.length > 0) {
        await graphqlMutation(
            `query($path: String!, $edges: [EdgeAddition!]!) {
                updateGraph(path: $path) { addEdges(edges: $edges) }
            }`,
            { path: targetPath, edges },
        );
    }

    for (const d of spec.deletions ?? []) {
        await graphqlMutation(
            `query($path: String!, $time: Int!, $src: String!, $dst: String!, $layer: String) {
                updateGraph(path: $path) {
                    deleteEdge(time: $time, src: $src, dst: $dst, layer: $layer) { success }
                }
            }`,
            {
                path: targetPath,
                time: d.time,
                src: d.src,
                dst: d.dst,
                layer: d.layer,
            },
        );
    }
}
