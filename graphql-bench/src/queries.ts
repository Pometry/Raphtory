import { QueryRootGenqlSelection } from './__generated';

function randomInt(limit: number) {
    return Math.floor(Math.random() * limit);
}

export const queries: Record<string, QueryRootGenqlSelection> = {
    // listGraphs: {
    //     root: { graphs: { list: { name: true } } },
    // },
    hello: {
        hello: true,
    },
    listNodes: {
        graph: {
            __args: { path: 'master' },
            nodes: {
                page: {
                    __args: { offset: 0, limit: 20 },
                    degree: true,
                    name: true,
                    // properties: { values: { key: true, value: true } },
                },
            },
        },
    },
    // listEdges: () => ({
    //     graph: {
    //         __args: { path: 'master' },
    //         edges: {
    //             page: {
    //                 __args: { offset: 0, limit: 20 },
    //                 src: { name: true },
    //                 dst: { name: true },
    //                 properties: { values: { key: true, value: true } },
    //             },
    //         },
    //     },
    // }),
    // randomGraph: (paths: string[]) => {
    //     const path = paths[randomInt(paths.length)];
    //     return {
    //         graph: {
    //             __args: {
    //                 path,
    //             },
    //             name: true,
    //             nodes: {
    //                 page: {
    //                     __args: {
    //                         offset: 0,
    //                         limit: 20,
    //                     },
    //                     properties: {
    //                         values: {
    //                             key: true,
    //                             value: true,
    //                         },
    //                     },
    //                 },
    //             },
    //         },
    //     };
    // },
};
