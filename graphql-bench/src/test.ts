import { check, fail, sleep } from 'k6';
import http from 'k6/http';
import { Rate } from 'k6/metrics';

import {
    generateQueryOp,
    generateMutationOp,
    QueryRootGenqlSelection,
    MutRootGenqlSelection,
} from './__generated';
import { randomUUID } from 'node:crypto';

const URL = __ENV.RAPHTORY_URL ?? 'http://localhost:1736';

export const errorRate = new Rate('errors');

const duration = 1;
const stagesInMinutes: { duration: number; target: number }[] = [
    // { duration, target: 50 },
    // { duration, target: 100 },
    { duration, target: 200 },
    // { duration, target: 400 },
    // { duration, target: 800 },
    // { duration, target: 1600 },
    // { duration, target: 3200 },
    // { duration, target: 6400 },
    // { duration, target: 12800 },
    // { duration, target: 25600 },
    // { duration, target: 51200 },
    // { duration, target: 102400 },
];

// +1 to leave enough time for the server to recover from prev scenario
const minutesPerScenario =
    stagesInMinutes.map(({ duration }) => duration).reduce((a, b) => a + b) + 1;

const execs = [
    // writeRequest70percent,
    // addVectorisedNode,
    randomNodePage,
    // nodePropsByName,
    // nodeNeighboursByName,
    // nodeNeighboursWithPropsByName,
    // edgePage,
];
const scenarios = execs.map(
    (exec, index) =>
        [
            exec.name,
            {
                executor: 'ramping-arrival-rate',
                exec: exec.name,
                startRate: 0,
                startTime: `${index * minutesPerScenario}m`,
                timeUnit: '1s',
                preAllocatedVUs: 5,
                maxVUs: 1000,
                stages: stagesInMinutes.map(({ duration, target }) => ({
                    duration: `${duration}m`,
                    target,
                })),
            },
        ] as const,
);

export const options = {
    scenarios: Object.fromEntries(scenarios),
    // thresholds: { // this is being analyzed externally
    //     http_req_duration: [
    //         { threshold: 'p(95)<500', abortOnFail, delayAbortEval },
    //     ],
    //     errors: [{ threshold: 'rate<0.05', abortOnFail, delayAbortEval }],
    //     http_req_failed: [
    //         { threshold: 'rate<0.01', abortOnFail, delayAbortEval },
    //     ],
    // },
};

// TODO: maybe use some of the seetings below instead, setting timeUnit: '1s' makes the test work by iterations/s instead of vus

const params = {
    headers: { 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip' },
};
function fetch(query: QueryRootGenqlSelection) {
    const { query: compiledQuery, variables } = generateQueryOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return http.post(URL, payload, params);
}

function mutate(query: MutRootGenqlSelection) {
    const { query: compiledQuery, variables } = generateMutationOp(query);
    const payload = JSON.stringify({
        query: compiledQuery,
        variables: variables,
    });
    return http.post(URL, payload, params);
}

function fetchAndParse(query: QueryRootGenqlSelection) {
    const response = fetch(query);
    if (typeof response.body !== 'string') {
        fail('Initial graph list query failed');
    }
    return JSON.parse(response.body);
}

function fetchAndCheck(query: QueryRootGenqlSelection) {
    const response = fetch(query);
    const result = check(response, {
        'variable query status is 200': (r) => r.status === 200,
        'variable query has user data': (r) => {
            if (typeof r.body === 'string') {
                const body = JSON.parse(r.body as string);
                return (
                    'data' in body &&
                    body.data !== undefined &&
                    body.data !== null // FIXME: improve query checking, I wish I could just rely on genql
                );
            } else {
                return false;
            }
        },
    });
    errorRate.add(!result);
}

type SetupData = {
    graphPaths: string[];
    countNodes: number;
    countEdges: number;
};

export function setup(): SetupData {
    const graphListResponse = fetchAndParse({
        namespaces: { list: { graphs: { list: { path: true } } } },
    });
    const graphPaths = graphListResponse.data.namespaces.list.flatMap(
        (ns: any) => ns.graphs.list.map((graph: any) => graph.path),
    );

    // mutate({
    //     // TODO: review if this removes the master graph when serving apache or it fails
    //     newGraph: {
    //         __args: {
    //             path: 'master',
    //             graphType: 'EVENT',
    //         },
    //     },
    // });

    const graphResponse = fetchAndParse({
        graph: {
            __args: {
                path: 'master',
            },
            countNodes: true,
            countEdges: true,
        },
    });

    return {
        graphPaths,
        countNodes: graphResponse.data.graph.countNodes,
        countEdges: graphResponse.data.graph.countEdges,
    };
}

export function writeRequest70percent(input: SetupData) {
    const random = Math.random();
    if (random > 0.7) {
      const id = Math.random().toString();
      fetchAndCheck({
          updateGraph: {
              __args: {
                  path: 'master',
              },
              addNode: {
                  __args: {
                      name: id,
                      time: 0,
                  },
                  success: true,
              },
          },
      });
    } else {
      randomNodePage(input)
    }
}

export function addVectorisedNode() {
    const id = Math.random().toString();
    fetchAndCheck({
        updateGraph: {
            __args: {
                path: 'master',
            },
            addNode: {
                __args: {
                    name: id,
                    time: 0,
                },
                success: true,
            },
        },
    });
}

export function firstNodePage() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            nodes: {
                page: {
                    __args: { offset: 0, limit: 20 },
                    degree: true,
                    name: true,
                },
            },
        },
    });
}

export function firstNodePageWithProps() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            nodes: {
                page: {
                    __args: { offset: 0, limit: 20 },
                    name: true,
                    properties: {
                        values: {
                            key: true,
                            value: true,
                        },
                    },
                },
            },
        },
    });
}

export function randomNodePage(input: SetupData) {
    const offset = Math.floor(Math.random() * (input.countNodes - 20));
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            nodes: {
                page: {
                    __args: { offset, limit: 20 },
                    degree: true,
                    name: true,
                },
            },
        },
    });
}

export function nodePropsByName() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            node: {
                __args: {
                    name: 'SPARK-22386',
                },
                properties: {
                    values: {
                        key: true,
                        value: true,
                    },
                },
            },
        },
    });
}

export function nodeNeighboursByName() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            node: {
                __args: {
                    name: 'SPARK-22386',
                },
                neighbours: {
                    list: {
                        name: true,
                    },
                },
            },
        },
    });
}

export function nodeNeighboursWithPropsByName() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            node: {
                __args: {
                    name: 'dongjoon',
                },
                neighbours: {
                    list: {
                        name: true,
                        properties: {
                            values: {
                                key: true,
                                value: true,
                            },
                        },
                    },
                },
            },
        },
    });
}

export function edgePage() {
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            edges: {
                page: {
                    __args: { offset: 0, limit: 20 },
                    explodeLayers: {
                        count: true,
                    },
                    src: { name: true },
                    dst: { name: true },
                },
            },
        },
    });
}

// export default function (payload: any) {
//     // http.post(URL, payload, params);
//     const response = fetch(query(paths));
//     const result = check(response, {
//         'variable query status is 200': (r) => r.status === 200,
//         'variable query has user data': (r) => {
//             if (typeof r.body === 'string') {
//                 const body = JSON.parse(r.body as string);
//                 return (
//                     'data' in body &&
//                     body.data !== undefined &&
//                     body.data !== null // FIXME: improve query checking, I wish I could just rely on genql
//                 );
//             } else {
//                 return false;
//             }
//         },
//     });
//     errorRate.add(!result);
// }

// export function handleSummary(data: any) {
//     const vus = data.metrics.vus.values.max;
//     const iters = data.metrics.iterations?.values?.rate ?? 0;
//     return {
//         [`breakpoints/${selectedQuery}.csv`]: `${selectedQuery},${vus},${iters}\n`, //the default data object
//     };

//     // return {
//     //   'summary.json': JSON.stringify(data), //the default data object
//     // };
// }
