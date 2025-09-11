import { check, fail, sleep } from 'k6';
import http from 'k6/http';
import { Rate } from 'k6/metrics';

import {
    generateQueryOp,
    generateMutationOp,
    QueryRootGenqlSelection,
    MutRootGenqlSelection,
} from './__generated';

const TIME_RANGE = 2000 * 365 * 24 * 60 * 60 * 1000;
const randomTime = () => Math.floor(Math.random() * TIME_RANGE);

const URL = __ENV.RAPHTORY_URL ?? 'http://localhost:1736';

export const errorRate = new Rate('errors');

const duration = 1;
const stagesInMinutes: { duration: number; target: number }[] = [
    // { duration, target: 50 },
    { duration, target: 100 },
    // { duration, target: 200 },
    { duration, target: 400 },
    // { duration, target: 800 },
    { duration, target: 1600 },
    // { duration, target: 3200 },
    { duration, target: 6400 },
    // { duration, target: 12800 },
    // { duration, target: 25600 },
    // { duration, target: 51200 },
    // { duration, target: 102400 },
];

// +1 to leave enough time for the server to recover from prev scenario
const minutesPerScenario =
    stagesInMinutes.map(({ duration }) => duration).reduce((a, b) => a + b) + 1;

const execs = [
    addNode,
    randomNodePage,
    randomEdgePage,
    nodePropsByName,
    nodeNeighboursByName,
    readAndWriteNodeProperties,
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
};

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
        fail(JSON.stringify(response));
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

    mutate({
        newGraph: {
            __args: {
                path: 'empty',
                graphType: 'EVENT',
            },
        },
    });

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



export function addNode() {
    const name = Math.random().toString();
    const time = randomTime();
    fetchAndCheck({
        updateGraph: {
            __args: {
                path: 'empty',
            },
            addNode: {
                __args: {
                    name,
                    time,
                },
                success: true,
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

export function randomEdgePage(input: SetupData) {
  const offset = Math.floor(Math.random() * (input.countEdges - 20));
    fetchAndCheck({
        graph: {
            __args: { path: 'master' },
            edges: {
                page: {
                    __args: { offset, limit: 20 },
                    explodeLayers: {
                        count: true,
                    },
                    history: true,
                    src: { name: true },
                    dst: { name: true },
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
                metadata: {
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


export function readAndWriteNodeProperties(input: SetupData) {
    const random = Math.random();
    const time = randomTime();
    if (random < 0.3) {
      fetchAndCheck({
          updateGraph: {
              __args: {
                  path: 'master',
              },
              node: {
                __args: {
                  name: "SPARK-22386"
                },
                addUpdates: {
                  __args: {
                    time,
                    properties: [{key: "temporal_bool", value: {bool: Math.random() > 0.5}}]
                  }
                }
              }

          },
      });
    } else {
      fetchAndCheck({
          graph: {
              __args: { path: 'master' },
              node: {
                  __args: {
                      name: 'SPARK-22386',
                  },
                  at: {
                    __args: {
                      simpleTime: time,
                    },
                    properties: {
                      get: {
                        __args: {
                          key: "temporal_bool"
                        }
                      }
                    }
                  }
              },
          },
      });
    }
}
