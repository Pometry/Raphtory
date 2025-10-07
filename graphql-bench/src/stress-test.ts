import { check, fail, sleep } from 'k6';
import http, { RefinedResponse } from 'k6/http';
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


const thresholdConf = {
  abortOnFail: true,
  delayAbortEval: '10s'
}

export const options = {
  executor: 'constant-vus',
  vus: 500,
  duration: "10m",
  thresholds: {
    errors: [{
      threshold: 'rate<0.1', // exec errors should be less than 10%
      ...thresholdConf
    }],
    http_req_failed: [{
      threshold: 'rate<0.01', // http errors should be less than 1%
      ...thresholdConf
    }],
    http_req_duration: [{
      threshold: 'p(95)<30000', // 95% of requests should be below 30_000ms = 30s
      ...thresholdConf
    }],
  },
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
    checkResponse(fetch(query));
}

function mutateAndCheck(query: MutRootGenqlSelection) {
    checkResponse(mutate(query));
}

function checkResponse<RT extends "binary" | "none" | "text"| undefined>(response: RefinedResponse<RT>) {
  const result = check(response, {
      'response status is 200': (r) => r.status === 200,
      'response has data field defined': (r) => {
          if (typeof r.body === 'string') {
              const body = JSON.parse(r.body);
              const result = 'data' in body &&
                body.data !== undefined &&
                body.data !== null; // FIXME: improve query checking, I wish I could just rely on genql
              if (!result) {
                console.log({
                  dataInBody: 'data' in body,
                  dataDefined: body.data !== undefined,
                  dataNotNull: body.data !== null
                })
                if (body.data === null) {
                  console.log(">>> data", body.data)
                  console.log(">>> body", JSON.stringify(body, null, 2))
                  console.log(">>> response", JSON.stringify(r, null, 2))
                }
              }
              return result;
          } else {
              return false;
          }
      },
  });
  errorRate.add(!result);
  // console.log("error on request:", JSON.stringify(JSON.parse(response.body as string), null, 2))
}

export function setup() {
    mutate({
        deleteGraph: {
            __args: {
                path: 'empty',
            },
        },
    });
    mutateAndCheck({
        newGraph: {
            __args: {
                path: 'empty',
                graphType: 'EVENT',
            },
        },
    });
}



function addEdge() {
  fetchAndCheck({
      updateGraph: {
          __args: {
              path: "empty",
          },
          addEdges: {
            __args: {
              edges: [{src: Math.random().toString(), dst: Math.random().toString(), updates: [{time: randomTime(), properties: [{key: "key", value: {str: "strvalue"}}]}]}]
            }
          }
      },
  });
}

function addNode() {
  fetchAndCheck({
      updateGraph: {
          __args: {
              path: "empty",
          },
          addNodes: {
            __args: {
              nodes: [{name: Math.random().toString(), updates: [{time: randomTime()}]}]
            }
          }
      },
  });
}

function queryGraphSize(path: string) {
  const response = fetchAndParse({
      graph: {
          __args: { path },
          countEdges: true
      },
  });
  return {
    numNodes: response.data.graph.countNodes as number,
    numEdges: response.data.graph.countEdges as number
  }
}

function deleteEdge() {
  const { numEdges } = queryGraphSize("empty");

  const edgeIndex = Math.floor(numEdges / 2 * Math.random()); // just in case, to avoid races, only target the first half
  const response = fetchAndParse({
      graph: {
          __args: {
              path: "empty",
          },
          edges: {
            page: {
              __args: {
                limit: 1,
                offset: edgeIndex
              },
              src: {
                name: true
              },
              dst: {
                name: true,
              }
            }
          }
      },
  });
  const edge = response.data.graph.edges.page[0]

  fetchAndCheck({
      updateGraph: {
          __args: {
              path: "empty",
          },
          deleteEdge: {
            __args: {
              time: randomTime(),
              src: edge.src.name,
              dst: edge.dst.name,
            },
            success: true,
          }
      },
  });
}

const queryProps = {
  metadata: {
    values: {
      key: true,
      value: true,
    }
  },
  properties: {
    temporal: {
      values: {
        key: true,
        values: true,
        history: true,
      }
    }
  }
}

// TODO: for all the queries, maybe make it not fail if the entity does not exist when accessing by offset

function readNode() {
  const { numNodes } = queryGraphSize("empty");
  const nodeIndex = Math.floor(numNodes / 2 * Math.random()); // just in case, to avoid races, only target the first half
  const response = fetchAndParse({
      graph: {
          __args: {
              path: "empty",
          },
          nodes: {
            page: {
              __args: {
                limit: 1,
                offset: nodeIndex
              },
              name: true,
            }
          }
      },
  });

  fetchAndCheck({
      graph: {
          __args: {
              path: "empty",
          },
          latest: {
            node: {
              __args: {
                name: response.data.graph.nodes.page[0].name,
              },
              name: true,
              outDegree: true,
              ...queryProps,
              inNeighbours: {
                list: {
                  name: true,
                    outDegree: true,
                }
              }
            }
          },
      },
  });
}

function readNodePage() {
  const { numNodes } = queryGraphSize("empty");
  const nodeIndex = Math.floor(numNodes / 2 * Math.random()); // just in case, to avoid races, only target the first half
  fetchAndCheck({
      graph: {
          __args: {
              path: "empty",
          },
          nodes: {
            page: {
              __args: {
                limit: 100,
                offset: nodeIndex
              },
              name: true,
              outDegree: true,
              ...queryProps,
            }
          }
      },
  });
}

function readEdgePage() {
  const { numEdges } = queryGraphSize("empty");
  const edgeIndex = Math.floor(numEdges * Math.random());
  fetchAndCheck({
      graph: {
          __args: {
              path: "empty",
          },
          edges: {
            page: {
              __args: {
                limit: 10, // TODO: change to 1000 also nodes
                offset: edgeIndex
              },
              src: {
                name: true,
                ...queryProps,
              },
              dst: {
                name: true,
                ...queryProps,
              },
              ...queryProps,
            }
          }
      },
  });
}


const QUERIES: ({ query: () => void, weight: number })[] = [
  { query: addEdge, weight: 1 },
  { query: addNode, weight: 1 },
  { query: deleteEdge, weight: 1 },
  { query: readNode, weight: 2 },
  { query: readNodePage, weight: 2 },
  { query: readEdgePage, weight: 2 },
]

const INDICES = QUERIES.flatMap(({weight}, index) => [...Array(weight).keys()].map(() => index))




export default function randomReadWriteQuery() {
  const indexPosition = Math.floor(Math.random() * INDICES.length)
  const index = INDICES[indexPosition]
  const { query } = QUERIES[index]
  query()
}
