import { check, fail, sleep } from 'k6';
import http, { RefinedResponse } from 'k6/http';
import { Rate } from 'k6/metrics';

import {
    generateQueryOp,
    generateMutationOp,
    QueryRootGenqlSelection,
    MutRootGenqlSelection,
    GraphGenqlSelection,
    EdgeGenqlSelection,
    NodeGenqlSelection,
    PathFromNodeViewCollection,
} from './__generated';

// ------------- CONF PARAMETERS
const TRAVERSAL_RATIO = 0; // 0.3; // TODO: bring back
// --------------------------------------------

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

              if (result === false) {
                console.log(">>> error:", JSON.stringify(body, null, 2));
                console.log(">>> request:", JSON.stringify(response.request.body, null, 2))
              }

              return result;
          } else {
              return false;
          }
      },
  });
  errorRate.add(!result);
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
  // { query: addNode, weight: 1 },
  // { query: deleteEdge, weight: 1 },
  { query: randomComposedReadQuery, weight: 6 },
]

const INDICES = QUERIES.flatMap(({weight}, index) => [...Array(weight).keys()].map(() => index))




export default function randomReadWriteQuery() {
  const indexPosition = Math.floor(Math.random() * INDICES.length)
  const index = INDICES[indexPosition]
  const { query } = QUERIES[index]
  query()
}

function getRandomEntityIds({ numEdges }: {numEdges: number}) {
  const response = fetchAndParse({
    graph: {
      __args: {
        path: "empty"
      },
      edges: {
        page: {
          __args: {
            limit: 1,
            offset: Math.floor(Math.random() * numEdges)
          },
          src: {
            name: true,
          },
          dst: {
            name: true,
          }
        }
      }
    }
  })
  const edge = response.data.graph.edges.page[0];
  // console.log(">>> edge: ", edge);
  return {
    src: edge?.src?.name as string | undefined,
    dst: edge?.dst?.name as string | undefined,
    name: edge?.src?.name as string | undefined, // TODO: maybe query a node independenty so not only node with edges are returned, so that they can have properties?
    // TODO: or instead, create edges only among existing nodes??
  }
}

function pickRandom<T>(choices: T[]) {
  return choices[randomInt(choices.length)]
}

function randomComposedReadQuery() {
  const view = randomView(); // TODO: bring back
  // const view = {
  //   __args: {
  //     views: []
  //   }
  // };
  const entity = randomEntityQuery()
  const query: QueryRootGenqlSelection = {
    graph: {
      __args: {
        path: "empty"
      },
      applyViews: {
        name: true,
        ...view,
        ...entity
      }
    }
  };
  fetchAndCheck(query)
}


function randomInt(n: number) {
  return Math.floor(Math.random() * n)
}

function randomEntityQuery(): GraphGenqlSelection {
  const { numNodes, numEdges } = queryGraphSize("empty")
  const { src, dst, name } = getRandomEntityIds({numEdges})
  if (src === undefined || dst === undefined || name === undefined) {
    return {}
  }
  const nodeQuery = randomNodeQuery()
  const edgeQuery = randomEdgeQuery()
  // const view = randomView(); // TODO: bring back
  const view = {
    __args: {
      views: []
    }
  };
  const queries: GraphGenqlSelection[] = [
    // { // just enabling this one causes the first version of the panic
    //   nodes: {
    //     applyViews: {
    //       ...view,
    //       page: {
    //         __args: {
    //           limit: 20,
    //           offset: randomInt(numNodes),
    //         },
    //         ...nodeQuery
    //       }
    //     },
    //   }
    // },
    // { // just enabling this one causes my entire colima to die
    //   node: {
    //     __args: {
    //       name,
    //     },
    //     ...nodeQuery
    //   }
    // },
    { // just enabling this one causes a panic in a different place
      edges: {
        applyViews: {
          ...view,
          page: {
            __args: {
              limit: Math.min(20, Math.floor(numEdges / 2)),
              // offset: randomInt(numEdges / 2),
              offset: 0,
            },
              ...edgeQuery,
          }
        },
      }
    },
    // { // just enabling this one causes my entire colima to die
    //   edge: {
    //     __args: {
    //       src,
    //       dst,
    //     },
    //     ...edgeQuery,
    //   }
    // }
  ];
  return pickRandom(queries);
}

const LAYERS = ["a", "b", "c"]

function randomLayer() {
  return pickRandom(LAYERS)
}

function randomView() {
  const [start, end] = [randomTime(), randomTime()].sort()
  const layer = randomLayer();
  const viewLists: PathFromNodeViewCollection[][] = [
    // [
    //   // NO VIEW AT ALL
    // ],
    // [{
    //   layer,
    // }],
    [{
      latest: true,
    }],
    // [{
    //   window: { start, end }
    // }],
    // [{
    //   latest: true,
    // }, {
    //   layer
    // }]
  ];

  // TODO: add more kind of filters and make it more random

  const views = pickRandom(viewLists);
  return {
    __args: {
      views
    }
  }
}

function randomTraversal(): NodeGenqlSelection {
  const view = randomView();
  const nodeInner = {
    applyViews: {
      ...view,
      page: {
        __args: {
          limit: 20
        },
        ...randomNodeQuery()
      }
    },
  }
  const edgeInner = {
    applyViews: {
      ...view,
      page: {
        __args: {
          limit: 20
        },
        ...randomEdgeQuery()
      }
    },
  }

  const queries: NodeGenqlSelection[] = [{
    // NO TRAVERSAL AT ALL IS ALSO AN OPTION
  }, {
    neighbours: {
      ...nodeInner
    }
  }, {
    inNeighbours: {
      ...nodeInner
    }
  }, {
    outNeighbours: {
      ...nodeInner
    }
  }, {
    inComponent: {
      ...nodeInner
    }
  }, {
    outComponent: {
      ...nodeInner
    }
  }, {
    edges: {
      ...edgeInner
    }
  }, {
    outEdges: {
      ...edgeInner
    }
  }, {
    inEdges: {
      ...edgeInner
    }
  }]
  return pickRandom(queries);
}

function randomNodeQuery(): NodeGenqlSelection {
  let traversal: NodeGenqlSelection = {}
  if (Math.random() > 1 - TRAVERSAL_RATIO) { // this is a fire line to avoid infinite recursions
    traversal = randomTraversal();
  }
  const properties = randomPropertyQuery()
  const degree = randomDegree()
  const inner = {
    name: true,
    ...properties,
    ...degree,
    ...traversal,
  }

  const queries: NodeGenqlSelection[] = [{
    applyViews: {
      ...randomView(),
      ...inner
    }
  },{
    ...inner
  }]
  return pickRandom(queries)
}

function randomDegree(): NodeGenqlSelection {
  const queries: NodeGenqlSelection[] = [{
    // nothing
  }, {
    degree: true,
    inDegree: true,
    outDegree: true,
  }]
  return pickRandom(queries)
}

function randomEdgeQuery(): EdgeGenqlSelection {
  const properties = randomPropertyQuery();
  const view = randomView(); // TODO: bring back
  // const view = {
  //   __args: {
  //     views: []
  //   }
  // };
  const inner = {
    id: true,
    ...view,
    ...properties,
  }

  const nodeQuery = randomNodeQuery();

  const queries: EdgeGenqlSelection[] = [
    {
      applyViews: {
        ...inner,
        // no src nor dst
      },
    },
    // {
    //   applyViews: {
    //     ...inner,
    //     src: nodeQuery,
    //   },
    // }, {
    //   applyViews: {
    //     ...inner,
    //     dst: nodeQuery,
    //   }
    // }, {
    //   applyViews: {
    //     ...inner,
    //     src: nodeQuery,
    //     dst: nodeQuery,
    //   }
    // }
  ]
  return pickRandom(queries)
}



type PropertyGenqlSelection = NodeGenqlSelection & EdgeGenqlSelection;
function randomPropertyQuery(): PropertyGenqlSelection {
  const queries: PropertyGenqlSelection[] = [{
    // no query at all
  },
  // {
  //   metadata: {
  //     values: {
  //       key: true,
  //       value: true,
  //     }
  //   }
  // },
  // {
  //   properties: {
  //     values: {
  //       key: true,
  //       value: true,
  //     }
  //   }
  // }, {
  //   properties: {
  //     temporal: {
  //       values: {
  //         key: true,
  //         values: true,
  //         history: true,
  //       }
  //     }
  //   }
  // }
  ];
  return pickRandom(queries);
}
