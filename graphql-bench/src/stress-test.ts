import { Rate } from 'k6/metrics';

import {
    GraphGenqlSelection,
    EdgeGenqlSelection,
    NodeGenqlSelection,
    PathFromNodeViewCollection,
} from './__generated';
import { fetchAndCheck, fetchAndParse, mutate, mutateAndCheck } from './utils';

// Global
const VUS = 300;
const PAGE_SIZE = 20;
const LAYERS = ["a", "b", "c"];
const CONST_KEYS = ["ca", "cb", "cc"];
const TEMP_KEYS = ["ta", "tb", "tc"];
const TIME_RANGE = 2000 * 365 * 24 * 60 * 60 * 1000;

// CONF PARAMETERS
const TRAVERSAL_RATIO = 0.3; // chance for the query to continue with a traversal from a node

// query type rates
const ADD_EDGE_WEIGHT = 1;
const ADD_NODE_WEIGHT = 1;
const DELETE_EDGE_WEIGHT = 1;
const READ_QUERY_RATE = 6;

// Entity query inside of the graph:
const NODE_BY_NAME_WEIGHT = 1
const EDGE_BY_SRC_DST_WEIGHT = 1
const NODE_PAGE_WEIGHT = 1
const EDGE_PAGE_WEIGHT = 1

// Traversal methods
const NEIGHBOURS_WEIGHT = 1;
const IN_NEIGHBOURS_WEIGHT = 1;
const OUT_NEIGHBOURS_WEIGHT = 1;
const IN_COMPONENT_WEIGHT = 1;
const OUT_COMPONENT_WEIGHT = 1;
const EDGES_WEIGHT = 1;
const OUT_EDGES_WEIGHT = 1;
const IN_EDGES_WEIGHT = 1;

// Property rates
const EDGE_PROPERTY_RATE: PropertyRates = {
  metadata: 0.3,
  temporalLatest: 0.3,
  temporalHistory: 0.3,
}
const NODE_PROPERTY_RATE: PropertyRates = {
  metadata: 0.3,
  temporalLatest: 0.3,
  temporalHistory: 0.3,
}
const GRAPH_PROPERTY_RATE: PropertyRates = {
  metadata: 0.3,
  temporalLatest: 0.3,
  temporalHistory: 0.3,
}

// View rates
const GRAPH_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2
}
const NODE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2
}
const EDGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2
}
const NODE_PAGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2
}
const EDGE_PAGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2
}
const TRAVERSAL_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2
}

type PropertyRates = {
  metadata: number,
  temporalLatest: number,
  temporalHistory: number,
}
type ViewRate = {
  layer: number,
  latest: number,
  window: number,
}


const randomTime = () => Math.floor(Math.random() * TIME_RANGE);
const randomStr = () => Math.random().toString();
const randomTempKey = () => pickRandom(TEMP_KEYS);
const randomConstKey = () => pickRandom(CONST_KEYS);

export const errorRate = new Rate('errors');

const thresholdConf = {
  abortOnFail: true,
  delayAbortEval: '10s'
}
export const options = {
  executor: 'constant-vus',
  vus: VUS,
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

export function setup() {
    mutate({
        deleteGraph: {
            __args: {
                path: 'empty',
            },
        },
    });
    mutateAndCheck(errorRate, {
        newGraph: {
            __args: {
                path: 'empty',
                graphType: 'EVENT',
            },
        },
    });
}

const QUERIES: Option<() => void>[] = [
  { query: addEdge, weight: ADD_EDGE_WEIGHT },
  { query: addNode, weight: ADD_NODE_WEIGHT },
  { query: deleteEdge, weight: DELETE_EDGE_WEIGHT },
  { query: randomComposedReadQuery, weight: READ_QUERY_RATE },
]

export default function randomReadWriteQuery() {
  const query = pickRandomOption(QUERIES)
  query()
}

function addEdge() {
  fetchAndCheck(errorRate, {
      updateGraph: {
          __args: {
              path: "empty",
          },
          addEdges: {
            __args: {
              edges: [{src: randomStr(), dst: randomStr(), layer: randomLayer(), updates: [{time: randomTime(), properties: [{key: randomTempKey(), value: {str: randomStr()}}]}]}]
            }
          }
      },
  });
}

function addNode() {
  fetchAndCheck(errorRate, {
      updateGraph: {
          __args: {
              path: "empty",
          },
          addNodes: {
            __args: {
              nodes: [{name: randomStr(), updates: [{time: randomTime(), properties: [{key: randomTempKey(), value: { str: randomStr()}}]}]}]
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

  fetchAndCheck(errorRate, {
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
  return {
    src: edge?.src?.name as string | undefined,
    dst: edge?.dst?.name as string | undefined,
    name: edge?.src?.name as string | undefined, // TODO: maybe query a node independenty so not only node with edges are returned, so that they can have properties?
    // TODO: or instead, create edges only among existing nodes??
  }
}


// ---------- random decision utilities ----------------
type Option<T> = {
  weight: number,
  query: T
}

function pickRandomOption<T>(choices: Option<T>[]) {
  const flattened = choices.flatMap(({weight, query}) => [...Array(weight).keys()].map(() => query))
  return flattened[randomInt(flattened.length)]
}

function pickRandom<T>(choices: T[]) {
  return choices[randomInt(choices.length)]
}

function randomIncl<T>(rate: number, value: T) {
  if (Math.random() < rate) {
    return value
  } else {
    return {}
  }
}

function randomAppend<T>(rate: number, value: T) {
  if (Math.random() < rate) {
    return [value]
  } else {
    return []
  }
}

function randomInt(n: number) {
  return Math.floor(Math.random() * n)
}
// ---------------------------------------------------


function randomComposedReadQuery() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: {
        path: "empty"
      },
      applyViews: {
        name: true,
        ...randomPropertyQuery(GRAPH_PROPERTY_RATE),
        ...randomView(GRAPH_VIEW_RATES),
        ...randomEntityQuery()
      }
    }
  })
}

function randomEntityQuery(): GraphGenqlSelection {
  const { numNodes, numEdges } = queryGraphSize("empty")
  const { src, dst, name } = getRandomEntityIds({numEdges})
  if (src === undefined || dst === undefined || name === undefined) {
    return {}
  }
  const nodeQuery = randomNodeQuery()
  const edgeQuery = randomEdgeQuery()
  const queries: Option<GraphGenqlSelection>[] = [
    {
      weight: NODE_PAGE_WEIGHT,
      query: {
        nodes: {
          applyViews: {
            ...randomView(NODE_PAGE_VIEW_RATES),
            page: {
              __args: {
                limit: PAGE_SIZE,
                offset: randomInt(numNodes),
              },
              ...nodeQuery
            }
          },
        }
      }
    },
    {
      weight: NODE_BY_NAME_WEIGHT,
      query: {
        node: {
          __args: {
            name,
          },
          ...nodeQuery
        }
      }
    },
    {
      weight: EDGE_PAGE_WEIGHT,
      query: {
        edges: {
          applyViews: {
            ...randomView(EDGE_PAGE_VIEW_RATES),
            page: {
              __args: {
                limit: PAGE_SIZE,
                offset: randomInt(numEdges),
              },
                ...edgeQuery,
            }
          },
        }
      }
    },
    {
      weight: EDGE_BY_SRC_DST_WEIGHT,
      query: {
        edge: {
          __args: {
            src,
            dst,
          },
          ...edgeQuery,
        }
      }
    }
  ];
  return pickRandomOption(queries);
}

function randomLayer() {
  return pickRandom(LAYERS)
}

function randomView(rate: ViewRate) {
  const [start, end] = [randomTime(), randomTime()].sort()
  const views: PathFromNodeViewCollection[] = [
    ...randomAppend(rate.latest, { latest: true }),
    ...randomAppend(rate.layer, { layer: randomLayer() }),
    ...randomAppend(rate.window, { window: { start, end } }),
  ]
  // TODO: add more kind of filters
  return {
    __args: {
      views
    }
  }
}

function randomTraversal(): NodeGenqlSelection {
  const view = randomView(TRAVERSAL_VIEW_RATES);
  const nodeInner = {
    applyViews: {
      ...view,
      page: {
        __args: {
          limit: PAGE_SIZE,
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
          limit: PAGE_SIZE,
        },
        ...randomEdgeQuery()
      }
    },
  }

  const queries: Option<NodeGenqlSelection>[] = [{
    weight: NEIGHBOURS_WEIGHT,
    query: {
      neighbours: {
        ...nodeInner
      }
    }
  }, {
    weight: IN_NEIGHBOURS_WEIGHT,
    query: {
      inNeighbours: {
        ...nodeInner
      }
    }
  }, {
    weight: OUT_NEIGHBOURS_WEIGHT,
    query: {
      outNeighbours: {
        ...nodeInner
      }
    }
  }, {
    weight: IN_COMPONENT_WEIGHT,
    query: {
      inComponent: {
        ...nodeInner
      }
    }
  }, {
    weight: OUT_COMPONENT_WEIGHT,
    query: {
      outComponent: {
        ...nodeInner
      }
    }
  }, {
    weight: EDGES_WEIGHT,
    query: {
      edges: {
        ...edgeInner
      }
    }
  }, {
    weight: OUT_EDGES_WEIGHT,
    query: {
      outEdges: {
        ...edgeInner
      }
    }
  }, {
    weight: IN_EDGES_WEIGHT,
    query: {
      inEdges: {
        ...edgeInner
      }
    }
  }]
  return pickRandomOption(queries);
}

function randomNodeQuery(): NodeGenqlSelection {
  let traversal: NodeGenqlSelection = {}
  if (Math.random() > 1 - TRAVERSAL_RATIO) { // this is a fire line to avoid infinite recursions
    traversal = randomTraversal();
  }
  return {
    applyViews: {
      name: true,
      ...randomView(NODE_VIEW_RATES),
      ...randomPropertyQuery(NODE_PROPERTY_RATE),
      ...randomIncl(0.5, { degree: true, inDegree: true, outDegree: true}),
      ...traversal,
    }
  }
}

function randomEdgeQuery(): EdgeGenqlSelection {
  const nodeQuery = randomNodeQuery();
  return {
    applyViews: {
      id: true,
      ...randomView(EDGE_VIEW_RATES),
      ...randomPropertyQuery(EDGE_PROPERTY_RATE),
      ...randomIncl(0.5, {src: nodeQuery}),
      ...randomIncl(0.5, {dst: nodeQuery}),
    }
  }
}

type PropertyGenqlSelection = NodeGenqlSelection & EdgeGenqlSelection;
function randomPropertyQuery(rates: PropertyRates): PropertyGenqlSelection {
  return {
    ...randomIncl(rates.metadata, {
      metadata: {
        values: {
          key: true,
          value: true,
        }
      }
    }),
    ...randomIncl(rates.temporalLatest, {
      properties: {
        values: {
          key: true,
          value: true,
        }
      }
    }),
    ...randomIncl(rates.temporalHistory, {
      properties: {
        temporal: {
          values: {
            key: true,
            values: true,
            history: true,
          }
        }
      }
    })
  };
}
