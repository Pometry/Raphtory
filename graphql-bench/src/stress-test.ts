import { Rate } from "k6/metrics";

import {
  GraphGenqlSelection,
  EdgeGenqlSelection,
  NodeGenqlSelection,
  PathFromNodeViewCollection,
} from "./__generated";
import {
  defineOp,
  fetchAndCheck,
  fetchAndParse,
  mutate,
  mutateAndCheck,
} from "./utils";

// Global
const VUS = 50;
const PAGE_SIZE = 20;
const LAYERS = ["a", "b", "c"];
const CONST_KEYS = ["ca", "cb", "cc"];
const TEMP_KEYS = ["ta", "tb", "tc"];

// Size of the node universe: node names are picked from `n0 .. n{NUM_NODES - 1}`,
// so this bounds how many distinct nodes the graph can ever contain.
const NUM_NODES = 1000;

// Inclusive bounds for every generated timestamp (updates and window views).
// Keep the range small relative to the number of updates so timestamps repeat.
const TIME_MIN = -100;
const TIME_MAX = 100;

// CONF PARAMETERS
const TRAVERSAL_RATIO = 0.3; // chance for the query to continue with a traversal from a node

// query type rates
const ADD_EDGE_WEIGHT = 1;
const ADD_NODE_WEIGHT = 1;
const DELETE_EDGE_WEIGHT = 1;
const READ_QUERY_RATE = 6;

// Entity query inside of the graph:
const NODE_BY_NAME_WEIGHT = 1;
const EDGE_BY_SRC_DST_WEIGHT = 1;
const NODE_PAGE_WEIGHT = 1;
const EDGE_PAGE_WEIGHT = 1;

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
};
const NODE_PROPERTY_RATE: PropertyRates = {
  metadata: 0.3,
  temporalLatest: 0.3,
  temporalHistory: 0.3,
};
const GRAPH_PROPERTY_RATE: PropertyRates = {
  metadata: 0.3,
  temporalLatest: 0.3,
  temporalHistory: 0.3,
};

// View rates
const GRAPH_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2,
};
const NODE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2,
};
const EDGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.3,
  window: 0.2,
};
const NODE_PAGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2,
};
const EDGE_PAGE_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2,
};
const TRAVERSAL_VIEW_RATES: ViewRate = {
  latest: 0.3,
  layer: 0.2,
  window: 0.2,
};

type PropertyRates = {
  metadata: number;
  temporalLatest: number;
  temporalHistory: number;
};
type ViewRate = {
  layer: number;
  latest: number;
  window: number;
};

const randomTime = () => TIME_MIN + randomInt(TIME_MAX - TIME_MIN + 1);
const randomStr = () => Math.random().toString();
const randomNodeName = () => `n${randomInt(NUM_NODES)}`;
const randomTempKey = () => pickRandom(TEMP_KEYS);
const randomConstKey = () => pickRandom(CONST_KEYS);

export const errorRate = new Rate("errors");

// Labels for every request the test makes. Reads and writes are reported separately
// (`read_duration`/`write_duration`, `read_reqs`/`write_reqs`, `read_errors`/`write_errors`) and
// each op also gets its own `op_<name>` trend, since an iteration mixes very different requests:
// `read_query` is the composed traversal under test, while `graph_size`, `entity_ids` and
// `edge_page` are the small lookups used to pick its arguments and would otherwise drag the read
// median down. Must stay at module level: k6 rejects metrics created after init.
const OP = {
  addEdge: defineOp("add_edge", "write"),
  addNode: defineOp("add_node", "write"),
  deleteEdge: defineOp("delete_edge", "write"),
  readQuery: defineOp("read_query", "read"),
  graphSize: defineOp("graph_size", "read"),
  entityIds: defineOp("entity_ids", "read"),
  edgePage: defineOp("edge_page", "read"),
  setup: defineOp("setup", "write"),
};

const thresholdConf = {
  abortOnFail: true,
  delayAbortEval: "10s",
};
export const options = {
  executor: "constant-vus",
  vus: VUS,
  duration: "7m",
  thresholds: {
    errors: [
      {
        threshold: "rate<0.1", // exec errors should be less than 10%
        ...thresholdConf,
      },
    ],
    http_req_failed: [
      {
        threshold: "rate<0.01", // http errors should be less than 1%
        ...thresholdConf,
      },
    ],
    http_req_duration: [
      {
        threshold: "p(95)<30000", // 95% of requests should be below 30_000ms = 30s
        ...thresholdConf,
      },
    ],
  },
};

export function setup() {
  mutate(
    {
      deleteGraph: {
        __args: {
          path: "empty",
        },
      },
    },
    OP.setup,
  );
  mutateAndCheck(
    errorRate,
    {
      newGraph: {
        __args: {
          path: "empty",
          graphType: "EVENT",
        },
      },
    },
    OP.setup,
  );
}

const QUERIES: Option<() => void>[] = [
  { query: addEdge, weight: ADD_EDGE_WEIGHT },
  { query: addNode, weight: ADD_NODE_WEIGHT },
  { query: deleteEdge, weight: DELETE_EDGE_WEIGHT },
  { query: randomComposedReadQuery, weight: READ_QUERY_RATE },
];

export default function randomReadWriteQuery() {
  const query = pickRandomOption(QUERIES);
  query();
}

function addEdge() {
  fetchAndCheck(
    errorRate,
    {
      updateGraph: {
        __args: {
          path: "empty",
        },
        addEdges: {
          __args: {
            edges: [
              {
                src: randomNodeName(),
                dst: randomNodeName(),
                layer: randomLayer(),
                updates: [
                  {
                    time: randomTime(),
                    properties: [
                      { key: randomTempKey(), value: { str: randomStr() } },
                    ],
                  },
                ],
              },
            ],
          },
        },
      },
    },
    OP.addEdge,
  );
}

function addNode() {
  fetchAndCheck(
    errorRate,
    {
      updateGraph: {
        __args: {
          path: "empty",
        },
        addNodes: {
          __args: {
            nodes: [
              {
                name: randomNodeName(),
                updates: [
                  {
                    time: randomTime(),
                    properties: [
                      { key: randomTempKey(), value: { str: randomStr() } },
                    ],
                  },
                ],
              },
            ],
          },
        },
      },
    },
    OP.addNode,
  );
}

function queryGraphSize(path: string) {
  const response = fetchAndParse(
    {
      graph: {
        __args: { path },
        countNodes: true,
        countEdges: true,
      },
    },
    OP.graphSize,
  );
  return {
    numNodes: response.data.graph.countNodes as number,
    numEdges: response.data.graph.countEdges as number,
  };
}

function deleteEdge() {
  const { numEdges } = queryGraphSize("empty");
  if (!numEdges || numEdges <= 0) return;

  const edgeIndex = randomInt(numEdges);
  const response = fetchAndParse(
    {
      graph: {
        __args: { path: "empty" },
        edges: {
          page: {
            __args: { limit: 1, offset: edgeIndex },
            src: { name: true },
            dst: { name: true },
          },
        },
      },
    },
    OP.edgePage,
  );

  const edge = response?.data?.graph?.edges?.page?.[0];
  if (!edge?.src?.name || !edge?.dst?.name) return;

  fetchAndCheck(
    errorRate,
    {
      updateGraph: {
        __args: { path: "empty" },
        deleteEdge: {
          __args: {
            time: randomTime(),
            src: edge.src.name,
            dst: edge.dst.name,
          },
          success: true,
        },
      },
    },
    OP.deleteEdge,
  );
}

function getRandomEntityIds({
  numEdges,
  numNodes,
}: {
  numEdges: number;
  numNodes: number;
}) {
  if (numEdges <= 0 && numNodes <= 0) {
    return { src: undefined, dst: undefined, name: undefined };
  }

  const response = fetchAndParse(
    {
      graph: {
        __args: { path: "empty" },
        ...(numNodes > 0
          ? {
              nodes: {
                page: {
                  __args: { limit: 1, offset: randomInt(numNodes) },
                  name: true,
                },
              },
            }
          : {}),
        ...(numEdges > 0
          ? {
              edges: {
                page: {
                  __args: { limit: 1, offset: randomInt(numEdges) },
                  src: { name: true },
                  dst: { name: true },
                },
              },
            }
          : {}),
      },
    },
    OP.entityIds,
  );

  const node = response?.data?.graph?.nodes?.page?.[0]?.name as
    | string
    | undefined;
  const edge = response?.data?.graph?.edges?.page?.[0];
  const src = edge?.src?.name as string | undefined;
  const dst = edge?.dst?.name as string | undefined;

  const candidates = [node, src, dst].filter(
    (x): x is string => typeof x === "string",
  );
  const name = candidates.length ? pickRandom(candidates) : undefined;

  return { src, dst, name };
}

// ---------- random decision utilities ----------------
type Option<T> = {
  weight: number;
  query: T;
};

function pickRandomOption<T>(choices: Option<T>[]) {
  const flattened = choices.flatMap(({ weight, query }) =>
    [...Array(weight).keys()].map(() => query),
  );
  let query = flattened[randomInt(flattened.length)];
  // console.log("random query", query)
  return query;
}

function pickRandom<T>(choices: T[]) {
  return choices[randomInt(choices.length)];
}

function randomIncl<T>(rate: number, value: T) {
  if (Math.random() < rate) {
    return value;
  } else {
    return {};
  }
}

function randomAppend<T>(rate: number, value: T) {
  if (Math.random() < rate) {
    return [value];
  } else {
    return [];
  }
}

function randomInt(n: number) {
  return Math.floor(Math.random() * n);
}
// ---------------------------------------------------

function randomComposedReadQuery() {
  fetchAndCheck(
    errorRate,
    {
      graph: {
        __args: {
          path: "empty",
        },
        applyViews: {
          name: true,
          ...randomPropertyQuery(GRAPH_PROPERTY_RATE),
          ...randomView(GRAPH_VIEW_RATES),
          ...randomEntityQuery(),
        },
      },
    },
    OP.readQuery,
  );
}

function randomEntityQuery(): GraphGenqlSelection {
  const { numNodes, numEdges } = queryGraphSize("empty");
  const { src, dst, name } = getRandomEntityIds({ numNodes, numEdges });
  if (src === undefined || dst === undefined || name === undefined) {
    return {};
  }
  const nodeQuery = randomNodeQuery();
  const edgeQuery = randomEdgeQuery();
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
              ...nodeQuery,
            },
          },
        },
      },
    },
    {
      weight: NODE_BY_NAME_WEIGHT,
      query: {
        node: {
          __args: {
            name,
          },
          ...nodeQuery,
        },
      },
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
            },
          },
        },
      },
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
        },
      },
    },
  ];
  return pickRandomOption(queries);
}

function randomLayer() {
  return pickRandom(LAYERS);
}

function randomView(rate: ViewRate) {
  const [start, end] = [randomTime(), randomTime()].sort((a, b) => a - b);
  const views: PathFromNodeViewCollection[] = [
    ...randomAppend(rate.latest, { latest: true }),
    ...randomAppend(rate.layer, { layers: [randomLayer()] }),
    ...randomAppend(rate.window, { window: { start, end } }),
  ];
  // TODO: add more kind of filters
  return {
    __args: {
      views,
    },
  };
}

const MAX_DEPTH = 3;

function randomNodeQuery(depth: number = MAX_DEPTH): NodeGenqlSelection {
  const allowTraversal = depth > 0 && Math.random() < TRAVERSAL_RATIO;

  const traversal: NodeGenqlSelection = allowTraversal
    ? randomTraversal(depth - 1)
    : {};

  return {
    applyViews: {
      name: true,
      ...randomView(NODE_VIEW_RATES),
      ...randomPropertyQuery(NODE_PROPERTY_RATE),
      ...randomIncl(0.5, { degree: true, inDegree: true, outDegree: true }),
      ...traversal,
    },
  };
}

function shallowNodeQuery(): NodeGenqlSelection {
  return {
    applyViews: {
      name: true,
      ...randomView(NODE_VIEW_RATES),
      ...randomPropertyQuery(NODE_PROPERTY_RATE),
      ...randomIncl(0.5, { degree: true, inDegree: true, outDegree: true }),
    },
  };
}

function randomEdgeQuery(depth: number = MAX_DEPTH): EdgeGenqlSelection {
  const endpointNode =
    depth > 0 ? randomNodeQuery(depth - 1) : shallowNodeQuery();

  return {
    applyViews: {
      id: true,
      ...randomView(EDGE_VIEW_RATES),
      ...randomPropertyQuery(EDGE_PROPERTY_RATE),
      ...randomIncl(0.5, { src: endpointNode }),
      ...randomIncl(0.5, { dst: endpointNode }),
    },
  };
}

function randomTraversal(depth: number): NodeGenqlSelection {
  if (depth <= 0) return {};

  const view = randomView(TRAVERSAL_VIEW_RATES);

  const nodeInner = {
    applyViews: {
      ...view,
      page: { __args: { limit: PAGE_SIZE }, ...randomNodeQuery(depth - 1) },
    },
  };

  const edgeInner = {
    applyViews: {
      ...view,
      page: { __args: { limit: PAGE_SIZE }, ...randomEdgeQuery(depth - 1) },
    },
  };

  const queries: Option<NodeGenqlSelection>[] = [
    { weight: NEIGHBOURS_WEIGHT, query: { neighbours: { ...nodeInner } } },
    { weight: IN_NEIGHBOURS_WEIGHT, query: { inNeighbours: { ...nodeInner } } },
    {
      weight: OUT_NEIGHBOURS_WEIGHT,
      query: { outNeighbours: { ...nodeInner } },
    },
    { weight: IN_COMPONENT_WEIGHT, query: { inComponent: { ...nodeInner } } },
    { weight: OUT_COMPONENT_WEIGHT, query: { outComponent: { ...nodeInner } } },
    { weight: EDGES_WEIGHT, query: { edges: { ...edgeInner } } },
    { weight: OUT_EDGES_WEIGHT, query: { outEdges: { ...edgeInner } } },
    { weight: IN_EDGES_WEIGHT, query: { inEdges: { ...edgeInner } } },
  ];

  return pickRandomOption(queries);
}

type PropertyGenqlSelection = NodeGenqlSelection & EdgeGenqlSelection;
function randomPropertyQuery(rates: PropertyRates): PropertyGenqlSelection {
  return {
    ...randomIncl(rates.metadata, {
      metadata: {
        values: {
          key: true,
          value: true,
        },
      },
    }),
    ...randomIncl(rates.temporalLatest, {
      properties: {
        values: {
          key: true,
          value: true,
        },
      },
    }),
    ...randomIncl(rates.temporalHistory, {
      properties: {
        temporal: {
          values: {
            key: true,
            values: true,
            history: {
              list: {
                timestamp: true,
              },
            },
          },
        },
      },
    }),
  };
}
