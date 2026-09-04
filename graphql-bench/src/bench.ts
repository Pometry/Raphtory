import { check, fail, sleep } from "k6";
import http from "k6/http";
import { Rate } from "k6/metrics";

import { fetchAndCheck, fetchAndParse, mutate } from "./utils";

const TIME_RANGE = 2000 * 365 * 24 * 60 * 60 * 1000;
const randomTime = () => Math.floor(Math.random() * TIME_RANGE);

export const errorRate = new Rate("errors");

const duration = 1;
const stagesInMinutes: { duration: number; target: number }[] = [
  { duration, target: 100 },
  { duration, target: 400 },
  { duration, target: 1600 },
  { duration, target: 6400 },
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
  shortestPathSingleSource,
  pagerank,
  degreeCentrality,
];

// Run a subset of the ramping scenarios, e.g. k6 run -e ONLY=pagerank,degreeCentrality dist/bench.js
const only = __ENV.ONLY ? new Set(String(__ENV.ONLY).split(",")) : null;
const enabledExecs = only ? execs.filter((e) => only.has(e.name)) : execs;
const rampingScenarios = enabledExecs.map(
  (exec, index) =>
    [
      exec.name,
      {
        executor: "ramping-arrival-rate",
        exec: exec.name,
        startRate: 0,
        startTime: `${index * minutesPerScenario}m`,
        timeUnit: "1s",
        preAllocatedVUs: 5,
        maxVUs: 1000,
        stages: stagesInMinutes.map(({ duration, target }) => ({
          duration: `${duration}m`,
          target,
        })),
      },
    ] as const,
);

// Scheduling scenario: full-graph scans saturate the server's compute pool while short
// queries arrive at a fixed rate. The tracked metric is the short queries' completed rate: if the
// scheduler starves them behind the scans, probe VUs jam on in-flight requests and the rate
// collapses; if short queries get slots promptly, the rate matches the offered rate.
// Run only this pair (30s) with: k6 run -e SCHEDULING_ONLY=1 dist/bench.js
const schedulingOnly = Boolean(__ENV.SCHEDULING_ONLY);
const schedulingStart = schedulingOnly ? "0s" : `${enabledExecs.length * minutesPerScenario}m`;
const schedulingDuration = schedulingOnly ? "30s" : "2m";
const schedulingScenarios = {
  heavy_load: {
    executor: "constant-vus",
    exec: "heavyNameScan",
    vus: 24,
    duration: schedulingDuration,
    startTime: schedulingStart,
  },
  short_queries_under_heavy_load: {
    executor: "constant-arrival-rate",
    exec: "shortCountNodes",
    // Below the dispatch capacity under heavy load: this models occasional short queries (health
    // checks, dashboards) during sustained heavy traffic, not a short-query flood.
    rate: 2,
    timeUnit: "1s",
    duration: schedulingDuration,
    preAllocatedVUs: 5,
    maxVUs: 25,
    startTime: schedulingStart,
  },
};

export const options = {
  scenarios: {
    ...(schedulingOnly ? {} : Object.fromEntries(rampingScenarios)),
    ...(only ? {} : schedulingScenarios),
  },
  // Empty thresholds split these out in the end-of-run summary, so the short queries' latency
  // under load is visible directly rather than folded into the heavy queries' distribution.
  thresholds: {
    "http_req_duration{scenario:heavy_load}": [],
    "http_req_duration{scenario:short_queries_under_heavy_load}": [],
  },
};

type SetupData = {
  graphPaths: string[];
  countNodes: number;
  countEdges: number;
};

export function setup(): SetupData {
  const graphListResponse = fetchAndParse({
    namespaces: { list: { graphs: { list: { path: true } } } },
  });
  const graphPaths = graphListResponse.data.namespaces.list.flatMap((ns: any) =>
    ns.graphs.list.map((graph: any) => graph.path),
  );

  mutate({
    newGraph: {
      __args: {
        path: "empty",
        graphType: "EVENT",
      },
    },
  });

  // this is to trigger the load of the empty graph into memory
  fetchAndCheck(errorRate, {
    graph: {
      __args: {
        path: "empty",
      },
      countNodes: true,
    },
  });

  // Load the generated `big` graph before the scheduling scenarios start, so their measurements
  // reflect scheduling rather than the one-off multi-second graph load.
  fetchAndCheck(errorRate, {
    graph: {
      __args: {
        path: "big",
      },
      countNodes: true,
    },
  });

  const graphResponse = fetchAndParse({
    graph: {
      __args: {
        path: "master",
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
  fetchAndCheck(errorRate, {
    updateGraph: {
      __args: {
        path: "empty",
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
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
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
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      edges: {
        page: {
          __args: { offset, limit: 20 },
          explodeLayers: {
            count: true,
          },
          history: {
            list: {
              timestamp: true,
            },
          },
          src: { name: true },
          dst: { name: true },
        },
      },
    },
  });
}

export function nodePropsByName() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      node: {
        __args: {
          name: "SPARK-22386",
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
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      node: {
        __args: {
          name: "SPARK-22386",
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

export function shortestPathSingleSource() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      algorithm: {
        singleSourceShortestPath: {
          __args: { source: "SPARK-22386" },
          count: true,
        },
      },
    },
  });
}

export function pagerank() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      algorithm: {
        pagerank: {
          count: true,
        },
      },
    },
  });
}

export function degreeCentrality() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "master" },
      algorithm: {
        degreeCentrality: {
          count: true,
        },
      },
    },
  });
}

export function readAndWriteNodeProperties(input: SetupData) {
  const random = Math.random();
  const time = randomTime();
  if (random < 0.3) {
    fetchAndCheck(errorRate, {
      updateGraph: {
        __args: {
          path: "master",
        },
        node: {
          __args: {
            name: "SPARK-22386",
          },
          addUpdates: {
            __args: {
              time,
              properties: [
                { key: "temporal_bool", value: { bool: Math.random() > 0.5 } },
              ],
            },
          },
        },
      },
    });
  } else {
    fetchAndCheck(errorRate, {
      graph: {
        __args: { path: "master" },
        node: {
          __args: {
            name: "SPARK-22386",
          },
          at: {
            __args: {
              time: { simpleTime: time },
            },
            properties: {
              get: {
                __args: {
                  key: "temporal_bool",
                },
                value: true,
              },
            },
          },
        },
      },
    });
  }
}

// A full name scan over the generated `big` graph: one long uninterrupted parallel task per
// request, the load shape that monopolises the compute pool.
export function heavyNameScan() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "big" },
      nodes: {
        __args: { select: { name: { where: { contains: { str: "99999" } } } } },
        count: true,
      },
    },
  });
}

export function shortCountNodes() {
  fetchAndCheck(errorRate, {
    graph: {
      __args: { path: "big" },
      countNodes: true,
    },
  });
}
