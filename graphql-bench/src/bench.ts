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
];
const scenarios = execs.map(
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

export const options = {
  scenarios: Object.fromEntries(scenarios),
};

type SetupData = {
  graphPaths: string[];
  countNodes: number;
  countEdges: number;
};

export function setup(): SetupData {
  console.log("=== Setup Phase Starting ===");

  const graphListResponse = fetchAndParse({
    namespaces: { list: { graphs: { list: { path: true } } } },
  });
  console.log(
    "Graph list response:",
    JSON.stringify(graphListResponse, null, 2),
  );

  const graphPaths = graphListResponse.data.namespaces.list.flatMap((ns: any) =>
    ns.graphs.list.map((graph: any) => graph.path),
  );
  console.log("Found graph paths:", graphPaths);

  mutate({
    newGraph: {
      __args: {
        path: "empty",
        graphType: "EVENT",
      },
    },
  });
  console.log("Created empty graph");

  // this is to trigger the load of the empty graph into memory
  fetchAndCheck(errorRate, {
    graph: {
      __args: {
        path: "empty",
      },
      countNodes: true,
    },
  });
  console.log("Loaded empty graph into memory");

  const graphResponse = fetchAndParse({
    graph: {
      __args: {
        path: "master",
      },
      countNodes: true,
      countEdges: true,
    },
  });
  console.log("Master graph response:", JSON.stringify(graphResponse, null, 2));

  const setupData = {
    graphPaths,
    countNodes: graphResponse.data.graph.countNodes,
    countEdges: graphResponse.data.graph.countEdges,
  };
  console.log("=== Setup Complete ===");
  console.log("Setup data:", JSON.stringify(setupData, null, 2));

  return setupData;
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
          history: true,
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
              time,
            },
            properties: {
              get: {
                __args: {
                  key: "temporal_bool",
                },
              },
            },
          },
        },
      },
    });
  }
}
