import logging
import os

import numpy as np
import pandas as pd
from raphtory import Graph, graphql

logging.basicConfig(level=logging.INFO)

# Graph for the heavy_load / short_queries_under_heavy_load scenarios: big enough that a full
# name scan takes hundreds of milliseconds. Built once and cached.
BIG_NODES = int(os.environ.get("BENCH_BIG_NODES", "5000000"))
BIG_PATH = os.path.join("data", "apache", "big")

if not os.path.exists(BIG_PATH):
    logging.info("building the %s-node bench graph at %s", BIG_NODES, BIG_PATH)
    rng = np.random.default_rng(seed=42)
    g = Graph()
    chunk = 1_000_000
    for start in range(0, BIG_NODES, chunk):
        hi = min(start + chunk, BIG_NODES)
        g.load_nodes(
            pd.DataFrame({"id": np.arange(start, hi, dtype="uint64"), "time": 1}),
            time="time",
            id="id",
        )
        g.load_edges(
            pd.DataFrame(
                {
                    "src": rng.integers(0, hi, size=hi - start, dtype="uint64"),
                    "dst": rng.integers(0, hi, size=hi - start, dtype="uint64"),
                    "time": 1,
                }
            ),
            time="time",
            src="src",
            dst="dst",
        )
    g.save_to_file(BIG_PATH)
    del g

graphql.GraphServer(work_dir="data/apache").run()
