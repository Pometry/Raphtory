import logging
import os
import time

import numpy as np
import pandas as pd
from raphtory import Graph, graphql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Graph for the heavy_load / short_queries_under_heavy_load scenarios: big enough that a full
# name scan takes hundreds of milliseconds. Built once and cached.
BIG_NODES = int(os.environ.get("BENCH_BIG_NODES", "5000000"))
BIG_PATH = os.path.join("data", "apache", "big")

if os.path.exists(BIG_PATH):
    logging.info("reusing the cached bench graph at %s", BIG_PATH)
else:
    # This is minutes of work on a cold checkout, and nothing is listening on the port until it
    # finishes, so the elapsed time is logged loudly: a bench run whose k6 setup() could not
    # connect is almost always one that started while this was still going.
    logging.info("building the %s-node bench graph at %s", BIG_NODES, BIG_PATH)
    build_started = time.monotonic()
    rng = np.random.default_rng(seed=42)
    g = Graph()
    chunk = 1_000_000
    for start in range(0, BIG_NODES, chunk):
        hi = min(start + chunk, BIG_NODES)
        g.load_nodes(
            pd.DataFrame({"id": np.arange(start, hi).astype(str), "time": 1}),
            time="time",
            id="id",
        )
        g.load_edges(
            pd.DataFrame(
                {
                    "src": rng.integers(0, hi, size=hi - start).astype(str),
                    "dst": rng.integers(0, hi, size=hi - start).astype(str),
                    "time": 1,
                }
            ),
            time="time",
            src="src",
            dst="dst",
        )
        logging.info(
            "loaded %s of %s nodes (%.1fs elapsed)",
            hi,
            BIG_NODES,
            time.monotonic() - build_started,
        )
    g.save_to_file(BIG_PATH)
    logging.info(
        "bench graph saved to %s after %.1fs",
        BIG_PATH,
        time.monotonic() - build_started,
    )
    del g

logging.info("starting the graphql server on work_dir=data/apache")
graphql.GraphServer(work_dir="data/apache").run()
