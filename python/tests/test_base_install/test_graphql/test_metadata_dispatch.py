"""
End-to-end test for `MetaGraph.metadata` format dispatch.

`MetaGraph.metadata` reads a graph's metadata without forcing a full
graph load. Three resolution paths exist:

  1. **In-memory cache hit** — `data.get_cached_graph(path)` returns a
     loaded graph; metadata comes from `graph.metadata()`.
  2. **Disk-backed graph on disk** — read the `graph_props` segment
     directly via `read_constant_graph_properties`.
  3. **Parquet-backed graph on disk** — read the parquet metadata
     footer via `decode_graph_metadata`.

This test runs a `GraphServer` over a work directory containing both a
disk-backed graph and a parquet-backed graph, then queries
`MetaGraph.metadata` to prove all three paths work.
"""

import gc
import json
import os
import tempfile

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

# The disk-backed half of this test requires the storage feature, which
# is not compiled into the base test wheel. Same gate as test_disk_graph.py
# and test_read_only_load.py.
pytestmark = pytest.mark.skipif(
    "DISK_TEST_MARK" not in os.environ,
    reason="disk-backed graph tests require the storage feature",
)

SERVER_URL = "http://localhost:1736"


def _persist_disk_graph(graph_dir):
    """Build a disk-backed graph at `graph_dir`, populate it, flush it,
    and drop the writer so the server can attach to it cleanly."""
    g = Graph(graph_dir)
    g.add_node(1, "alice", {"role": "engineer"})
    g.add_metadata({"format": "disk", "owner": "pometry"})
    g.flush()


def _persist_parquet_graph(graph_dir):
    """Build a graph in memory and write it to ``graph_dir`` as parquet.
    ``to_parquet`` produces a folder containing both the parquet data
    and the ``.meta`` file the server uses for namespace listings."""
    g = Graph()
    g.add_node(1, "bob", {"role": "designer"})
    g.add_metadata({"format": "parquet", "owner": "pometry"})
    g.to_parquet(graph_dir)


def _read_is_diskgraph(graph_dir):
    """Walk ``graph_dir`` for the ``.meta`` JSON file (which lives inside
    a data subdirectory) and return its ``is_diskgraph`` flag — the
    field that drives `MetaGraph.metadata` dispatch."""
    for root, _, files in os.walk(graph_dir):
        if ".meta" in files:
            with open(os.path.join(root, ".meta")) as fp:
                return json.load(fp)["meta"]["is_diskgraph"]
    raise AssertionError(f"no .meta file under {graph_dir}")


def _list_metadata_by_path(client):
    """Query metadata for every graph in the namespace via the standard
    listing field. Returns ``{path: {key: value}}``."""
    result = client.query("""{
            root {
                graphs {
                    list {
                        path
                        metadata { key value }
                    }
                }
            }
        }""")
    return {
        entry["path"]: {item["key"]: item["value"] for item in entry["metadata"]}
        for entry in result["root"]["graphs"]["list"]
    }


def test_metadata_returned_for_both_disk_and_parquet_graphs():
    work_dir = tempfile.mkdtemp()

    # Pre-create both graphs before the server starts. The disk graph
    # needs its writer lock released; the parquet graph is written via
    # `to_parquet`, which produces the format-specific files plus the
    # `.meta` JSON that drives `MetaGraph.metadata` dispatch. We avoid
    # `client.send_graph(...)` here because, with the storage feature
    # compiled in, it would materialize the uploaded graph as a disk
    # graph rather than parquet — which would defeat the point of this
    # test.
    disk_graph_dir = os.path.join(work_dir, "disk_graph")
    parquet_graph_dir = os.path.join(work_dir, "parquet_graph")
    _persist_disk_graph(disk_graph_dir)
    _persist_parquet_graph(parquet_graph_dir)

    # Sanity-check the on-disk format. Without this, a regression that
    # accidentally wrote both graphs in the same format would leave the
    # test still passing (metadata round-trips for either format), and
    # the parquet dispatch path would silently stop being exercised.
    assert (
        _read_is_diskgraph(disk_graph_dir) is True
    ), "disk_graph was not saved as a disk graph"
    assert (
        _read_is_diskgraph(parquet_graph_dir) is False
    ), "parquet_graph was not saved as parquet"

    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)

        # ---- Path 2 (disk on-disk read) and Path 3 (parquet on-disk read).
        # Neither graph has been loaded into the server's cache yet, so
        # MetaGraph.metadata dispatches on `is_diskgraph` and reads
        # directly from the corresponding on-disk format.
        meta = _list_metadata_by_path(client)

        assert set(meta.keys()) == {
            "disk_graph",
            "parquet_graph",
        }, f"unexpected graphs in namespace listing: {sorted(meta)}"

        assert meta["disk_graph"]["format"] == "disk"
        assert meta["disk_graph"]["owner"] == "pometry"

        assert meta["parquet_graph"]["format"] == "parquet"
        assert meta["parquet_graph"]["owner"] == "pometry"

        # ---- Path 1 (in-memory cache hit). Force a load of each graph
        # via the `graph(path: ...)` field; this populates the server's
        # cache. A subsequent metadata query goes through
        # data.get_cached_graph(...) instead of the on-disk readers.
        client.query('{ graph(path: "disk_graph") { created } }')
        client.query('{ graph(path: "parquet_graph") { created } }')

        meta_cached = _list_metadata_by_path(client)
        assert (
            meta_cached == meta
        ), "cached-path metadata should match the on-disk-path metadata"
