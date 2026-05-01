"""
End-to-end test for read-only graph loading.

`Graph.load(path, read_only=True)` is meant to let a separate Python
process attach to a graph directory another process (e.g. the GraphQL
server) is currently holding open, without contending for any exclusive
lock and without permitting writes.

These tests verify both halves of that contract:
  1. The server is running and "owns" the directory; a fresh read-only
     handle to the same path opens cleanly and sees the data.
  2. Mutations on that read-only handle fail loudly rather than silently
     corrupting state.
"""

import gc
import json
import os
import subprocess
import sys
import tempfile

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

SERVER_URL = "http://localhost:1736"


def _persist_graph(graph_dir):
    """Build a disk-backed graph at `graph_dir`, populate it, flush it, and
    drop the writer so the directory is fully on disk and the writer's lock
    is released before the server (or anything else) opens it.
    """
    g = Graph(graph_dir)
    g.add_edge(1, "ben", "hamza", {"weight": 1.5})
    g.add_edge(2, "lucas", "hamza", {"weight": 2.5})
    g.add_edge(3, "ben", "lucas", {"weight": 3.5})
    g.add_node(4, "ben", {"role": "engineer"})
    g.flush()
    del g  # release the resolver's writer lock before any other handle attaches


def _make_work_dir_with_graph(name="g"):
    work_dir = tempfile.mkdtemp()
    graph_dir = os.path.join(work_dir, name)
    _persist_graph(graph_dir)
    gc.collect()
    return work_dir, graph_dir


def test_read_only_load_while_server_owns_directory():
    """While the server is running and holding the writer's lock on
    `<work_dir>/g`, a separate `Graph.load(..., read_only=True)` call in the
    same process should succeed and observe the data.

    GraphQL queries against the server and direct Python reads against the
    read-only handle should both succeed and report the same data.
    """
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)

        # Trigger the server to load the graph so it holds the writer.
        client.query('{ graph(path: "g") { created } }')

        ro = Graph.load(graph_dir, read_only=True)

        # --- direct Python reads on the read-only handle ---
        assert ro.count_nodes() == 3
        assert ro.count_edges() == 3
        py_names = sorted(n.name for n in ro.nodes)
        assert py_names == ["ben", "hamza", "lucas"]

        # --- GraphQL queries against the server, on the same graph ---
        gql_names_result = client.query(
            '{ graph(path: "g") { nodes { page(limit: 10) { name } } } }'
        )
        gql_names = sorted(
            n["name"] for n in gql_names_result["graph"]["nodes"]["page"]
        )
        assert gql_names == py_names

        gql_edges_result = client.query(
            '{ graph(path: "g") { edges { page(limit: 10) { src { name } dst { name } } } } }'
        )
        gql_edges = sorted(
            (e["src"]["name"], e["dst"]["name"])
            for e in gql_edges_result["graph"]["edges"]["page"]
        )
        py_edges = sorted((e.src.name, e.dst.name) for e in ro.edges)
        assert gql_edges == py_edges


def test_graphql_and_read_only_handle_interleaved():
    """Interleave GraphQL queries to the server with reads on a separate
    read-only handle to the same directory. Both pathways should keep
    serving consistent results across multiple round-trips."""
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        # Prime the server's load.
        client.query('{ graph(path: "g") { created } }')
        ro = Graph.load(graph_dir, read_only=True)

        for _ in range(3):
            gql_nodes = client.query(
                '{ graph(path: "g") { nodes { page(limit: 100) { name } } } }'
            )
            assert len(gql_nodes["graph"]["nodes"]["page"]) == ro.count_nodes() == 3

            gql_edges = client.query(
                '{ graph(path: "g") { edges { page(limit: 100) { src { name } } } } }'
            )
            assert len(gql_edges["graph"]["edges"]["page"]) == ro.count_edges() == 3


def test_read_only_load_blocks_all_mutation_paths():
    """Every mutation entry point on a read-only handle must raise — not
    just node-id resolution. Covers:
      - new node (goes through id resolution)
      - edge between existing nodes (only needs VID lookups, no id resolver)
      - graph-level metadata (never touches the id resolver at all)
    """
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        client.query('{ graph(path: "g") { created } }')
        ro = Graph.load(graph_dir, read_only=True)

        mutations = [
            ("add_node", lambda: ro.add_node(99, "new_person")),
            (
                "add_edge_between_existing",
                lambda: ro.add_edge(99, "ben", "hamza", {"weight": 9.9}),
            ),
            ("add_metadata", lambda: ro.add_metadata({"owner": "pometry"})),
        ]
        for name, mutate in mutations:
            with pytest.raises(Exception) as exc:
                mutate()
            msg = str(exc.value).lower()
            assert (
                "locked" in msg or "immutable" in msg
            ), f"{name} did not raise a locked/immutable error: {exc.value}"


def test_writer_load_against_live_server_directory_fails():
    """Sanity-check the contrapositive: if the server is holding the writer
    on `<work_dir>/g`, attempting to open *another* writer (the default,
    `read_only=False`) against the same directory must fail. This proves
    `read_only=True` is not accidentally a no-op flag — it really is the
    only way to coexist with a live writer."""
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        # Force the server to take the writer lock by loading the graph.
        client.query('{ graph(path: "g") { created } }')

        with pytest.raises(Exception):
            Graph.load(graph_dir)  # read_only defaults to False


def test_multiple_read_only_handles_can_coexist_with_server():
    """Two simultaneous read-only handles + the server writer = three total
    attachments to the same graph directory."""
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        client.query('{ graph(path: "g") { created } }')

        ro1 = Graph.load(graph_dir, read_only=True)
        ro2 = Graph.load(graph_dir, read_only=True)

        assert ro1.count_nodes() == ro2.count_nodes() == 3
        assert ro1.count_edges() == ro2.count_edges() == 3


# --- separate-process variant ---------------------------------------------
#
# The above tests open the read-only handle from the same Python interpreter
# the server is running in. To prove there is genuinely no exclusive
# OS-level lock, the test below shells out to a fresh Python process that
# attaches to the live directory while the parent process's server is still
# running.

_SUBPROCESS_READER = """
import json, sys
from raphtory import Graph

g = Graph.load(sys.argv[1], read_only=True)
print(json.dumps({
    "nodes": g.count_nodes(),
    "edges": g.count_edges(),
    "names": sorted(n.name for n in g.nodes),
}))
"""


def test_flush_via_update_graph_makes_writes_visible_to_read_only_handle():
    """`{ updateGraph(path: "g") { flush } }` should persist the server's
    in-memory state to disk so a freshly opened read-only handle sees it.

    Without the flush, the read-only handle (a snapshot of what's on disk
    when it opens) wouldn't see writes the server has made via
    `updateGraph` since the last flush."""
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        # Server loads as writer.
        client.query('{ graph(path: "g") { created } }')

        # Mutate via GraphQL — this lands in the server's memory only.
        client.query(
            '{ updateGraph(path: "g") { addNode(time: 100, name: "new_after_flush") { success } } }'
        )

        # Force the server to persist via the same updateGraph entry point.
        flush_result = client.query('{ updateGraph(path: "g") { flush } }')
        assert flush_result["updateGraph"]["flush"] is True

        # Open a fresh read-only handle — must see the post-flush state.
        ro = Graph.load(graph_dir, read_only=True)
        names = sorted(n.name for n in ro.nodes)
        assert "new_after_flush" in names


def test_read_only_load_from_a_separate_python_process():
    work_dir, graph_dir = _make_work_dir_with_graph()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        client.query('{ graph(path: "g") { created } }')

        result = subprocess.run(
            [sys.executable, "-c", _SUBPROCESS_READER, graph_dir],
            check=True,
            capture_output=True,
            text=True,
        )

        payload = json.loads(result.stdout)
        assert payload["nodes"] == 3
        assert payload["edges"] == 3
        assert payload["names"] == ["ben", "hamza", "lucas"]
