"""
In-memory read-only handles via `g.read_only()`.

`Graph.load(path, read_only=True)` is the disk-backed entry point — it
requires the storage feature and lives in `test_graphql/test_read_only_load.py`.
This file covers the in-memory analogue: take a normal `Graph()`, populate
it, then call `g.read_only()` to get a handle that shares the underlying
data but blocks all mutations.
"""

import pytest
from raphtory import Graph


def _build_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"weight": 1.5})
    g.add_edge(2, "lucas", "hamza", {"weight": 2.5})
    g.add_edge(3, "ben", "lucas", {"weight": 3.5})
    g.add_node(4, "ben", {"role": "engineer"})
    return g


def test_read_only_handle_observes_data():
    g = _build_graph()
    ro = g.read_only()

    assert ro.count_nodes() == 3
    assert ro.count_edges() == 3
    assert sorted(n.name for n in ro.nodes) == ["ben", "hamza", "lucas"]
    assert sorted((e.src.name, e.dst.name) for e in ro.edges) == [
        ("ben", "hamza"),
        ("ben", "lucas"),
        ("lucas", "hamza"),
    ]


def test_read_only_blocks_all_mutation_paths():
    """Every mutation entry point on a read-only handle must raise. Covers
    the full graph/node/edge mutation surface — temporal updates,
    metadata creation, and metadata updates — at all three levels."""
    g = _build_graph()
    ro = g.read_only()
    ro_node = ro.node("ben")
    ro_edge = ro.edge("ben", "hamza")

    mutations = [
        # graph-level
        ("graph.add_node", lambda: ro.add_node(99, "new_person")),
        (
            "graph.add_edge_between_existing",
            lambda: ro.add_edge(99, "ben", "hamza", {"weight": 9.9}),
        ),
        ("graph.add_metadata", lambda: ro.add_metadata({"owner": "pometry"})),
        ("graph.update_metadata", lambda: ro.update_metadata({"owner": "new"})),
        (
            "graph.add_properties",
            lambda: ro.add_properties(99, {"version": 2}),
        ),
        # node-level
        (
            "node.add_updates",
            lambda: ro_node.add_updates(99, {"role": "manager"}),
        ),
        ("node.add_metadata", lambda: ro_node.add_metadata({"team": "core"})),
        (
            "node.update_metadata",
            lambda: ro_node.update_metadata({"role": "lead"}),
        ),
        # edge-level
        (
            "edge.add_updates",
            lambda: ro_edge.add_updates(99, {"weight": 9.9}),
        ),
        ("edge.add_metadata", lambda: ro_edge.add_metadata({"kind": "friend"})),
        (
            "edge.update_metadata",
            lambda: ro_edge.update_metadata({"kind": "colleague"}),
        ),
    ]
    for name, mutate in mutations:
        with pytest.raises(Exception) as exc:
            mutate()
        msg = str(exc.value).lower()
        assert (
            "locked" in msg or "immutable" in msg
        ), f"{name} did not raise a locked/immutable error: {exc.value}"


def test_multiple_read_only_handles_share_data():
    g = _build_graph()
    ro1 = g.read_only()
    ro2 = g.read_only()

    assert ro1.count_nodes() == ro2.count_nodes() == 3
    assert ro1.count_edges() == ro2.count_edges() == 3
    assert sorted(n.name for n in ro1.nodes) == sorted(n.name for n in ro2.nodes)


def test_read_only_handle_preserves_properties():
    g = _build_graph()
    ro = g.read_only()

    edge = ro.edge("ben", "hamza")
    assert edge is not None
    assert edge.properties.get("weight") == 1.5

    node = ro.node("ben")
    assert node is not None
    assert node.properties.get("role") == "engineer"
