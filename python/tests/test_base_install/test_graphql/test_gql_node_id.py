"""Node-id arguments now accept the full `NodeId` shape:

- A `String` (e.g. `"alice"`).
- A non-negative `Int` (e.g. `42`).

These tests exercise both forms across the major lookup, mutation, and
view-transform surfaces, and confirm a graph indexed by integers can be
queried and mutated through the GraphQL server.
"""

import json
import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer

from utils import PORT


def _query(server, q: str) -> dict:
    response = server.get_client().query(q)
    return json.loads(response) if isinstance(response, str) else response


def test_addnode_and_node_lookup_with_integer_ids():
    """A graph with integer node ids can be added and queried via the
    GraphQL server. Raphtory enforces a single id type per graph, so this
    test uses integers throughout."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                a: addNode(time: 1, name: 1) { success }
                b: addNode(time: 2, name: 2) { success }
                c: addNode(time: 3, name: 42) { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                hasInt:    hasNode(name: 1)
                hasOther:  hasNode(name: 42)
                hasMissingInt: hasNode(name: 999)
                int_node:  node(name: 1)  { earliestTime { timestamp } }
                int_node2: node(name: 42) { earliestTime { timestamp } }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["hasInt"] is True
        assert graph["hasOther"] is True
        assert graph["hasMissingInt"] is False
        assert graph["int_node"]["earliestTime"]["timestamp"] == 1
        assert graph["int_node2"]["earliestTime"]["timestamp"] == 3


def test_addedge_and_edge_lookup_with_integer_endpoints():
    """Edge mutations and lookups accept integer ids on src/dst."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                e1: addEdge(time: 10, src: 1, dst: 2) { success }
                e2: addEdge(time: 20, src: 2, dst: 3) { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                hasIntEdge:  hasEdge(src: 1, dst: 2)
                hasIntEdge2: hasEdge(src: 2, dst: 3)
                hasNoEdge:   hasEdge(src: 1, dst: 3)
                e1: edge(src: 1, dst: 2) { earliestTime { timestamp } }
                e2: edge(src: 2, dst: 3) { earliestTime { timestamp } }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["hasIntEdge"] is True
        assert graph["hasIntEdge2"] is True
        assert graph["hasNoEdge"] is False
        assert graph["e1"]["earliestTime"]["timestamp"] == 10
        assert graph["e2"]["earliestTime"]["timestamp"] == 20


def test_view_transforms_with_integer_node_ids():
    """`subgraph`, `excludeNodes`, and `sharedNeighbours` accept integer
    node ids."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        # Build a small integer-id graph: 1 → 2, 1 → 3, 4 → 2 (so 1 and 4 share neighbour 2).
        client.query("""
            {
              updateGraph(path: "g") {
                e1: addEdge(time: 1, src: 1, dst: 2) { success }
                e2: addEdge(time: 2, src: 1, dst: 3) { success }
                e3: addEdge(time: 3, src: 4, dst: 2) { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                sub:     subgraph(nodes: [1, 2])     { countNodes }
                exclude: excludeNodes(nodes: [3])    { countNodes }
                shared:  sharedNeighbours(selectedNodes: [1, 4]) { id }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["sub"]["countNodes"] == 2
        assert graph["exclude"]["countNodes"] == 3  # 1, 2, 4 (3 removed)
        # `1` and `4` both connect to `2`, so 2 is the shared neighbour.
        # Integer-indexed graph → `id` comes back as a number.
        shared_ids = sorted(s["id"] for s in graph["shared"])
        assert shared_ids == [2]


def test_batch_addnodes_addedges_with_integer_ids():
    """`addNodes` and `addEdges` accept integer ids in `name`/`src`/`dst`."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                addNodes(nodes: [
                  { name: 1,  updates: [{ time: 1, properties: [{key: "v", value: {i64: 10}}] }] }
                  { name: 42, updates: [{ time: 2, properties: [{key: "v", value: {i64: 20}}] }] }
                ])
                addEdges(edges: [
                  { src: 1, dst: 2,  updates: [{ time: 3, properties: [{key: "w", value: {f64: 1.5}}] }] }
                  { src: 2, dst: 42, updates: [{ time: 4, properties: [{key: "w", value: {f64: 2.5}}] }] }
                ])
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                n1: node(name: 1)           { properties { temporal { get(key: "v") { values } } } }
                n2: node(name: 42)          { properties { temporal { get(key: "v") { values } } } }
                e1: edge(src: 1, dst: 2)    { properties { temporal { get(key: "w") { values } } } }
                e2: edge(src: 2, dst: 42)   { properties { temporal { get(key: "w") { values } } } }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["n1"]["properties"]["temporal"]["get"]["values"] == [10]
        assert graph["n2"]["properties"]["temporal"]["get"]["values"] == [20]
        assert graph["e1"]["properties"]["temporal"]["get"]["values"] == [1.5]
        assert graph["e2"]["properties"]["temporal"]["get"]["values"] == [2.5]


def test_view_transforms_with_string_node_ids():
    """`subgraph`, `excludeNodes`, and `sharedNeighbours` accept string node ids."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        # alice → bob, alice → carol, dave → bob (alice and dave share bob).
        client.query("""
            {
              updateGraph(path: "g") {
                e1: addEdge(time: 1, src: "alice", dst: "bob")   { success }
                e2: addEdge(time: 2, src: "alice", dst: "carol") { success }
                e3: addEdge(time: 3, src: "dave",  dst: "bob")   { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                sub:     subgraph(nodes: ["alice", "bob"])      { countNodes }
                exclude: excludeNodes(nodes: ["carol"])         { countNodes }
                shared:  sharedNeighbours(selectedNodes: ["alice", "dave"]) { id }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["sub"]["countNodes"] == 2
        assert graph["exclude"]["countNodes"] == 3  # alice, bob, dave
        shared_ids = sorted(s["id"] for s in graph["shared"])
        assert shared_ids == ["bob"]


def test_batch_addnodes_addedges_with_string_ids():
    """`addNodes` and `addEdges` accept string ids in `name`/`src`/`dst`."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                addNodes(nodes: [
                  { name: "alice", updates: [{ time: 1, properties: [{key: "v", value: {i64: 10}}] }] }
                  { name: "bob",   updates: [{ time: 2, properties: [{key: "v", value: {i64: 20}}] }] }
                ])
                addEdges(edges: [
                  { src: "alice", dst: "bob",   updates: [{ time: 3, properties: [{key: "w", value: {f64: 1.5}}] }] }
                  { src: "bob",   dst: "carol", updates: [{ time: 4, properties: [{key: "w", value: {f64: 2.5}}] }] }
                ])
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                n1: node(name: "alice")                  { properties { temporal { get(key: "v") { values } } } }
                n2: node(name: "bob")                    { properties { temporal { get(key: "v") { values } } } }
                e1: edge(src: "alice", dst: "bob")       { properties { temporal { get(key: "w") { values } } } }
                e2: edge(src: "bob",   dst: "carol")     { properties { temporal { get(key: "w") { values } } } }
              }
            }
            """,
        )
        graph = result["graph"]
        assert graph["n1"]["properties"]["temporal"]["get"]["values"] == [10]
        assert graph["n2"]["properties"]["temporal"]["get"]["values"] == [20]
        assert graph["e1"]["properties"]["temporal"]["get"]["values"] == [1.5]
        assert graph["e2"]["properties"]["temporal"]["get"]["values"] == [2.5]


def test_string_ids_remain_unchanged_for_existing_clients():
    """Existing clients passing string node ids continue to work without
    modification — the schema change is wire-compatible for strings."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                addNode(time: 1, name: "alice") { success }
                addEdge(time: 2, src: "alice", dst: "bob") { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                node(name: "alice")         { earliestTime { timestamp } }
                edge(src: "alice", dst: "bob") { earliestTime { timestamp } }
              }
            }
            """,
        )
        assert result["graph"]["node"]["earliestTime"]["timestamp"] == 1
        assert result["graph"]["edge"]["earliestTime"]["timestamp"] == 2


def test_negative_integer_rejected():
    """Schema rejects negative integers — `NodeId` only accepts non-negative."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        try:
            client.query("""
                {
                  updateGraph(path: "g") {
                    addNode(time: 1, name: -1) { success }
                  }
                }
                """)
            raise AssertionError(
                "Expected schema-level rejection for negative integer NodeId"
            )
        except Exception as e:
            assert "NodeId" in str(e) or "non-negative" in str(
                e
            ), f"Expected NodeId rejection, got: {e}"
