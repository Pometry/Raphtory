"""Every mutation that takes a `TimeInput` must auto-allocate a fresh
`event_id` when the user passes only a timestamp (no `eventId`). Two writes
at the same millisecond should therefore produce two distinct history
entries, not be deduplicated.

Before the `GqlTimeInput` refactor, every Number/String time input was
coerced to `EventTime { t, event_id: 0 }`, so two same-ms writes shared the
same composite key and the second silently overwrote the first.

These tests pin down the fix on every mutation surface that takes a time.
Each test checks both the timestamps in the history and the values, so a
regression that drops a timestamp entry (not just a value) would still
fail.
"""

import json
import tempfile

from raphtory.graphql import GraphServer

from utils import PORT


def _query(server, q: str) -> dict:
    response = server.get_client().query(q)
    return json.loads(response) if isinstance(response, str) else response


def _new_event_graph(server):
    server.get_client().new_graph("g", "EVENT")


def _temporal_history(get_block: dict) -> tuple[list, list]:
    """Return (timestamps, values) for a `temporal.get(key:)` GraphQL block
    that selected `history { list { timestamp } } values`."""
    timestamps = [h["timestamp"] for h in get_block["history"]["list"]]
    return timestamps, get_block["values"]


# ----- Top-level graph properties --------------------------------------------


def test_add_properties_same_timestamp_appends():
    """`addProperties` three times at the same ms → three history entries
    with the same timestamp and distinct values."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                a: addProperties(t: 100, properties: [{key: "x", value: {i64: 1}}])
                b: addProperties(t: 100, properties: [{key: "x", value: {i64: 2}}])
                c: addProperties(t: 100, properties: [{key: "x", value: {i64: 3}}])
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") { properties { temporal { get(key: "x") {
                history { list { timestamp } }
                values
            } } } } }
            """,
        )
        ts, values = _temporal_history(
            result["graph"]["properties"]["temporal"]["get"]
        )
        assert ts == [100, 100, 100]
        assert values == [1, 2, 3]


# ----- Single-call addNode / addEdge / createNode ----------------------------


def test_add_node_same_timestamp_appends():
    """`addNode` twice at the same ms with different per-event properties →
    both updates land. Verified on the node's `history` and on the
    temporal property's `history`."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                a: addNode(time: 100, name: "n", properties: [{key: "v", value: {i64: 1}}]) { success }
                b: addNode(time: 100, name: "n", properties: [{key: "v", value: {i64: 2}}]) { success }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                node(name: "n") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "v") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        node = result["graph"]["node"]
        node_ts = [h["timestamp"] for h in node["history"]["list"]]
        assert node_ts == [100, 100]
        ts, values = _temporal_history(node["properties"]["temporal"]["get"])
        assert ts == [100, 100]
        assert values == [1, 2]


def test_add_edge_same_timestamp_appends():
    """`addEdge` twice at the same ms → both edge updates land. Verified on
    the edge's `history` and on the temporal property's `history`."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                a: addEdge(time: 100, src: "a", dst: "b", properties: [{key: "w", value: {i64: 1}}]) { success }
                b: addEdge(time: 100, src: "a", dst: "b", properties: [{key: "w", value: {i64: 2}}]) { success }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                edge(src: "a", dst: "b") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "w") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        edge = result["graph"]["edge"]
        edge_ts = [h["timestamp"] for h in edge["history"]["list"]]
        assert edge_ts == [100, 100]
        ts, values = _temporal_history(edge["properties"]["temporal"]["get"])
        assert ts == [100, 100]
        assert values == [1, 2]


def test_create_node_then_add_node_same_timestamp_appends():
    """`createNode` followed by `addNode` at the same ms → both updates
    land (createNode creates the node, addNode appends an update)."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                a: createNode(time: 100, name: "n", properties: [{key: "v", value: {i64: 1}}]) { success }
                b: addNode(time: 100, name: "n", properties: [{key: "v", value: {i64: 2}}]) { success }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                node(name: "n") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "v") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        node = result["graph"]["node"]
        node_ts = [h["timestamp"] for h in node["history"]["list"]]
        assert node_ts == [100, 100]
        ts, values = _temporal_history(node["properties"]["temporal"]["get"])
        assert ts == [100, 100]
        assert values == [1, 2]


# ----- MutableNode.addUpdates / MutableEdge.addUpdates -----------------------


def test_mutable_node_add_updates_same_timestamp_appends():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            { updateGraph(path: "g") { addNode(time: 0, name: "n") { success } } }
            """
        )
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                node(name: "n") {
                  a: addUpdates(time: 100, properties: [{key: "v", value: {i64: 1}}])
                  b: addUpdates(time: 100, properties: [{key: "v", value: {i64: 2}}])
                }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                node(name: "n") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "v") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        node = result["graph"]["node"]
        node_ts = [h["timestamp"] for h in node["history"]["list"]]
        # t=0 from the seed addNode plus two t=100 updates.
        assert node_ts == [0, 100, 100]
        ts, values = _temporal_history(node["properties"]["temporal"]["get"])
        assert ts == [100, 100]
        assert values == [1, 2]


def test_mutable_edge_add_updates_same_timestamp_appends():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            { updateGraph(path: "g") { addEdge(time: 0, src: "a", dst: "b") { success } } }
            """
        )
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                edge(src: "a", dst: "b") {
                  x: addUpdates(time: 100, properties: [{key: "w", value: {i64: 1}}])
                  y: addUpdates(time: 100, properties: [{key: "w", value: {i64: 2}}])
                }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                edge(src: "a", dst: "b") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "w") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        edge = result["graph"]["edge"]
        edge_ts = [h["timestamp"] for h in edge["history"]["list"]]
        # t=0 from the seed addEdge plus two t=100 updates.
        assert edge_ts == [0, 100, 100]
        ts, values = _temporal_history(edge["properties"]["temporal"]["get"])
        assert ts == [100, 100]
        assert values == [1, 2]


# ----- Batch addNodes / addEdges --------------------------------------------


def test_add_nodes_batch_same_timestamp_appends():
    """A single batch `addNodes` with three updates at the same ms on the
    same node should produce three history entries."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                addNodes(nodes: [
                  { name: "n", updates: [
                    { time: 100, properties: [{key: "v", value: {i64: 1}}] }
                    { time: 100, properties: [{key: "v", value: {i64: 2}}] }
                    { time: 100, properties: [{key: "v", value: {i64: 3}}] }
                  ] }
                ])
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                node(name: "n") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "v") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        node = result["graph"]["node"]
        node_ts = [h["timestamp"] for h in node["history"]["list"]]
        assert node_ts == [100, 100, 100]
        ts, values = _temporal_history(node["properties"]["temporal"]["get"])
        assert ts == [100, 100, 100]
        assert values == [1, 2, 3]


def test_add_edges_batch_same_timestamp_appends():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                addEdges(edges: [
                  { src: "a", dst: "b", updates: [
                    { time: 100, properties: [{key: "w", value: {i64: 1}}] }
                    { time: 100, properties: [{key: "w", value: {i64: 2}}] }
                    { time: 100, properties: [{key: "w", value: {i64: 3}}] }
                  ] }
                ])
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                edge(src: "a", dst: "b") {
                    history { list { timestamp } }
                    properties { temporal { get(key: "w") {
                        history { list { timestamp } }
                        values
                    } } }
                }
            } }
            """,
        )
        edge = result["graph"]["edge"]
        edge_ts = [h["timestamp"] for h in edge["history"]["list"]]
        assert edge_ts == [100, 100, 100]
        ts, values = _temporal_history(edge["properties"]["temporal"]["get"])
        assert ts == [100, 100, 100]
        assert values == [1, 2, 3]


# ----- Persistent graph: deleteEdge -----------------------------------------


def test_delete_edge_same_timestamp_appends():
    """Multiple `deleteEdge` calls at the same ms on a persistent graph all
    land in the deletion history with the same timestamp."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "PERSISTENT")
        client.query(
            """
            {
              updateGraph(path: "g") {
                a: addEdge(time: 1, src: "a", dst: "b") { success }
                d1: deleteEdge(time: 100, src: "a", dst: "b") { success }
                d2: deleteEdge(time: 100, src: "a", dst: "b") { success }
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") {
                edge(src: "a", dst: "b") { deletions { list { timestamp } } }
            } }
            """,
        )
        timestamps = [
            d["timestamp"]
            for d in result["graph"]["edge"]["deletions"]["list"]
        ]
        assert timestamps == [100, 100]


# ----- Object form pins event_id explicitly ---------------------------------


def test_object_time_input_distinct_event_ids_append():
    """Two writes at the same timestamp with distinct explicit event_ids
    both land — the user-provided event_ids partition the events."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start(PORT) as server:
        _new_event_graph(server)
        server.get_client().query(
            """
            {
              updateGraph(path: "g") {
                a: addProperties(t: {timestamp: 100, eventId: 0}, properties: [{key: "x", value: {i64: 1}}])
                b: addProperties(t: {timestamp: 100, eventId: 1}, properties: [{key: "x", value: {i64: 2}}])
              }
            }
            """
        )
        result = _query(
            server,
            """
            { graph(path: "g") { properties { temporal { get(key: "x") {
                history { list { timestamp } }
                values
            } } } } }
            """,
        )
        ts, values = _temporal_history(
            result["graph"]["properties"]["temporal"]["get"]
        )
        assert ts == [100, 100]
        assert values == [1, 2]
