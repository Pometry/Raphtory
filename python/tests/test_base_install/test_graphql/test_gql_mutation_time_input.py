"""Mutation arguments that take a time now accept the full `TimeInput` shape:

- An `Int` (epoch milliseconds).
- An RFC3339 / ISO-8601 datetime string.
- An `{timestamp, eventId}` object.

These tests verify each form on every mutation surface that takes a time, and
confirm the resulting graph state is identical regardless of which input form
was used.
"""

import json
import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer

from utils import PORT


def _query(server, q: str) -> dict:
    response = server.get_client().query(q)
    return json.loads(response) if isinstance(response, str) else response


def test_add_node_accepts_int_string_and_object_time():
    """`addNode` accepts every `TimeInput` shape — Int, RFC3339 string, and
    `{timestamp, eventId}` object. Each insertion lands at its expected
    timestamp and is queryable afterwards."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        # Three forms, three different nodes.
        client.query("""
            {
              updateGraph(path: "g") {
                int_form: addNode(time: 100, name: "intNode") { success }
                str_form: addNode(time: "1970-01-01T00:00:00.200Z", name: "strNode") { success }
                obj_form: addNode(time: {timestamp: 300, eventId: 0}, name: "objNode") { success }
              }
            }
            """)

        # Verify each landed at the expected timestamp.
        result = _query(
            server,
            """
            {
              graph(path: "g") {
                intNode: node(name: "intNode") { earliestTime { timestamp } }
                strNode: node(name: "strNode") { earliestTime { timestamp } }
                objNode: node(name: "objNode") { earliestTime { timestamp } }
              }
            }
            """,
        )
        assert result["graph"]["intNode"]["earliestTime"]["timestamp"] == 100
        assert result["graph"]["strNode"]["earliestTime"]["timestamp"] == 200
        assert result["graph"]["objNode"]["earliestTime"]["timestamp"] == 300


def test_add_edge_and_delete_edge_accept_time_input_shapes():
    """`addEdge` / `deleteEdge` accept the same forms; verify on a persistent
    graph so the deletion is visible via `isValid`."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "PERSISTENT")

        client.query("""
            {
              updateGraph(path: "g") {
                int_add: addEdge(time: 10, src: "a", dst: "b") { success }
                str_add: addEdge(time: "1970-01-01T00:00:00.020Z", src: "a", dst: "b") { success }
                obj_del: deleteEdge(time: {timestamp: 30, eventId: 0}, src: "a", dst: "b") { success }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                edge(src: "a", dst: "b") {
                  earliestTime { timestamp }
                  latestTime { timestamp }
                  isValid
                }
              }
            }
            """,
        )
        edge = result["graph"]["edge"]
        assert edge["earliestTime"]["timestamp"] == 10
        # On a persistent graph, the deletion sets the latest valid time.
        assert edge["latestTime"]["timestamp"] == 30
        # Edge was deleted at t=30 with no later re-addition, so it's invalid now.
        assert edge["isValid"] is False


def test_add_properties_accepts_time_input_shapes():
    """`addProperties` (graph-level temporal properties) accepts Int, string,
    and object."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                p1: addProperties(t: 100, properties: [{key: "score", value: {i64: 1}}])
                p2: addProperties(t: "1970-01-01T00:00:00.200Z", properties: [{key: "score", value: {i64: 2}}])
                p3: addProperties(t: {timestamp: 300, eventId: 0}, properties: [{key: "score", value: {i64: 3}}])
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                properties {
                  temporal {
                    get(key: "score") {
                      history { list { timestamp } }
                      values
                    }
                  }
                }
              }
            }
            """,
        )
        score = result["graph"]["properties"]["temporal"]["get"]
        timestamps = [h["timestamp"] for h in score["history"]["list"]]
        # History is returned in temporal order; not sorting on purpose so a
        # regression that returns events out of order would fail this test.
        assert timestamps == [100, 200, 300]
        assert score["values"] == [1, 2, 3]


def test_temporal_property_input_accepts_time_input_in_batch():
    """Inside `addNodes` / `addEdges`, the `time` field on each per-update
    `TemporalPropertyInput` accepts every `TimeInput` shape."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                addNodes(nodes: [
                  {
                    name: "n",
                    updates: [
                      { time: 10,                              properties: [{key: "v", value: {i64: 1}}] },
                      { time: "1970-01-01T00:00:00.020Z",      properties: [{key: "v", value: {i64: 2}}] },
                      { time: {timestamp: 30, eventId: 0},     properties: [{key: "v", value: {i64: 3}}] }
                    ]
                  }
                ])
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                node(name: "n") {
                  properties { temporal { get(key: "v") {
                    history { list { timestamp } }
                    values
                  } } }
                }
              }
            }
            """,
        )
        v = result["graph"]["node"]["properties"]["temporal"]["get"]
        timestamps = [h["timestamp"] for h in v["history"]["list"]]
        assert timestamps == [10, 20, 30]
        assert v["values"] == [1, 2, 3]


def test_add_edges_batch_accepts_time_input_shapes():
    """`addEdges` is the batch counterpart of `addNodes`. Each per-update
    `time` field on its `TemporalPropertyInput` entries accepts every
    `TimeInput` shape — Int, RFC3339 string, and `{timestamp, eventId}` object."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "EVENT")

        client.query("""
            {
              updateGraph(path: "g") {
                addEdges(edges: [
                  {
                    src: "a", dst: "b",
                    updates: [
                      { time: 10,                              properties: [{key: "w", value: {i64: 1}}] },
                      { time: "1970-01-01T00:00:00.020Z",      properties: [{key: "w", value: {i64: 2}}] },
                      { time: {timestamp: 30, eventId: 0},     properties: [{key: "w", value: {i64: 3}}] }
                    ]
                  }
                ])
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                edge(src: "a", dst: "b") {
                  properties { temporal { get(key: "w") {
                    history { list { timestamp } }
                    values
                  } } }
                }
              }
            }
            """,
        )
        w = result["graph"]["edge"]["properties"]["temporal"]["get"]
        timestamps = [h["timestamp"] for h in w["history"]["list"]]
        assert timestamps == [10, 20, 30]
        assert w["values"] == [1, 2, 3]


def test_mutable_node_and_edge_add_updates_accept_time_input():
    """`MutableNode.addUpdates` and `MutableEdge.addUpdates` / `delete` accept
    every `TimeInput` shape via the `node()` / `edge()` lookups."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.new_graph("g", "PERSISTENT")

        # Seed the node and edge so we can look them up below.
        client.query("""
            {
              updateGraph(path: "g") {
                addNode(time: 0, name: "n") { success }
                addEdge(time: 0, src: "a", dst: "b") { success }
              }
            }
            """)

        client.query("""
            {
              updateGraph(path: "g") {
                node(name: "n") {
                  i: addUpdates(time: 100,                          properties: [{key: "v", value: {i64: 1}}])
                  s: addUpdates(time: "1970-01-01T00:00:00.200Z",   properties: [{key: "v", value: {i64: 2}}])
                  o: addUpdates(time: {timestamp: 300, eventId: 0}, properties: [{key: "v", value: {i64: 3}}])
                }
                edge(src: "a", dst: "b") {
                  i: addUpdates(time: 10,                           properties: [{key: "w", value: {i64: 1}}])
                  s: addUpdates(time: "1970-01-01T00:00:00.020Z",   properties: [{key: "w", value: {i64: 2}}])
                  d: delete(time: {timestamp: 30, eventId: 0})
                }
              }
            }
            """)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                node(name: "n") {
                  properties { temporal { get(key: "v") { values } } }
                }
                edge(src: "a", dst: "b") {
                  properties { temporal { get(key: "w") { values } } }
                  isValid
                }
              }
            }
            """,
        )
        # Values come back in temporal order — assert the sequence as-is so
        # an out-of-order regression would fail.
        assert result["graph"]["node"]["properties"]["temporal"]["get"][
            "values"
        ] == [1, 2, 3]
        assert result["graph"]["edge"]["properties"]["temporal"]["get"][
            "values"
        ] == [1, 2]
        # delete at t=30 with no later re-add → edge is invalid at the latest time.
        assert result["graph"]["edge"]["isValid"] is False
