"""`graph(path:, graphType:)` accepts an optional `graphType` argument that
re-interprets the stored graph at query time.

- `EVENT` semantics: each update is a point-in-time event; windows only see
  updates whose timestamps fall inside them.
- `PERSISTENT` semantics: values carry forward until overwritten or deleted;
  an edge added at t=1 is visible in a window starting at t=5.

These tests confirm both forms of override work both ways, and that omitting
the argument preserves the graph's native type.
"""

import json
import tempfile

from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer

from utils import PORT


def _query(server, q: str) -> dict:
    response = server.get_client().query(q)
    return json.loads(response) if isinstance(response, str) else response


def test_event_graph_default_uses_event_semantics():
    """Without `graphType`, an event-stored graph keeps event semantics — a
    window after the addition event sees no edge."""
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                window(start: 5, end: 10) {
                  hasEdge(src: "a", dst: "b")
                }
              }
            }
            """,
        )
        assert result["graph"]["window"]["hasEdge"] is False


def test_event_graph_read_as_persistent_carries_value_forward():
    """Reading an event graph through `graphType: PERSISTENT` makes the edge
    visible in a window that starts after the addition event."""
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              graph(path: "g", graphType: PERSISTENT) {
                window(start: 5, end: 10) {
                  hasEdge(src: "a", dst: "b")
                }
              }
            }
            """,
        )
        assert result["graph"]["window"]["hasEdge"] is True


def test_persistent_graph_default_carries_value_forward():
    """Without `graphType`, a persistent-stored graph keeps persistent
    semantics — the edge is alive in a window after the add."""
    work_dir = tempfile.mkdtemp()
    g = PersistentGraph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              graph(path: "g") {
                window(start: 5, end: 10) {
                  hasEdge(src: "a", dst: "b")
                }
              }
            }
            """,
        )
        assert result["graph"]["window"]["hasEdge"] is True


def test_persistent_graph_read_as_event_drops_carried_values():
    """Reading a persistent graph through `graphType: EVENT` makes the edge
    invisible in a window that starts after the addition event."""
    work_dir = tempfile.mkdtemp()
    g = PersistentGraph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              graph(path: "g", graphType: EVENT) {
                window(start: 5, end: 10) {
                  hasEdge(src: "a", dst: "b")
                }
              }
            }
            """,
        )
        assert result["graph"]["window"]["hasEdge"] is False


def test_mutable_event_graph_default_uses_event_semantics():
    """`updateGraph(path).graph` without `graphType` keeps event semantics."""
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              updateGraph(path: "g") {
                graph {
                  window(start: 5, end: 10) { hasEdge(src: "a", dst: "b") }
                }
              }
            }
            """,
        )
        assert result["updateGraph"]["graph"]["window"]["hasEdge"] is False


def test_mutable_event_graph_read_as_persistent_carries_value_forward():
    """`updateGraph(path).graph(graphType: PERSISTENT)` re-interprets an
    event-stored graph through persistent semantics."""
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              updateGraph(path: "g") {
                graph(graphType: PERSISTENT) {
                  window(start: 5, end: 10) { hasEdge(src: "a", dst: "b") }
                }
              }
            }
            """,
        )
        assert result["updateGraph"]["graph"]["window"]["hasEdge"] is True


def test_mutable_persistent_graph_default_carries_value_forward():
    """`updateGraph(path).graph` on a persistent graph keeps persistent
    semantics by default."""
    work_dir = tempfile.mkdtemp()
    g = PersistentGraph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              updateGraph(path: "g") {
                graph {
                  window(start: 5, end: 10) { hasEdge(src: "a", dst: "b") }
                }
              }
            }
            """,
        )
        assert result["updateGraph"]["graph"]["window"]["hasEdge"] is True


def test_mutable_persistent_graph_read_as_event_drops_carried_values():
    """`updateGraph(path).graph(graphType: EVENT)` re-interprets a
    persistent graph through event semantics."""
    work_dir = tempfile.mkdtemp()
    g = PersistentGraph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              updateGraph(path: "g") {
                graph(graphType: EVENT) {
                  window(start: 5, end: 10) { hasEdge(src: "a", dst: "b") }
                }
              }
            }
            """,
        )
        assert result["updateGraph"]["graph"]["window"]["hasEdge"] is False


def test_mutable_graph_reads_pending_mutations_through_override():
    """Mutate via `updateGraph(path)`, then read back via the graph accessor
    with a `graphType` override — both the existing data and the new edge
    should be visible under the chosen semantics."""
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              updateGraph(path: "g") {
                addEdge(time: 2, src: "b", dst: "c") { success }
                asPersistent: graph(graphType: PERSISTENT) {
                  window(start: 5, end: 10) {
                    abEdge: hasEdge(src: "a", dst: "b")
                    bcEdge: hasEdge(src: "b", dst: "c")
                  }
                }
              }
            }
            """,
        )
        win = result["updateGraph"]["asPersistent"]["window"]
        assert win["abEdge"] is True
        assert win["bcEdge"] is True


def test_persistent_deletes_visible_via_persistent_view():
    """A delete event in a persistent graph propagates: a window after the
    deletion shows the edge as gone under persistent semantics."""
    work_dir = tempfile.mkdtemp()
    g = PersistentGraph()
    g.add_edge(1, "a", "b")
    g.delete_edge(5, "a", "b")
    with GraphServer(work_dir).start(PORT) as server:
        server.get_client().send_graph(path="g", graph=g)

        result = _query(
            server,
            """
            {
              before: graph(path: "g") {
                window(start: 2, end: 4) { hasEdge(src: "a", dst: "b") }
              }
              after: graph(path: "g") {
                window(start: 6, end: 10) { hasEdge(src: "a", dst: "b") }
              }
            }
            """,
        )
        assert result["before"]["window"]["hasEdge"] is True
        assert result["after"]["window"]["hasEdge"] is False
