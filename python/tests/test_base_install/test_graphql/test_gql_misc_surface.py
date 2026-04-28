"""Tests for smaller GraphQL surface fields that previously had no coverage:

- `Nodes.ids`
- `Edges.explode`, `Edges.explodeLayers`
- `PathFromNode.ids`
- `History.isEmpty`
- `TemporalProperty.orderedDedupe`
- `MetaGraph.nodeCount`, `MetaGraph.edgeCount`, `QueryRoot.graphMetadata`
"""

import json
import tempfile

from utils import PORT, run_group_graphql_test
from raphtory import Graph
from raphtory.graphql import GraphServer


def create_graph() -> Graph:
    graph = Graph()
    graph.add_node(10, "A", node_type="person")
    graph.add_node(10, "B", node_type="person")
    graph.add_node(15, "C", node_type="org")
    graph.add_node(40, "D", node_type="org")

    graph.add_edge(10, "A", "B", properties={"weight": 1.0}, layer="layer1")
    graph.add_edge(20, "A", "B", properties={"weight": 2.0}, layer="layer1")
    graph.add_edge(30, "A", "B", properties={"weight": 3.0}, layer="layer2")
    graph.add_edge(15, "A", "C", layer="layer1")
    graph.add_edge(25, "A", "C", layer="layer2")
    graph.add_edge(40, "C", "D", layer="layer1")
    graph.add_edge(50, "B", "A", layer="layer2")
    graph.add_edge(25, "A", "A", layer="layer1")

    return graph


def test_nodes_ids():
    """`nodes.ids` on base / window / layer views. Order isn't guaranteed
    by the resolver, so each result is compared as a set."""
    graph = create_graph()

    cases = [
        # base: all 4 nodes present.
        (
            """{ graph(path: "g") { nodes { ids } } }""",
            ("graph", "nodes"),
            {"A", "B", "C", "D"},
        ),
        # window [10, 25): D (added at 40) excluded.
        (
            """{ graph(path: "g") { window(start: 10, end: 25) { nodes { ids } } } }""",
            ("graph", "window", "nodes"),
            {"A", "B", "C"},
        ),
        # layer(layer2): D only has a layer1 edge (C->D) but is still present
        # because base-layer (non-layered) node events are always included in
        # any layer view — D was added via `add_node(40, "D", ...)` with no
        # layer arg.
        (
            """{ graph(path: "g") { layer(name: "layer2") { nodes { ids } } } }""",
            ("graph", "layer", "nodes"),
            {"A", "B", "C", "D"},
        ),
    ]

    tmp = tempfile.mkdtemp()
    with GraphServer(tmp, create_index=True).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)
        for query, path, expected in cases:
            response = client.query(query)
            data = json.loads(response) if isinstance(response, str) else response
            for key in path:
                data = data[key]
            assert set(data["ids"]) == expected, f"query: {query}"


def test_edges_explode_and_explode_layers():
    """`edges.explode` / `explodeLayers` — collection-level explosion."""
    graph = create_graph()
    queries_and_expected = []

    # Restrict to layer(layer1) + window [10, 25) so the output stays small:
    # edges in scope: A->B@10, A->B@20, A->C@15. 3 explode events.
    # explodeLayers: one per (edge, layer) pair => 2 entries (A->B on layer1, A->C on layer1).
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          window(start: 10, end: 25) {
            edges {
              explode { list { src { name } dst { name } time { timestamp } layerName } }
              explodeLayers { list { src { name } dst { name } layerName } }
            }
          }
        }
      }
    }
    """
    expected = {
        "graph": {
            "layer": {
                "window": {
                    "edges": {
                        "explode": {
                            "list": [
                                {
                                    "src": {"name": "A"},
                                    "dst": {"name": "B"},
                                    "time": {"timestamp": 10},
                                    "layerName": "layer1",
                                },
                                {
                                    "src": {"name": "A"},
                                    "dst": {"name": "B"},
                                    "time": {"timestamp": 20},
                                    "layerName": "layer1",
                                },
                                {
                                    "src": {"name": "A"},
                                    "dst": {"name": "C"},
                                    "time": {"timestamp": 15},
                                    "layerName": "layer1",
                                },
                            ]
                        },
                        "explodeLayers": {
                            "list": [
                                {
                                    "src": {"name": "A"},
                                    "dst": {"name": "B"},
                                    "layerName": "layer1",
                                },
                                {
                                    "src": {"name": "A"},
                                    "dst": {"name": "C"},
                                    "layerName": "layer1",
                                },
                            ]
                        },
                    }
                }
            }
        }
    }
    queries_and_expected.append((query, expected))

    run_group_graphql_test(queries_and_expected, graph, sort_output=True)


def test_path_from_node_ids():
    """`pathFromNode.ids` via `neighbours` / `inNeighbours` / `outNeighbours`."""
    graph = create_graph()
    queries_and_expected = []

    # A's neighbours (undirected): B, C, A (self-loop).
    # A's outNeighbours: B, C, A
    # A's inNeighbours: A, B
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          neighbours { ids }
          outNeighbours { ids }
          inNeighbours { ids }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "node": {
                        "neighbours": {"ids": ["A", "B", "C"]},
                        "outNeighbours": {"ids": ["A", "B", "C"]},
                        "inNeighbours": {"ids": ["A", "B"]},
                    }
                }
            },
        )
    )

    # layer(layer2) changes A's neighbourhood: A->B, A->C, B->A are the only
    # layer2 edges touching A => neighbours={B, C}, outNeighbours={B, C}, inNeighbours={B}.
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          node(name: "A") {
            neighbours { ids }
            outNeighbours { ids }
            inNeighbours { ids }
          }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "layer": {
                        "node": {
                            "neighbours": {"ids": ["B", "C"]},
                            "outNeighbours": {"ids": ["B", "C"]},
                            "inNeighbours": {"ids": ["B"]},
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph, sort_output=True)


def test_history_is_empty():
    """`history.isEmpty` is true on an empty window, false otherwise."""
    graph = create_graph()
    queries_and_expected = []

    # A has history (created at t=10)
    query = """{ graph(path: "g") { node(name: "A") { history { isEmpty } } } }"""
    queries_and_expected.append(
        (query, {"graph": {"node": {"history": {"isEmpty": False}}}})
    )

    # Windowing the node (not the graph) keeps the node reachable but empties
    # its history => isEmpty = True.
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          window(start: 0, end: 5) { history { isEmpty } }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {"graph": {"node": {"window": {"history": {"isEmpty": True}}}}},
        )
    )

    # Same trick for an edge: pick a window with no A->B updates (the only
    # A->B updates are at t=10, 20, 30).
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          window(start: 40, end: 45) { history { isEmpty } }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {"graph": {"edge": {"window": {"history": {"isEmpty": True}}}}},
        )
    )

    # Edge A->B's full history is non-empty
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") { history { isEmpty } }
      }
    }
    """
    queries_and_expected.append(
        (query, {"graph": {"edge": {"history": {"isEmpty": False}}}})
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_temporal_property_ordered_dedupe():
    """`TemporalProperty.orderedDedupe` — collapses consecutive-equal updates."""
    g = Graph()
    # state timeline: a(1), a(2), b(3), a(4), a(5)
    # latestTime=True  => keeps the latest timestamp of each run: (2,'a'), (3,'b'), (5,'a')
    # latestTime=False => keeps the first timestamp of each run:   (1,'a'), (3,'b'), (4,'a')
    g.add_node(1, "X", properties={"state": "a"})
    g.add_node(2, "X", properties={"state": "a"})
    g.add_node(3, "X", properties={"state": "b"})
    g.add_node(4, "X", properties={"state": "a"})
    g.add_node(5, "X", properties={"state": "a"})

    queries_and_expected = []

    query = """
    {
      graph(path: "g") {
        node(name: "X") {
          properties {
            temporal {
              get(key: "state") {
                latest:  orderedDedupe(latestTime: true)  { time { timestamp } value }
                first:   orderedDedupe(latestTime: false) { time { timestamp } value }
              }
            }
          }
        }
      }
    }
    """
    expected = {
        "graph": {
            "node": {
                "properties": {
                    "temporal": {
                        "get": {
                            "latest": [
                                {"time": {"timestamp": 2}, "value": "a"},
                                {"time": {"timestamp": 3}, "value": "b"},
                                {"time": {"timestamp": 5}, "value": "a"},
                            ],
                            "first": [
                                {"time": {"timestamp": 1}, "value": "a"},
                                {"time": {"timestamp": 3}, "value": "b"},
                                {"time": {"timestamp": 4}, "value": "a"},
                            ],
                        }
                    }
                }
            }
        }
    }
    queries_and_expected.append((query, expected))

    run_group_graphql_test(queries_and_expected, g)


def test_meta_graph_counts():
    """`graphMetadata` → `MetaGraph.nodeCount` / `edgeCount` report persisted
    counts for a stored graph without loading it."""
    graph = create_graph()
    queries_and_expected = []

    query = """{ graphMetadata(path: "g") { nodeCount edgeCount name path } }"""
    queries_and_expected.append(
        (
            query,
            {
                "graphMetadata": {
                    "nodeCount": 4,
                    "edgeCount": 5,
                    "name": "g",
                    "path": "g",
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)
