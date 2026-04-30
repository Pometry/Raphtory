"""Tests for `Graph`-level fields that previously had no GraphQL coverage:

- `countEdges`, `countNodes`, `countTemporalEdges`
- `hasNode`, `hasEdge` (+ optional `layer:` arg on `hasEdge`)
- `earliestEdgeTime`, `latestEdgeTime`
- `uniqueLayers`
- `sharedNeighbours`

Each field is tested under a combination of base / window / layer views to
exercise the composition plumbing, not just the field itself.

`searchEdges` is left untested here — it's marked experimental in the schema
and requires an index-creation path that these fixtures don't exercise.
"""

from utils import run_group_graphql_test
from raphtory import Graph


def create_graph() -> Graph:
    graph = Graph()

    # Nodes with a node_type so `typeFilter` paths could be reused
    graph.add_node(10, "A", node_type="person")
    graph.add_node(10, "B", node_type="person")
    graph.add_node(15, "C", node_type="org")
    graph.add_node(40, "D", node_type="org")

    # Edges across two layers, including a self-loop and a reverse edge
    graph.add_edge(10, "A", "B", properties={"weight": 1.0}, layer="layer1")
    graph.add_edge(20, "A", "B", properties={"weight": 2.0}, layer="layer1")
    graph.add_edge(30, "A", "B", properties={"weight": 3.0}, layer="layer2")
    graph.add_edge(15, "A", "C", layer="layer1")
    graph.add_edge(25, "A", "C", layer="layer2")
    graph.add_edge(40, "C", "D", layer="layer1")
    graph.add_edge(50, "B", "A", layer="layer2")
    graph.add_edge(25, "A", "A", layer="layer1")  # self-loop

    return graph


def test_graph_counts():
    """`countNodes`, `countEdges`, `countTemporalEdges` under base / window /
    layer / layer+window views."""
    graph = create_graph()
    queries_and_expected = []

    # base: 4 nodes, 5 unique edges (AB, AC, CD, BA, AA), 8 temporal edge events
    query = """
    {
      graph(path: "g") {
        countNodes
        countEdges
        countTemporalEdges
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "countNodes": 4,
                    "countEdges": 5,
                    "countTemporalEdges": 8,
                }
            },
        )
    )

    # windowed [10, 30): drops t=30, 40, 50, keeps t=10..25.
    # nodes visible: A, B, C (D not yet). edges: AB@layer1, AC@layer1, AC@layer2, AA@layer1 => 3 unique edges (AB, AC, AA).
    # temporal edges in window: AB@10, AB@20, AC@15, AC@25, AA@25 => 5
    query = """
    {
      graph(path: "g") {
        window(start: 10, end: 30) {
          countNodes
          countEdges
          countTemporalEdges
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "window": {
                        "countNodes": 3,
                        "countEdges": 3,
                        "countTemporalEdges": 5,
                    }
                }
            },
        )
    )

    # layer(layer1) only: edges AB (2 updates), AC, CD, AA => 4 unique, 5 temporal
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          countNodes
          countEdges
          countTemporalEdges
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
                        "countNodes": 4,
                        "countEdges": 4,
                        "countTemporalEdges": 5,
                    }
                }
            },
        )
    )

    # layer(layer1) + window [10, 30): AB@10, AB@20, AC@15, AA@25 => 3 unique, 4 temporal
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          window(start: 10, end: 30) {
            countNodes
            countEdges
            countTemporalEdges
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
                        "window": {
                            "countNodes": 3,
                            "countEdges": 3,
                            "countTemporalEdges": 4,
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_has_node_and_has_edge():
    """`hasNode` / `hasEdge` under base / window / layer views.

    `hasEdge` also accepts an optional `layer:` arg.
    """
    graph = create_graph()
    queries_and_expected = []

    # base: all exist
    query = """
    {
      graph(path: "g") {
        hasA: hasNode(name: "A")
        hasZ: hasNode(name: "Z")
        edgeAB: hasEdge(src: "A", dst: "B")
        edgeBA: hasEdge(src: "B", dst: "A")
        edgeAD: hasEdge(src: "A", dst: "D")
        edgeAB_layer1: hasEdge(src: "A", dst: "B", layer: "layer1")
        edgeAB_layer2: hasEdge(src: "A", dst: "B", layer: "layer2")
        edgeBA_layer1: hasEdge(src: "B", dst: "A", layer: "layer1")
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "hasA": True,
                    "hasZ": False,
                    "edgeAB": True,
                    "edgeBA": True,
                    "edgeAD": False,
                    "edgeAB_layer1": True,
                    "edgeAB_layer2": True,
                    "edgeBA_layer1": False,  # B->A only exists on layer2
                }
            },
        )
    )

    # windowed [10, 30): D not yet present, edge CD not yet either
    query = """
    {
      graph(path: "g") {
        window(start: 10, end: 30) {
          hasD: hasNode(name: "D")
          edgeCD: hasEdge(src: "C", dst: "D")
          edgeAB: hasEdge(src: "A", dst: "B")
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "window": {
                        "hasD": False,
                        "edgeCD": False,
                        "edgeAB": True,
                    }
                }
            },
        )
    )

    # layer(layer2): edge AB exists, BA exists on layer2, CD doesn't (layer1 only)
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          edgeAB: hasEdge(src: "A", dst: "B")
          edgeBA: hasEdge(src: "B", dst: "A")
          edgeCD: hasEdge(src: "C", dst: "D")
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
                        "edgeAB": True,
                        "edgeBA": True,
                        "edgeCD": False,
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_edge_time_bounds_and_unique_layers():
    """`earliestEdgeTime`, `latestEdgeTime`, `uniqueLayers` base + window + layer."""
    graph = create_graph()
    queries_and_expected = []

    # base: edges from t=10 to t=50; layers layer1 + layer2
    query = """
    {
      graph(path: "g") {
        earliestEdgeTime { timestamp }
        latestEdgeTime { timestamp }
        uniqueLayers
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "earliestEdgeTime": {"timestamp": 10},
                    "latestEdgeTime": {"timestamp": 50},
                    "uniqueLayers": ["layer1", "layer2"],
                }
            },
        )
    )

    # windowed: edges from t=15 to t=25 only
    query = """
    {
      graph(path: "g") {
        window(start: 15, end: 30) {
          earliestEdgeTime { timestamp }
          latestEdgeTime { timestamp }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "window": {
                        "earliestEdgeTime": {"timestamp": 15},
                        "latestEdgeTime": {"timestamp": 25},
                    }
                }
            },
        )
    )

    # layer(layer2): edges at t=25, 30, 50
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          earliestEdgeTime { timestamp }
          latestEdgeTime { timestamp }
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
                        "earliestEdgeTime": {"timestamp": 25},
                        "latestEdgeTime": {"timestamp": 50},
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_shared_neighbours():
    """`sharedNeighbours` returns the intersection of neighbour sets."""
    graph = create_graph()
    queries_and_expected = []

    # A's neighbours (undirected): {A (self-loop), B, C}
    # B's neighbours: {A}  -> shared(A,B) = {A}
    # C's neighbours: {A, D} -> shared(A,C) = {A}, shared(B,C) = {}
    query = """
    {
      graph(path: "g") {
        AB: sharedNeighbours(selectedNodes: ["A", "B"]) { name }
        AC: sharedNeighbours(selectedNodes: ["A", "C"]) { name }
        BC: sharedNeighbours(selectedNodes: ["B", "C"]) { name }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "AB": [{"name": "A"}],
                    "AC": [{"name": "A"}],
                    "BC": [{"name": "A"}],
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph, sort_output=True)
