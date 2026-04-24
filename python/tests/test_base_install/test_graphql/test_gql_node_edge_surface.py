"""Tests for `Node` and `Edge` fields that previously had no GraphQL coverage.

Node fields covered:
- `firstUpdate`, `lastUpdate`
- `edgeHistoryCount`
- `inDegree`, `outDegree`
- `inEdges`, `outEdges`

Edge fields covered:
- `firstUpdate`, `lastUpdate`
- `layerNames`
- `layerName` (with error case)
- `explode`, `explodeLayers`
- `isValid`, `isSelfLoop`
- `nbr` on an exploded edge

All tested under base + window + layer composition where applicable.
"""

from utils import run_group_graphql_test, run_graphql_error_test_contains
from raphtory import Graph


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
    graph.add_edge(25, "A", "A", layer="layer1")  # self-loop

    return graph


def test_node_update_times_and_edge_history_count():
    """`firstUpdate`, `lastUpdate`, `edgeHistoryCount` under base / window / layer."""
    graph = create_graph()
    queries_and_expected = []

    # Base: A has events from t=10 (add_node + A->B) to t=50 (B->A).
    # Edge events touching A: A->B @10, @20, @30; A->C @15, @25; A->A @25; B->A @50 => 7.
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          firstUpdate { timestamp }
          lastUpdate { timestamp }
          edgeHistoryCount
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
                        "firstUpdate": {"timestamp": 10},
                        "lastUpdate": {"timestamp": 50},
                        "edgeHistoryCount": 7,
                    }
                }
            },
        )
    )

    # Windowed [15, 40): first event for A is at 15 (A->C), last is at 30 (A->B).
    # Events touching A in window: A->B@20, A->B@30, A->C@15, A->C@25, A->A@25 => 5.
    query = """
    {
      graph(path: "g") {
        window(start: 15, end: 40) {
          node(name: "A") {
            firstUpdate { timestamp }
            lastUpdate { timestamp }
            edgeHistoryCount
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
                    "window": {
                        "node": {
                            "firstUpdate": {"timestamp": 15},
                            "lastUpdate": {"timestamp": 30},
                            "edgeHistoryCount": 5,
                        }
                    }
                }
            },
        )
    )

    # layer(layer2) on A: node events (add_node @ t=10) aren't layer-scoped so
    # firstUpdate still sees t=10. Edge events on layer2 touching A are at 25,
    # 30, 50 => lastUpdate=50, edgeHistoryCount=3.
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          node(name: "A") {
            firstUpdate { timestamp }
            lastUpdate { timestamp }
            edgeHistoryCount
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
                            "firstUpdate": {"timestamp": 10},
                            "lastUpdate": {"timestamp": 50},
                            "edgeHistoryCount": 3,
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_node_directed_degrees_and_edges():
    """`inDegree`, `outDegree`, `inEdges`, `outEdges` under base / window / layer."""
    graph = create_graph()
    queries_and_expected = []

    # Base on A:
    # out-edges: A->B, A->C, A->A   => outDegree=3
    # in-edges:  A->A, B->A         => inDegree=2
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          inDegree
          outDegree
          inEdges  { list { src { name } dst { name } } }
          outEdges { list { src { name } dst { name } } }
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
                        "inDegree": 2,
                        "outDegree": 3,
                        "inEdges": {
                            "list": [
                                {"src": {"name": "A"}, "dst": {"name": "A"}},
                                {"src": {"name": "B"}, "dst": {"name": "A"}},
                            ]
                        },
                        "outEdges": {
                            "list": [
                                {"src": {"name": "A"}, "dst": {"name": "B"}},
                                {"src": {"name": "A"}, "dst": {"name": "C"}},
                                {"src": {"name": "A"}, "dst": {"name": "A"}},
                            ]
                        },
                    }
                }
            },
        )
    )

    # layer(layer2): only A->B@30, A->C@25, B->A@50 touch A.
    #   outEdges(A): A->B, A->C    (no A->A since self-loop is layer1)
    #   inEdges(A):  B->A
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          node(name: "A") {
            inDegree
            outDegree
            inEdges  { list { src { name } dst { name } } }
            outEdges { list { src { name } dst { name } } }
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
                            "inDegree": 1,
                            "outDegree": 2,
                            "inEdges": {
                                "list": [
                                    {"src": {"name": "B"}, "dst": {"name": "A"}},
                                ]
                            },
                            "outEdges": {
                                "list": [
                                    {"src": {"name": "A"}, "dst": {"name": "B"}},
                                    {"src": {"name": "A"}, "dst": {"name": "C"}},
                                ]
                            },
                        }
                    }
                }
            },
        )
    )

    # windowed [10, 30): A->A@25 + A->B@10, A->B@20 + A->C@15 => A has 3 out-edges (to A, B, C), 1 in-edge (A->A)
    query = """
    {
      graph(path: "g") {
        window(start: 10, end: 30) {
          node(name: "A") {
            inDegree
            outDegree
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
                    "window": {
                        "node": {
                            "inDegree": 1,
                            "outDegree": 3,
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph, sort_output=True)


def test_edge_update_times():
    """`firstUpdate` / `lastUpdate` on edges under base / window / layer."""
    graph = create_graph()
    queries_and_expected = []

    # A->B: base updates at 10, 20, 30. first=10, last=30.
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          firstUpdate { timestamp }
          lastUpdate { timestamp }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "edge": {
                        "firstUpdate": {"timestamp": 10},
                        "lastUpdate": {"timestamp": 30},
                    }
                }
            },
        )
    )

    # layer(layer1) on A->B: updates at 10 and 20 only.
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          edge(src: "A", dst: "B") {
            firstUpdate { timestamp }
            lastUpdate { timestamp }
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
                        "edge": {
                            "firstUpdate": {"timestamp": 10},
                            "lastUpdate": {"timestamp": 20},
                        }
                    }
                }
            },
        )
    )

    # window [15, 25) on A->B: only update at 20.
    query = """
    {
      graph(path: "g") {
        window(start: 15, end: 25) {
          edge(src: "A", dst: "B") {
            firstUpdate { timestamp }
            lastUpdate { timestamp }
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
                    "window": {
                        "edge": {
                            "firstUpdate": {"timestamp": 20},
                            "lastUpdate": {"timestamp": 20},
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_edge_layers_and_explode():
    """`layerNames`, `layerName`, `explode`, `explodeLayers`, `isSelfLoop`, `isValid`."""
    graph = create_graph()
    queries_and_expected = []

    # A->B spans layer1 + layer2
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          layerNames
          isSelfLoop
          isValid
          explode { list { src { name } dst { name } time { timestamp } layerName } }
          explodeLayers { list { layerName } }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "edge": {
                        "layerNames": ["layer1", "layer2"],
                        "isSelfLoop": False,
                        "isValid": True,
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
                                    "dst": {"name": "B"},
                                    "time": {"timestamp": 30},
                                    "layerName": "layer2",
                                },
                            ]
                        },
                        "explodeLayers": {
                            "list": [
                                {"layerName": "layer1"},
                                {"layerName": "layer2"},
                            ]
                        },
                    }
                }
            },
        )
    )

    # A->A self-loop (layer1 only)
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "A") {
          isSelfLoop
          layerNames
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "edge": {
                        "isSelfLoop": True,
                        "layerNames": ["layer1"],
                    }
                }
            },
        )
    )

    # `layerName` only works on edges that have been exploded (either fully or
    # per-layer). Verified via explodeLayers on a multi-layer edge.
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          explodeLayers { list { layerName layerNames } }
        }
      }
    }
    """
    queries_and_expected.append(
        (
            query,
            {
                "graph": {
                    "edge": {
                        "explodeLayers": {
                            "list": [
                                {"layerName": "layer1", "layerNames": ["layer1"]},
                                {"layerName": "layer2", "layerNames": ["layer2"]},
                            ]
                        }
                    }
                }
            },
        )
    )

    run_group_graphql_test(queries_and_expected, graph)


def test_edge_layer_name_errors_on_non_exploded_edge():
    """`layerName` errors on any edge that hasn't been exploded — the
    single-layer form is only available after `.explode()` or
    `.explodeLayers()`."""
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          layerName
        }
      }
    }
    """
    run_graphql_error_test_contains(
        query, ["layer_name function is only available", "exploded"], create_graph()
    )


def test_edge_nbr_on_exploded_edge():
    """`nbr` on the exploded form of an out-edge returns `dst`."""
    graph = create_graph()
    queries_and_expected = []

    # Explode A->B and ask for `nbr` on each: each should be B (the other end).
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          outEdges {
            list {
              explode { list { nbr { name } } }
            }
          }
        }
      }
    }
    """
    # A has three out-edges (A->A, A->B, A->C). nbr of each exploded event is
    # the "other" node — for A->A that's A (both ends), for A->B it's B, etc.
    # A->A: 1 exploded event  -> [A]
    # A->B: 3 exploded events -> [B, B, B]
    # A->C: 2 exploded events -> [C, C]
    expected = {
        "graph": {
            "node": {
                "outEdges": {
                    "list": [
                        {
                            "explode": {
                                "list": [{"nbr": {"name": "A"}}],
                            }
                        },
                        {
                            "explode": {
                                "list": [
                                    {"nbr": {"name": "B"}},
                                    {"nbr": {"name": "B"}},
                                    {"nbr": {"name": "B"}},
                                ],
                            }
                        },
                        {
                            "explode": {
                                "list": [
                                    {"nbr": {"name": "C"}},
                                    {"nbr": {"name": "C"}},
                                ],
                            }
                        },
                    ]
                }
            }
        }
    }
    queries_and_expected.append((query, expected))
    run_group_graphql_test(queries_and_expected, graph, sort_output=True)
