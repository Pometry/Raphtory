from utils import run_group_graphql_test
from raphtory import Graph


def create_graph() -> Graph:
    graph = Graph()

    # Bare node event so "A" exists at t=50 before any score updates
    graph.add_node(50, "A")
    # Node "A" with a numeric temporal property "score" at 4 timestamps
    graph.add_node(100, "A", properties={"score": 10})
    graph.add_node(200, "A", properties={"score": 20})
    graph.add_node(300, "A", properties={"score": 30})
    graph.add_node(400, "A", properties={"score": 40})

    # Edge "A -> B" with "weight" on two layers
    graph.add_edge(100, "A", "B", properties={"weight": 1.0}, layer="layer1")
    graph.add_edge(200, "A", "B", properties={"weight": 2.0}, layer="layer1")
    graph.add_edge(300, "A", "B", properties={"weight": 3.0}, layer="layer2")
    graph.add_edge(400, "A", "B", properties={"weight": 4.0}, layer="layer2")

    return graph


def test_node_temporal_aggregates():
    graph = create_graph()
    queries_and_expected_outputs = []

    # full timeline: score = [10, 20, 30, 40]
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          properties {
            temporal {
              get(key: "score") {
                sum
                mean
                average
                count
                min { time { timestamp } value }
                max { time { timestamp } value }
                median { time { timestamp } value }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "node": {
                "properties": {
                    "temporal": {
                        "get": {
                            "sum": 100,
                            "mean": 25.0,
                            "average": 25.0,
                            "count": 4,
                            "min": {"time": {"timestamp": 100}, "value": 10},
                            "max": {"time": {"timestamp": 400}, "value": 40},
                            # lower median on even-length input: sorted[(4-1)/2] = index 1
                            "median": {"time": {"timestamp": 200}, "value": 20},
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # windowed [150, 350): score = [20, 30]
    query = """
    {
      graph(path: "g") {
        window(start: 150, end: 350) {
          node(name: "A") {
            properties {
              temporal {
                get(key: "score") {
                  sum
                  mean
                  count
                  min { time { timestamp } value }
                  max { time { timestamp } value }
                  median { time { timestamp } value }
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "window": {
                "node": {
                    "properties": {
                        "temporal": {
                            "get": {
                                "sum": 50,
                                "mean": 25.0,
                                "count": 2,
                                "min": {"time": {"timestamp": 200}, "value": 20},
                                "max": {"time": {"timestamp": 300}, "value": 30},
                                # lower median of [20, 30] => index 0
                                "median": {"time": {"timestamp": 200}, "value": 20},
                            }
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # window with no updates in range: score property doesn't exist => get is null
    query = """
    {
      graph(path: "g") {
        window(start: 40, end: 60) {
          node(name: "A") {
            properties {
              temporal {
                get(key: "score") {
                  sum
                  mean
                  count
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "window": {
                "node": {
                    "properties": {
                        "temporal": {
                            "get": {"sum": None, "mean": None, "count": 0}
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, graph)


def create_non_numeric_graph() -> Graph:
    graph = Graph()
    # string-valued temporal property (insertion order != sorted order, so
    # min/max/median land on interior timestamps)
    graph.add_node(1, "A", properties={"name": "cherry"})
    graph.add_node(2, "A", properties={"name": "apple"})
    graph.add_node(3, "A", properties={"name": "banana"})
    graph.add_node(4, "A", properties={"name": "date"})
    # bool-valued temporal property, mixed so True/False appear multiple times
    graph.add_node(1, "A", properties={"flag": True})
    graph.add_node(2, "A", properties={"flag": False})
    graph.add_node(3, "A", properties={"flag": True})
    graph.add_node(4, "A", properties={"flag": False})
    graph.add_node(5, "A", properties={"flag": True})
    return graph


def test_temporal_aggregates_on_non_numeric():
    """Pin down semantics for aggregates on non-numeric temporal properties.

    - Strings: `sum` concatenates (strings are additive), `min/max/median` work
      lexicographically, `mean` is null (not f64-convertible).
    - Bools: `sum`/`mean` are null (not additive, not f64-convertible), but
      `min/max/median` work (False < True).
    """
    graph = create_non_numeric_graph()
    queries_and_expected_outputs = []

    # strings
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          properties {
            temporal {
              get(key: "name") {
                sum
                mean
                average
                count
                min { time { timestamp } value }
                max { time { timestamp } value }
                median { time { timestamp } value }
              }
            }
          }
        }
      }
    }
    """
    # timeline: cherry(t=1), apple(t=2), banana(t=3), date(t=4)
    # sorted lex:        apple(t=2) < banana(t=3) < cherry(t=1) < date(t=4)
    expected_output = {
        "graph": {
            "node": {
                "properties": {
                    "temporal": {
                        "get": {
                            # concatenation in insertion order
                            "sum": "cherryapplebananadate",
                            "mean": None,
                            "average": None,
                            "count": 4,
                            "min": {"time": {"timestamp": 2}, "value": "apple"},
                            "max": {"time": {"timestamp": 4}, "value": "date"},
                            # lower median of len=4 is sorted[(4-1)/2] = index 1
                            "median": {"time": {"timestamp": 3}, "value": "banana"},
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # bools
    query = """
    {
      graph(path: "g") {
        node(name: "A") {
          properties {
            temporal {
              get(key: "flag") {
                sum
                mean
                average
                count
                min { time { timestamp } value }
                max { time { timestamp } value }
                median { time { timestamp } value }
              }
            }
          }
        }
      }
    }
    """
    # timeline: True(t=1), False(t=2), True(t=3), False(t=4), True(t=5)
    # min fold keeps first smaller value encountered => first False at t=2
    # max fold keeps first larger-or-equal value encountered => True at t=1
    # median: stable sort by value gives [False@2, False@4, True@1, True@3, True@5]
    # lower median of len=5 => sorted[(5-1)/2] = index 2 = True at t=1
    expected_output = {
        "graph": {
            "node": {
                "properties": {
                    "temporal": {
                        "get": {
                            "sum": None,
                            "mean": None,
                            "average": None,
                            "count": 5,
                            "min": {"time": {"timestamp": 2}, "value": False},
                            "max": {"time": {"timestamp": 1}, "value": True},
                            "median": {"time": {"timestamp": 1}, "value": True},
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, graph)


def test_edge_temporal_aggregates_across_layers():
    graph = create_graph()
    queries_and_expected_outputs = []

    # full (both layers): weight = [1.0, 2.0, 3.0, 4.0]
    query = """
    {
      graph(path: "g") {
        edge(src: "A", dst: "B") {
          properties {
            temporal {
              get(key: "weight") {
                sum
                mean
                count
                min { time { timestamp } value }
                max { time { timestamp } value }
                median { time { timestamp } value }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "properties": {
                    "temporal": {
                        "get": {
                            "sum": 10.0,
                            "mean": 2.5,
                            "count": 4,
                            "min": {"time": {"timestamp": 100}, "value": 1.0},
                            "max": {"time": {"timestamp": 400}, "value": 4.0},
                            "median": {"time": {"timestamp": 200}, "value": 2.0},
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # layer1 only: weight = [1.0, 2.0]
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          edge(src: "A", dst: "B") {
            properties {
              temporal {
                get(key: "weight") {
                  sum
                  mean
                  count
                  min { time { timestamp } value }
                  max { time { timestamp } value }
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "layer": {
                "edge": {
                    "properties": {
                        "temporal": {
                            "get": {
                                "sum": 3.0,
                                "mean": 1.5,
                                "count": 2,
                                "min": {"time": {"timestamp": 100}, "value": 1.0},
                                "max": {"time": {"timestamp": 200}, "value": 2.0},
                            }
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # layer2 only: weight = [3.0, 4.0]
    query = """
    {
      graph(path: "g") {
        layer(name: "layer2") {
          edge(src: "A", dst: "B") {
            properties {
              temporal {
                get(key: "weight") {
                  sum
                  mean
                  count
                  min { time { timestamp } value }
                  max { time { timestamp } value }
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "layer": {
                "edge": {
                    "properties": {
                        "temporal": {
                            "get": {
                                "sum": 7.0,
                                "mean": 3.5,
                                "count": 2,
                                "min": {"time": {"timestamp": 300}, "value": 3.0},
                                "max": {"time": {"timestamp": 400}, "value": 4.0},
                            }
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # combined: windowed + layer-filtered (layer1, window [150, 400))
    # => weight = [2.0]
    query = """
    {
      graph(path: "g") {
        layer(name: "layer1") {
          window(start: 150, end: 400) {
            edge(src: "A", dst: "B") {
              properties {
                temporal {
                  get(key: "weight") {
                    sum
                    mean
                    count
                    min { time { timestamp } value }
                    max { time { timestamp } value }
                    median { time { timestamp } value }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "layer": {
                "window": {
                    "edge": {
                        "properties": {
                            "temporal": {
                                "get": {
                                    "sum": 2.0,
                                    "mean": 2.0,
                                    "count": 1,
                                    "min": {"time": {"timestamp": 200}, "value": 2.0},
                                    "max": {"time": {"timestamp": 200}, "value": 2.0},
                                    "median": {
                                        "time": {"timestamp": 200},
                                        "value": 2.0,
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, graph)
