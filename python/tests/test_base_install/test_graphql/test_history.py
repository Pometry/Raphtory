from utils import run_group_graphql_test
from raphtory import Graph


def create_graph() -> Graph:
    graph = Graph()

    # Add nodes with timestamps and properties
    graph.add_node(100, "Dumbledore")
    graph.add_node(200, "Dumbledore", properties={"Age": 50})
    graph.add_node(300, "Dumbledore", properties={"Age": 51})

    graph.add_node(150, "Harry")
    graph.add_node(250, "Harry", properties={"Age": 20})
    graph.add_node(350, "Harry", properties={"Age": 21})

    # Add edges with timestamps and layers
    graph.add_edge(150, "Dumbledore", "Harry", layer="communication")
    graph.add_edge(200, "Dumbledore", "Harry", properties={"weight": 0.5}, layer="friendship")
    graph.add_edge(300, "Dumbledore", "Harry", properties={"weight": 0.7}, layer="communication")
    graph.add_edge(350, "Dumbledore", "Harry", properties={"weight": 0.9}, layer="friendship")
    return graph


def test_history():
    graph = create_graph()

    # test node
    query = """
    {
      graph(path: "g") {
        node(name: "Dumbledore") {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }"""
    expected_output = {'graph': {'node': {'history': {'timestamps': {'list': [100, 150, 200, 200, 300, 300, 350]}}}}}
    queries_and_expected_outputs = [(query, expected_output)]

    # test edge
    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            timestamps {
              list
            }
          }
        }
      }
    }"""
    expected_output = {'graph': {'edge': {'history': {'timestamps': {'list': [150, 200, 300, 350]}}}}}
    queries_and_expected_outputs.append((query, expected_output))

    # test windowed node
    query = """
    {
      graph(path: "g") {
        window(start: 0, end: 150) {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_1 = {'graph': {'window': {'node': {'history': {'timestamps': {'list': [100]}}}}}}
    queries_and_expected_outputs.append((query, expected_output_1))
    query = """
    {
      graph(path: "g") {
        window(start: 150, end: 300) {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_2 = {'graph': {'window': {'node': {'history': {'timestamps': {'list': [150, 200, 200]}}}}}}
    queries_and_expected_outputs.append((query, expected_output_2))

    query = """
    {
      graph(path: "g") {
        window(start: 300, end: 450) {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_3 = {'graph': {'window': {'node': {'history': {'timestamps': {'list': [300, 300, 350]}}}}}}
    queries_and_expected_outputs.append((query, expected_output_3))

    # test windowed edge
    query = """
    {
      graph(path: "g") {
        window(start: 0, end: 150) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    """
    expected_output_1 = {'graph': {'window': {'edge': None}}}
    queries_and_expected_outputs.append((query, expected_output_1))

    query = """
    {
      graph(path: "g") {
        window(start: 150, end: 300) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    """
    expected_output_2 = {'graph': {'window': {'edge': {'history': {'timestamps': {'list': [150, 200]}}}}}}
    queries_and_expected_outputs.append((query, expected_output_2))

    query = """
    {
      graph(path: "g") {
        window(start: 300, end: 450) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    """
    expected_output_3 = {'graph': {'window': {'edge': {'history': {'timestamps': {'list': [300, 350]}}}}}}
    queries_and_expected_outputs.append((query, expected_output_3))

    # test layered node
    query = """
    {
      graph(path: "g") {
        layer(name: "friendship") {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output = {'graph': {'layer': {'node': {'history': {'timestamps': {'list': [100, 200, 200, 300, 350]}}}}}}
    queries_and_expected_outputs.append((query, expected_output))

    # test layered edge
    query = """
    {
      graph(path: "g") {
        layer(name: "communication") {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }
    """
    expected_output = {'graph': {'layer': {'edge': {'history': {'timestamps': {'list': [150, 300]}}}}}}
    queries_and_expected_outputs.append((query, expected_output))

    # test edge filtered by property, when the same property for the same edge is updated at different times
    query_1 = """
    {
      graph(path: "g") {
        edgeFilter(
          filter: {property: {name: "weight", operator: EQUAL, value: {f64: 0.9}}}
        ) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_1 = {'graph': {'edgeFilter': {'edge': {'history': {'timestamps': {'list': [150, 200, 300, 350]}}}}}}
    queries_and_expected_outputs.append((query_1, expected_output_1))

    query_2 = """
    {
      graph(path: "g") {
        edgeFilter(
          filter: {property: {name: "weight", operator: EQUAL, value: {f64: 0.7}}}
        ) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_2 = {'graph': {'edgeFilter': {'edge': None}}}
    queries_and_expected_outputs.append((query_2, expected_output_2))

    # test node filtered by property, when the same property is updated at different times
    query_1 = """
    {
      graph(path: "g") {
        nodeFilter(
          filter: {property: {name: "Age", operator: LESS_THAN, value: {i64: 51}}}
        ) {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_1 = {'graph': {'nodeFilter': {'node': None}}}
    queries_and_expected_outputs.append((query_1, expected_output_1))

    query_2 = """
    {
      graph(path: "g") {
        nodeFilter(
          filter: {property: {name: "Age", operator: GREATER_THAN_OR_EQUAL, value: {i64: 51}}}
        ) {
          node(name: "Dumbledore") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_2 = {'graph': {'nodeFilter': {'node': {'history': {'timestamps': {'list': [100, 200, 300]}}}}}}
    queries_and_expected_outputs.append((query_2, expected_output_2))

    query_3 = """
    {
      graph(path: "g") {
        nodeFilter(
          filter: {property: {name: "Age", operator: LESS_THAN, value: {i64: 21}}}
        ) {
          node(name: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_3 = {'graph': {'nodeFilter': {'node': None}}}
    queries_and_expected_outputs.append((query_3, expected_output_3))

    query_4 = """
    {
      graph(path: "g") {
        nodeFilter(
          filter: {property: {name: "Age", operator: GREATER_THAN_OR_EQUAL, value: {i64: 21}}}
        ) {
          node(name: "Harry") {
            history {
              timestamps {
                list
              }
            }
          }
        }
      }
    }"""
    expected_output_4 = {'graph': {'nodeFilter': {'node': {'history': {'timestamps': {'list': [150, 150, 200, 250, 300, 350, 350]}}}}}}
    queries_and_expected_outputs.append((query_4, expected_output_4))

    run_group_graphql_test(queries_and_expected_outputs, graph)
