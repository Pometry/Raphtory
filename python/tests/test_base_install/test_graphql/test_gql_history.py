from datetime import datetime, timezone

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
    graph.add_edge(
        200, "Dumbledore", "Harry", properties={"weight": 0.5}, layer="friendship"
    )
    graph.add_edge(
        300, "Dumbledore", "Harry", properties={"weight": 0.7}, layer="communication"
    )
    graph.add_edge(
        350, "Dumbledore", "Harry", properties={"weight": 0.9}, layer="friendship"
    )
    return graph


def test_history():
    graph = create_graph()
    queries_and_expected_outputs = []

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
    expected_output = {
        "graph": {
            "node": {
                "history": {"timestamps": {"list": [100, 150, 200, 200, 300, 300, 350]}}
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

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
    expected_output = {
        "graph": {"edge": {"history": {"timestamps": {"list": [150, 200, 300, 350]}}}}
    }
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
    expected_output_1 = {
        "graph": {"window": {"node": {"history": {"timestamps": {"list": [100]}}}}}
    }
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
    expected_output_2 = {
        "graph": {
            "window": {"node": {"history": {"timestamps": {"list": [150, 200, 200]}}}}
        }
    }
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
    expected_output_3 = {
        "graph": {
            "window": {"node": {"history": {"timestamps": {"list": [300, 300, 350]}}}}
        }
    }
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
    expected_output_1 = {"graph": {"window": {"edge": None}}}
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
    expected_output_2 = {
        "graph": {"window": {"edge": {"history": {"timestamps": {"list": [150, 200]}}}}}
    }
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
    expected_output_3 = {
        "graph": {"window": {"edge": {"history": {"timestamps": {"list": [300, 350]}}}}}
    }
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
    expected_output = {
        "graph": {
            "layer": {
                "node": {"history": {"timestamps": {"list": [100, 200, 200, 300, 350]}}}
            }
        }
    }
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
    expected_output = {
        "graph": {"layer": {"edge": {"history": {"timestamps": {"list": [150, 300]}}}}}
    }
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
    expected_output_1 = {
        "graph": {
            "edgeFilter": {
                "edge": {"history": {"timestamps": {"list": [150, 200, 300, 350]}}}
            }
        }
    }
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
    expected_output_2 = {"graph": {"edgeFilter": {"edge": None}}}
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
    expected_output_1 = {"graph": {"nodeFilter": {"node": None}}}
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
    expected_output_2 = {
        "graph": {
            "nodeFilter": {
                "node": {"history": {"timestamps": {"list": [100, 200, 300]}}}
            }
        }
    }
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
    expected_output_3 = {"graph": {"nodeFilter": {"node": None}}}
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
    expected_output_4 = {
        "graph": {
            "nodeFilter": {
                "node": {
                    "history": {
                        "timestamps": {"list": [150, 150, 200, 250, 300, 350, 350]}
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query_4, expected_output_4))

    run_group_graphql_test(queries_and_expected_outputs, graph)


def test_gql_event_time():
    graph = create_graph()
    graph.add_edge(
        datetime(2025, 1, 20, 0, 0, tzinfo=timezone.utc), "Dumbledore", "Harry"
    )
    queries_and_expected_outputs = []
    queries_and_expected_errors = []

    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            datetimes {
              list
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "history": {
                    "datetimes": {
                        "list": [
                            "1970-01-01T00:00:00.150+00:00",
                            "1970-01-01T00:00:00.200+00:00",
                            "1970-01-01T00:00:00.300+00:00",
                            "1970-01-01T00:00:00.350+00:00",
                            "2025-01-20T00:00:00+00:00",
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            datetimes(formatString: "%Y-%m-%d") {
              list
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "history": {
                    "datetimes": {
                        "list": [
                            "1970-01-01",
                            "1970-01-01",
                            "1970-01-01",
                            "1970-01-01",
                            "2025-01-20",
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            datetimes(formatString: "%Y-%m-%d %H:%M:%S %3fms") {
              list
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "history": {
                    "datetimes": {
                        "list": [
                            "1970-01-01 00:00:00 150ms",
                            "1970-01-01 00:00:00 200ms",
                            "1970-01-01 00:00:00 300ms",
                            "1970-01-01 00:00:00 350ms",
                            "2025-01-20 00:00:00 000ms",
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # call datetime on individual EventTimes
    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            list {
              datetime(formatString: "%Y-%m-%d %H:%M:%S %3fms")
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edge": {
                "history": {
                    "list": [
                        {"datetime": "1970-01-01 00:00:00 150ms"},
                        {"datetime": "1970-01-01 00:00:00 200ms"},
                        {"datetime": "1970-01-01 00:00:00 300ms"},
                        {"datetime": "1970-01-01 00:00:00 350ms"},
                        {"datetime": "2025-01-20 00:00:00 000ms"},
                    ]
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # invalid format string should return error but not crash server
    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            datetimes(formatString: "%Y-%m-%d %H:%M:%S %4fms") {
              list
            }
          }
        }
      }
    }
    """
    queries_and_expected_errors.append(
        (query, "Invalid datetime format string: '%Y-%m-%d %H:%M:%S %4fms'")
    )

    # error when we call datetime on individual EventTimes
    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            list {
              datetime(formatString: "%Y-%m-%d %H:%M:%S %4fms")
            }
          }
        }
      }
    }
    """
    queries_and_expected_errors.append(
        (query, "Invalid datetime format string: '%Y-%m-%d %H:%M:%S %4fms'")
    )

    query = """
    {
      graph(path: "g") {
        edge(src: "Dumbledore", dst: "Harry") {
          history {
            eventId {
              list
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"edge": {"history": {"eventId": {"list": [6, 7, 8, 9, 10]}}}}
    }
    queries_and_expected_outputs.append((query, expected_output))

    # test parsing of time inputs
    query = """
    {
      graph(path: "g") {
        window(start: "1970-01-01T00:00:00", end: "1970-01-01T00:00:00.150") {
          node(name: "Dumbledore") {
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
    expected_output = {
        "graph": {"window": {"node": {"history": {"timestamps": {"list": [100]}}}}}
    }
    queries_and_expected_outputs.append((query, expected_output))

    # test eventId time parsing and windowing behaviour
    query = """
    {
      graph(path: "g") {
        window(
          start: {timestamp: "1970-01-01T00:00:00.150+00:00", id: 0}
          end: {time: "1970-01-01T00:00:00.350+00:00", eventId: 10}
        ) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              list {
                timestamp
                eventId
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
                "edge": {
                    "history": {
                        "list": [
                            {"timestamp": 150, "eventId": 6},
                            {"timestamp": 200, "eventId": 7},
                            {"timestamp": 300, "eventId": 8},
                            {"timestamp": 350, "eventId": 9},
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # end of the window is exclusive, timestamp: 350 should be missing
    query = """
    {
      graph(path: "g") {
        window(
          start: {timestamp: 150, eventId: 6}
          end: {time: "1970-01-01 00:00:00.350", id: 9}
        ) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              list {
                timestamp
                eventId
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
                "edge": {
                    "history": {
                        "list": [
                            {"timestamp": 150, "eventId": 6},
                            {"timestamp": 200, "eventId": 7},
                            {"timestamp": 300, "eventId": 8},
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    # start of window is inclusive, we need to go past it
    query = """
    {
      graph(path: "g") {
        window(
          start: {timestamp: 150, id: 7}
          end: {time: "1970-01-01T00:00:00.351", eventId: 8}
        ) {
          edge(src: "Dumbledore", dst: "Harry") {
            history {
              list {
                timestamp
                eventId
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
                "edge": {
                    "history": {
                        "list": [
                            {"timestamp": 200, "eventId": 7},
                            {"timestamp": 300, "eventId": 8},
                            {"timestamp": 350, "eventId": 9},
                        ]
                    }
                }
            }
        }
    }
    queries_and_expected_outputs.append((query, expected_output))

    run_group_graphql_test(queries_and_expected_outputs, graph)
