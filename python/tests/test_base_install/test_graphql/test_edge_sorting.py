import pytest
from raphtory import Graph, PersistentGraph
from utils import run_graphql_test


def create_test_graph(g):
    g.add_edge(
        3,
        "a",
        "d",
        properties={
            "eprop1": 60,
            "eprop2": 0.4,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [1, 2, 3],
        },
    )
    g.add_edge(
        2,
        "b",
        "d",
        properties={
            "eprop1": 10,
            "eprop2": 1.7,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [3, 4, 5],
        },
    )
    g.add_edge(
        1,
        "c",
        "d",
        properties={
            "eprop1": 30,
            "eprop2": 6.4,
            "eprop3": "xyz123",
            "eprop4": False,
            "eprop5": [10],
        },
    )
    g.add_edge(
        1,
        "a",
        "b",
        properties={
            "eprop1": 80,
            "eprop2": 3.3,
            "eprop3": "xyz1234",
            "eprop4": False,
        },
    )
    g.add_edge(
        4,
        "b",
        "c",
        properties={
            "eprop1": 100,
            "eprop2": -2.3,
            "eprop3": "ayz123",
            "eprop5": [10, 20, 30],
        },
    )
    return g


EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_no_sort(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "list": [
                    {"src": {"id": "a"}, "dst": {"id": "d"}},
                    {"src": {"id": "b"}, "dst": {"id": "d"}},
                    {"src": {"id": "c"}, "dst": {"id": "d"}},
                    {"src": {"id": "a"}, "dst": {"id": "b"}},
                    {"src": {"id": "b"}, "dst": {"id": "c"}},
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_nothing(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: []) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_src(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ src: true }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_dst(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ dst: true }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_earliest_time(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ time: EARLIEST }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
              earliestTime
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "c"}, "dst": {"id": "d"}, "earliestTime": 1},
                        {"src": {"id": "a"}, "dst": {"id": "b"}, "earliestTime": 1},
                        {"src": {"id": "b"}, "dst": {"id": "d"}, "earliestTime": 2},
                        {"src": {"id": "a"}, "dst": {"id": "d"}, "earliestTime": 3},
                        {"src": {"id": "b"}, "dst": {"id": "c"}, "earliestTime": 4},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_earliest_time_reversed(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ time: EARLIEST, reverse: true }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        # C->D and A->B have the same time so will maintain their relative order
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_latest_time(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ time: LATEST }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_eprop1(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop1" }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_eprop2(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop2" }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_eprop3(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop3" }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_eprop4(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop4" }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_eprop5(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop5" }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_nonexistent_prop(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "i_dont_exist" }, { property: "eprop2", reverse: true }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_combined(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ property: "eprop3", reverse: true }, { property: "eprop4" }, { time: EARLIEST }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_sort_by_combined_2(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          sorted(sortBys: [{ dst: true }, { time: EARLIEST }, { property: "eprop3" }, { time: LATEST, reverse: true }]) {
            list {
              src {
                id
              }
              dst {
                id
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "sorted": {
                    "list": [
                        {"src": {"id": "a"}, "dst": {"id": "b"}},
                        {"src": {"id": "b"}, "dst": {"id": "c"}},
                        {"src": {"id": "c"}, "dst": {"id": "d"}},
                        {"src": {"id": "b"}, "dst": {"id": "d"}},
                        {"src": {"id": "a"}, "dst": {"id": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
