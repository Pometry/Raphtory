import pytest
from raphtory import Graph, PersistentGraph

from utils import run_graphql_test


def create_test_graph(g):
    g.add_node(
        3,
        "a",
        properties={
            "prop1": 60,
            "prop2": 0.4,
            "prop3": "xyz123",
            "prop4": True,
            "prop5": [1, 2, 3],
        },
    )
    g.add_node(
        2,
        "b",
        properties={
            "prop1": 10,
            "prop2": 1.7,
            "prop3": "xyz123",
            "prop4": True,
            "prop5": [3, 4, 5],
        },
    )
    g.add_node(
        1,
        "d",
        properties={
            "prop1": 30,
            "prop2": 6.4,
            "prop3": "xyz123",
            "prop4": False,
            "prop5": [50, 30],
        },
    )
    g.add_node(
        1,
        "b",
        properties={
            "prop1": 80,
            "prop2": 3.3,
            "prop3": "xyz1234",
            "prop4": False,
        },
    )
    g.add_node(
        4,
        "c",
        properties={
            "prop1": 100,
            "prop2": -2.3,
            "prop3": "ayz123",
            "prop5": [10, 20, 30],
        },
    )
    return g


EVENT_GRAPH = create_test_graph(Graph())

PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_sort_by_nothing(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: []) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "a"}, {"id": "b"}, {"id": "d"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_sort_by_id(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{id:true}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_earliest_time(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{time: EARLIEST}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "b"}, {"id": "d"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }

    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_earliest_time_reversed(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{time: EARLIEST, reverse: true}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "c"}, {"id": "a"}, {"id": "b"}, {"id": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_nodes_sort_by_latest_time(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{time: LATEST}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "d"}, {"id": "b"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_latest_time(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{time: LATEST}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    # In the persistent graph all nodes will have the same latest_time
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "a"}, {"id": "b"}, {"id": "d"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_prop1(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{property: "prop1"}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "b"}, {"id": "d"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_prop2(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{property: "prop2"}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "c"}, {"id": "a"}, {"id": "b"}, {"id": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_prop3(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{property: "prop3"}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "c"}, {"id": "a"}, {"id": "b"}, {"id": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_prop4(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{property: "prop4"}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "c"}, {"id": "d"}, {"id": "a"}, {"id": "b"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_prop5(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{property: "prop5"}]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_nonexistent_prop(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(sortBys: [{ property: "i_dont_exist" }, { property: "prop2", reverse: true }]) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "d"}, {"id": "b"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_sort_by_combined(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(
                sortBys: [{property: "prop3", reverse: true}, {property: "prop4"}, {time: EARLIEST}]
              ) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "d"}, {"id": "b"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_sort_by_combined_2(graph):
    query = """
        {
          graph(path: "g") {
            nodes {
              sorted(
                sortBys: [{time: EARLIEST}, {property: "prop3"}, {id: true, reverse: true}]
              ) {
                list {
                  id
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "sorted": {"list": [{"id": "d"}, {"id": "b"}, {"id": "a"}, {"id": "c"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
