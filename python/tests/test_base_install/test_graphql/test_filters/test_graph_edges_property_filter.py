import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop5"
              where: { eq: { list: [{i64: 1},{i64: 2},{i64: 3}] } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop5"
              where: { eq: { i64: 1 } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop5: expected List(I64) but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop4"
              where: { ne: { bool: true } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "c"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop4"
              where: { ne: { i64: 1 } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop4: expected Bool but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop1"
              where: { ge: { i64: 60 } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop1"
              where: { ge: { bool: true } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: {
            property: {
              name: "eprop1"
              where: { le: { i64: 30 } }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { le: { str: "shivam" } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { gt: { i64: 30 } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { gt: { str: "shivam" } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { lt: { i64: 30 } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "b"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { lt: { str: "shivam" } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop5", where: { isNone: true } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterEdges": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop5", where: { isSome: true } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isIn: { list: [{i64: 10},{i64: 20},{i64: 30}] } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isIn: { list: [] } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterEdges": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isIn: { str: "shivam" } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isNotIn: { list: [{i64: 10},{i64: 20},{i64: 30}] } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isNotIn: { list: [] } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(
          expr: { property: { name: "eprop1", where: { isNotIn: { str: "shivam" } } } }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isNotIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_not_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges (
          expr: {
            not: {
              property: {
                name: "eprop5"
                where: { eq: { list: [{i64: 1},{i64: 2}] } }
              }
            }
          }
        ) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"dst": {"name": "d"}, "src": {"name": "a"}},
                        {"dst": {"name": "d"}, "src": {"name": "b"}},
                        {"dst": {"name": "d"}, "src": {"name": "c"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(expr: {
          property: {
            name: "eprop3"
            where: { startsWith: { str: "xyz" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(expr: {
          property: {
            name: "eprop3"
            where: { endsWith: { str: "123" } }
          }
        }) {
          edges { list { src { name } dst { name } } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterEdges": {
                "edges": {
                    "list": [
                        {"src": {"name": "a"}, "dst": {"name": "d"}},
                        {"src": {"name": "b"}, "dst": {"name": "d"}},
                        {"src": {"name": "c"}, "dst": {"name": "d"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
