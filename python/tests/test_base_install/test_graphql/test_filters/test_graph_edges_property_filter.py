import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: EQUAL
                  value: { list: [{i64: 1},{i64: 2},{i64: 3}]}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
          edgeFilter(
            filter: {
                property: {
                    name: "eprop5"
                    operator: EQUAL
                    value: { list: [{i64: 1},{i64: 2},{i64: 3}]}
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: EQUAL
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop5"
                    operator: EQUAL
                    value: { i64: 1 }
              }
            }
          ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop5: expected List(I64) but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: EQUAL
                  value: { i64: 1 }
            }
          }
        ) {
          nodes {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop4"
                    operator: NOT_EQUAL
                    value: { bool: true }
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "c"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_not_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
         edgeFilter(
          filter: {
              property: {
                  name: "eprop4"
                  operator: NOT_EQUAL
                  value: { bool: true }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_not_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop4"
                    operator: NOT_EQUAL
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator NOT_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop4"
                  operator: NOT_EQUAL
                  value: { i64: 1 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop4: expected Bool but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_not_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop4"
                    operator: NOT_EQUAL
                    value: { i64: 1 }
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN_OR_EQUAL
                  value: { i64: 60 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_greater_than_or_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop1"
                    operator: GREATER_THAN_OR_EQUAL
                    value: { i64: 60 }
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN_OR_EQUAL
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator GREATER_THAN_OR_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop1"
                    operator: GREATER_THAN_OR_EQUAL
                    value: { bool: true }
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_greater_than_or_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN_OR_EQUAL
                  value: { bool: true }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
            filter: {
                property: {
                    name: "eprop1"
                    operator: LESS_THAN_OR_EQUAL
                    value: { i64: 30 }
              }
            }
          ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
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


def test_graph_edge_property_filter_less_than_or_equal_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN_OR_EQUAL
                  value: { i64: 30 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN_OR_EQUAL
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator LESS_THAN_OR_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN_OR_EQUAL
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_less_than_or_equal_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN_OR_EQUAL
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN
                  value: { i64: 30 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_greater_than_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN
                  value: { i64: 30 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_greater_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator GREATER_THAN requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_greater_than_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: GREATER_THAN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN
                  value: { i64: 30 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "b"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_less_than_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN
                  value: { i64: 30 }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_less_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator LESS_THAN requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property eprop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_less_than_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: LESS_THAN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: IS_NONE
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"edgeFilter": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_is_none_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: IS_NONE
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: IS_SOME
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
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


def test_graph_edge_property_filter_is_some_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop5"
                  operator: IS_SOME
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_IN
                  value: { list: [{i64: 10},{i64: 20},{i64: 30}]}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
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


def test_graph_edge_property_filter_is_in_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_IN
                  value: { list: [{i64: 10},{i64: 20},{i64: 30}]}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"edgeFilter": {"edges": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_is_in_empty_list_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_in_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: IS_IN
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator IS_IN requires a non-empty list"
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_is_in_type_error():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_IN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: Operator IS_IN requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, EVENT_GRAPH)


def test_graph_edge_property_filter_is_in_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: IS_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { list: [{i64: 10},{i64: 20},{i64: 30}]}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "a"}, "dst": {"name": "d"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


def test_graph_edge_property_filter_is_not_in_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { list: [{i64: 10},{i64: 20},{i64: 30}]}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [EVENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
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


def test_graph_edge_property_filter_is_not_in_empty_list_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_edge_property_filter_is_not_in_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator IS_NOT_IN requires a non-empty list"
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_is_not_in_type_error():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { str: "shivam" }
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: Operator IS_NOT_IN requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, EVENT_GRAPH)


def test_graph_edge_property_filter_is_not_in_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "eprop1"
                  operator: IS_NOT_IN
                  value: { list: []}
            }
          }
        ) {
          edges {
            list {
              src{name}
              dst{name}
            }
          }
        }
      }
    }
    """
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    run_graphql_error_test(query, expected_error_message, PERSISTENT_GRAPH)


def test_graph_edge_not_property_filter():
    query = """
    query {
      graph(path: "g") {
        edgeFilter (
          filter: {
            not:
              {
                property: {
                  name: "eprop5"
                  operator: EQUAL
                  value: { list: [{i64: 1},{i64: 2}]}
              	}
              }
          }
        ) {
          edges {
              list {
                src{name}
                dst{name}
              }
            }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
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
    run_graphql_test(query, expected_output, EVENT_GRAPH)
