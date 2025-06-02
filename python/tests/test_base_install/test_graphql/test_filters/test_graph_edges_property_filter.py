import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import PORT, create_test_graph
from utils import run_graphql_test, run_graphql_error_test


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for Equal operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop5: expected List(I64) but actual type is I64"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for NotEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop4: expected Bool but actual type is I64"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for GreaterThanOrEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Bool"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for LessThanOrEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for GreaterThan operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for LessThan operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for IsIn operator"
    graph = create_test_graph(graph())
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    graph = create_test_graph(Graph())
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_is_in_type_error_persistent_graph():
    query = """
    query {
      graph(path: "g") {
        edgeFilter(
          filter: {
              property: {
                  name: "prop1"
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
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


# Edge property filter is not supported yet for PersistentGraph
@pytest.mark.parametrize("graph", [Graph])
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
    graph = create_test_graph(graph())
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
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for IsNotIn operator"
    graph = create_test_graph(graph())
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
    expected_error_message = "PropertyType Error: Wrong type for property eprop1: expected I64 but actual type is Str"
    graph = create_test_graph(Graph())
    run_graphql_error_test(query, expected_error_message, graph)


def test_graph_edge_property_filter_is_not_in_type_error_persistent_graph():
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
    expected_error_message = "Property filtering not implemented on PersistentGraph yet"
    graph = create_test_graph(PersistentGraph())
    run_graphql_error_test(query, expected_error_message, graph)


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
    graph = create_test_graph(Graph())
    run_graphql_test(query, expected_output, graph)
