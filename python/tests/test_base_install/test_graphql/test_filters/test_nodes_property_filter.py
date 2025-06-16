import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
            nodeFilter(
                filter: {
                    property: {
                        name: "prop5"
                        operator: EQUAL
                        value: { list: [ {i64: 1}, {i64: 2}, {i64: 3} ] }
                  }
                }
              ) {
                list {
                  name
                }
            }
         }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
            nodeFilter(
              filter: {
                  property: {
                      name: "prop5"
                      operator: EQUAL
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
            nodeFilter(
              filter: {
                  property: {
                      name: "prop5"
                      operator: EQUAL
                      value: { i64: 1 }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop5: expected List(I64) but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
         nodes {
            nodeFilter(
              filter: {
                  property: {
                      name: "prop4"
                      operator: NOT_EQUAL
                      value: { bool: true }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop4"
                      operator: NOT_EQUAL
                }
              }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator NOT_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
           nodeFilter(
               filter: {
                   property: {
                       name: "prop4"
                       operator: NOT_EQUAL
                       value:  { i64: 1 }
                 }
               }
           ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop4: expected Bool but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN_OR_EQUAL
                    value:  { i64: 60 }
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN_OR_EQUAL
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator GREATER_THAN_OR_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: GREATER_THAN_OR_EQUAL
                      value: { bool: true }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN_OR_EQUAL
                    value: { i64: 30 }
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "nodeFilter": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN_OR_EQUAL
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator LESS_THAN_OR_EQUAL requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: LESS_THAN_OR_EQUAL
                      value: { str: "shivam" }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN
                    value: { i64: 30 }
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: GREATER_THAN
                }
              }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator GREATER_THAN requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN
                    value: { str: "shivam" }
              }
            }
           ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: LESS_THAN
                      value: { i64: 30 }
                }
              }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator LESS_THAN requires a value"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: LESS_THAN
                      value: { str: "shivam" }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop5"
                    operator: IS_NONE
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop5"
                      operator: IS_SOME
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_IN
                    value: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}]}
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_IN
                    value: { list: []}
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_no_value(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: IS_IN
                      value: { list: [{i64: 100}]}
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_IN
                    value: { str: "shivam" }
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: Operator IS_IN requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: IS_NOT_IN
                      value: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}]}
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"nodeFilter": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: IS_NOT_IN
                      value: { list: []}
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "nodeFilter": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_NOT_IN
              }
            }
          ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Operator IS_NOT_IN requires a non-empty list"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
              filter: {
                  property: {
                      name: "prop1"
                      operator: IS_NOT_IN
                      value: { str: "shivam" }
                }
              }
            ) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: Operator IS_NOT_IN requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)
