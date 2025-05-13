import tempfile

import pytest

from raphtory.graphql import GraphServer
from raphtory import Graph, PersistentGraph
import json
import re

PORT = 1737


def create_test_graph(g):
    g.add_node(
        1,
        "a",
        properties={
            "prop1": 60,
            "prop2": 31.3,
            "prop3": "abc123",
            "prop4": True,
            "prop5": [1, 2, 3],
        },
    )
    g.add_node(
        1,
        "b",
        properties={"prop1": 10, "prop2": 31.3, "prop3": "abc223", "prop4": False},
    )
    g.add_node(
        1,
        "c",
        properties={
            "prop1": 20,
            "prop2": 31.3,
            "prop3": "abc333",
            "prop4": True,
            "prop5": [5, 6, 7],
        },
    )
    g.add_node(
        1,
        "d",
        properties={"prop1": 30, "prop2": 31.3, "prop3": "abc444", "prop4": False},
    )
    g.add_edge(
        2,
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
        2,
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
    return g


def run_graphql_test(query, expected_output, graph):
    create_test_graph(graph)
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        response = client.query(query)

        # Convert response to a dictionary if needed and compare
        response_dict = json.loads(response) if isinstance(response, str) else response
        assert response_dict == expected_output


def run_graphql_error_test(query, expected_error_message, graph):
    create_test_graph(graph)
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(PORT) as server:
        client = server.get_client()
        client.send_graph(path="g", graph=graph)

        with pytest.raises(Exception) as excinfo:
            client.query(query)

        full_error_message = str(excinfo.value)
        match = re.search(r'"message":"(.*?)"', full_error_message)
        error_message = match.group(1) if match else ""

        assert (
            error_message == expected_error_message
        ), f"Expected '{expected_error_message}', but got '{error_message}'"


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for Equal operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop5: expected List(I64) but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for NotEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop4: expected Bool but actual type is I64"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for GreaterThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Bool"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for LessThanOrEqual operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for GreaterThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for LessThan operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": []}} }}
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    run_graphql_test(query, expected_output, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "Expected a value for IsNotIn operator"
    run_graphql_error_test(query, expected_error_message, graph())


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    run_graphql_error_test(query, expected_error_message, graph())