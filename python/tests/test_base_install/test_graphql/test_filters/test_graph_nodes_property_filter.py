import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import PORT, create_test_graph
from utils import run_graphql_test, run_graphql_error_test

@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
       nodeFilter(
        filter: {
            property: {
                name: "prop5"
                operator: EQUAL
                value: { list: [ {i64: 1}, {i64: 2}, {i64: 3} ] }
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
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop5"
                  operator: EQUAL
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
    expected_error_message = "Expected a value for Equal operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop5"
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
    expected_error_message = "PropertyType Error: Wrong type for property prop5: expected List(I64) but actual type is I64"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop4"
                  operator: NOT_EQUAL
                  value: { bool: true }
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop4"
                    operator: NOT_EQUAL
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
    expected_error_message = "Expected a value for NotEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop4"
                    operator: NOT_EQUAL
                    value:  { i64: 1 }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop4: expected Bool but actual type is I64"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: GREATER_THAN_OR_EQUAL
                  value:  { i64: 60 }
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
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: GREATER_THAN_OR_EQUAL
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
    expected_error_message = "Expected a value for GreaterThanOrEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN_OR_EQUAL
                    value: { bool: true }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Bool"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: LESS_THAN_OR_EQUAL
                  value: { i64: 30 }
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
    expected_output = {
        "graph": {
            "nodeFilter": {
                "nodes": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}
            }
        }
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: LESS_THAN_OR_EQUAL
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
    expected_error_message = "Expected a value for LessThanOrEqual operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN_OR_EQUAL
                    value: { str: "shivam" }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: GREATER_THAN
                  value: { i64: 30 }
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
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: GREATER_THAN
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
    expected_error_message = "Expected a value for GreaterThan operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: GREATER_THAN
                  value: { str: "shivam" }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN
                    value: { i64: 30 }
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: LESS_THAN
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
    expected_error_message = "Expected a value for LessThan operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: LESS_THAN
                    value: { str: "shivam" }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop5"
                  operator: IS_NONE
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
          nodeFilter(
            filter: {
                property: {
                    name: "prop5"
                    operator: IS_SOME
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: IS_IN
                  value: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}]}
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


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
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_in_no_value(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_IN
                    value: { list: []}
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
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": []}}}}
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: IS_IN
                  value: { str: "shivam" }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_not_in_any(graph):
    query = """
    query {
      graph(path: "g") {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_NOT_IN
                    value: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}]}
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
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_node_property_filter_not_is_not_in_empty_list(graph):
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
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_not_in_no_value_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
              property: {
                  name: "prop1"
                  operator: IS_NOT_IN
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
    expected_error_message = "Expected a value for IsNotIn operator"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
          nodeFilter(
            filter: {
                property: {
                    name: "prop1"
                    operator: IS_NOT_IN
                    value: { str: "shivam" }
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
    expected_error_message = "PropertyType Error: Wrong type for property prop1: expected I64 but actual type is Str"
    graph = create_test_graph(graph())
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_not_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter (
          filter: {
            not: 
              {
                property: {
                  name: "prop5"
                  operator: EQUAL
                  value: { list: [ {i64: 1}, {i64: 2} ] }
              	}
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
    expected_output = {
        "graph": {
            "nodeFilter": {
                "nodes": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)

@pytest.mark.parametrize("graph", [Graph, PersistentGraph])
def test_graph_node_type_and_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(filter: {
            and: [{
              node: {
              field: NODE_TYPE,
              operator: IS_IN,
              value:  {
                list: [
                  {str: "fire_nation"},
                  {str: "water_tribe"}
                ]
              }
              }
            },{
              property: {
                name: "prop2",
                operator: GREATER_THAN,
                value: { f64:1 }
              }
            }]
          }) {
            count
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
              "count": 3,
              "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}]
            }
          }
        }
    }
    graph = create_test_graph(graph())
    run_graphql_test(query, expected_output, graph)
