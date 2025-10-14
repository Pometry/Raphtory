import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop5"
              where: { eq: { list: [ {i64: 1}, {i64: 2}, {i64: 3} ] } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop5"
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
        "Wrong type for property prop5: expected List(I64) but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop4"
              where: { ne: { bool: true } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop4"
              where: { ne: { i64: 1 } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop4: expected Bool but actual type is I64"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop1"
              where: { ge: { i64: 60 } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: {
              name: "prop1"
              where: { ge: { bool: true } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            property: { name: "prop1", where: { le: { i64: 30 } } }
          }
        ) {
          nodes { list { name } }
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
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { le: { str: "shivam" } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { gt: { i64: 30 } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { gt: { str: "shivam" } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { lt: { i64: 30 } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { lt: { str: "shivam" } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Wrong type for property prop1: expected I64 but actual type is Str"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop5", where: { isNone: true } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop5", where: { isSome: true } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { isIn: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}] } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: { property: { name: "prop1", where: { isIn: { list: [] } } } }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"nodeFilter": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in_no_value(graph):
    # With where-shape, an empty list is a valid value (yields empty result).
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { isIn: { list: [] } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { isIn: { str: "shivam" } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_not_in_any(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { isNotIn: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}] } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(
            filter: { property: { name: "prop1", where: { isNotIn: { list: [] } } } }
          ) {
            list { name }
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
def test_graph_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: { property: { name: "prop1", where: { isNotIn: { str: "shivam" } } } }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isNotIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_not_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter (
          filter: {
            not: {
              property: {
                name: "prop5"
                where: { eq: { list: [ {i64: 1}, {i64: 2} ] } }
              }
            }
          }
        ) {
          nodes { list { name } }
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
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_type_and_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          nodeFilter(filter: {
            and: [
              {
                node: {
                  field: NODE_TYPE,
                  where: { isIn: { list: [ {str: "fire_nation"}, {str: "water_tribe"} ] } }
                }
              },
              {
                property: {
                  name: "prop2",
                  where: { gt: { f64: 1 } }
                }
              }
            ]
          }) {
            count
            list { name }
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
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(filter: {
          property: {
            name: "prop3"
            where: { startsWith: { str: "abc" } }
          }
        }) {
          nodes { list { name } }
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
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(filter: {
          property: {
            name: "prop3"
            where: { endsWith: { str: "123" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_starts_with_temporal_any(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            temporalProperty: {
              name: "prop3",
              where: { any: { startsWith: { str: "abc1" } } }
            }
          }
        ) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)
