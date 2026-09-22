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
        filterNodes: filter(expr: {
          eq: {
            lhs: { read: { entity: NODE, target: { property: "prop5" } } }
            rhs: { const: { list: [{ i64: 1 }, { i64: 2 }, { i64: 3 }] } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          eq: {
            lhs: { read: { entity: NODE, target: { property: "prop5" } } }
            rhs: { const: { i64: 1 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: value I64(1) of type I64 cannot be compared with List<I64>"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_not_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          ne: {
            lhs: { read: { entity: NODE, target: { property: "prop4" } } }
            rhs: { const: { bool: true } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          ne: {
            lhs: { read: { entity: NODE, target: { property: "prop4" } } }
            rhs: { const: { i64: 1 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: value I64(1) of type I64 cannot be compared with Bool"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          ge: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { i64: 60 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          ge: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_or_equal(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          le: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterNodes": {
                "nodes": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          le: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          gt: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          gt: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          lt: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { i64: 30 } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          lt: {
            lhs: { read: { entity: NODE, target: { property: "prop1" } } }
            rhs: { const: { str: "shivam" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_error_message = 'Invalid filter: value Str(ArcStr("shivam")) of type Str cannot be compared with I64'
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_none(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: { isNone: { read: { entity: NODE, target: { property: "prop5" } } } }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: { isSome: { read: { entity: NODE, target: { property: "prop5" } } } }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          isIn: {
            expr: { read: { entity: NODE, target: { property: "prop1" } } }
            values: { list: [{ i64: 10 }, { i64: 30 }, { i64: 50 }, { i64: 70 }] }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            isIn: {
              expr: { read: { entity: NODE, target: { property: "prop1" } } }
              values: { list: [] }
            }
          }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in_no_value(graph):
    # With where-shape, an empty list is a valid value (yields empty result).
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          isIn: {
            expr: { read: { entity: NODE, target: { property: "prop1" } } }
            values: { list: [] }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": []}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          isIn: {
            expr: { read: { entity: NODE, target: { property: "prop1" } } }
            values: { str: "shivam" }
          }
        }) {
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
        filterNodes: filter(expr: {
          isNotIn: {
            expr: { read: { entity: NODE, target: { property: "prop1" } } }
            values: { list: [{ i64: 10 }, { i64: 30 }, { i64: 50 }, { i64: 70 }] }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          filter(expr: {
            isNotIn: {
              expr: { read: { entity: NODE, target: { property: "prop1" } } }
              values: { list: [] }
            }
          }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "filter": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          isNotIn: {
            expr: { read: { entity: NODE, target: { property: "prop1" } } }
            values: { str: "shivam" }
          }
        }) {
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
        filterNodes: filter(expr: {
          not: {
            eq: {
              lhs: { read: { entity: NODE, target: { property: "prop5" } } }
              rhs: { const: { list: [{ i64: 1 }, { i64: 2 }] } }
            }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterNodes": {
                "nodes": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_node_type_and_property_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            and: [
              {
                isIn: {
                  expr: { read: { entity: NODE, target: { field: NODE_TYPE } } }
                  values: { list: [{ str: "fire_nation" }, { str: "water_tribe" }] }
                }
              },
              {
                gt: {
                  lhs: { read: { entity: NODE, target: { property: "prop2" } } }
                  rhs: { const: { f64: 1 } }
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
                "select": {
                    "count": 3,
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          startsWith: {
            lhs: { read: { entity: NODE, target: { property: "prop3" } } }
            rhs: { const: { str: "abc" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "filterNodes": {
                "nodes": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph, sort_output=True)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          endsWith: {
            lhs: { read: { entity: NODE, target: { property: "prop3" } } }
            rhs: { const: { str: "123" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_graph_nodes_property_filter_starts_with_temporal_any(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes: filter(expr: {
          startsWith: {
            lhs: {
              any: { temporal: { read: { entity: NODE, target: { property: "prop3" } } } }
            }
            rhs: { const: { str: "abc1" } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)
