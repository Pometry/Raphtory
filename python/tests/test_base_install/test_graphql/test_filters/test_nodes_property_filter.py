import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import (
    create_test_graph,
    create_test_graph2,
    create_test_graph3,
    init_graph,
    init_graph2,
)
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          filter(
            expr: {
              property: {
                name: "prop5"
                where: {
                  eq: { list: [ {i64: 1}, {i64: 2}, {i64: 3} ] }
                }
              }
            }
          ) {
            list {
                neighbours {
                  list {
                    name
                  }
                }
          }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "filter": {
                    "list": [
                        {"neighbours": {"list": []}},
                        {"neighbours": {"list": []}},
                        {"neighbours": {"list": []}},
                        {"neighbours": {"list": [{"name": "a"}]}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal3(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop5"
                where: {
                  eq: { list: [ {i64: 1}, {i64: 2}, {i64: 3} ] }
                }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop5"
                where: {
                  eq: { i64: 1 }
                }
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
          select(
            expr: {
              property: {
                name: "prop4"
                where: {
                  ne: { bool: true }
                }
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
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_not_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop4"
                where: {
                  ne: { i64: 1 }
                }
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
          select(
            expr: {
              property: {
                name: "prop1"
                where: {
                  ge: { i64: 60 }
                }
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
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: {
                  ge: { bool: true }
                }
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
          select(
            expr: {
              property: {
                name: "prop1"
                where: {
                  le: { i64: 30 }
                }
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
            "nodes": {"select": {"list": [{"name": "b"}, {"name": "c"}, {"name": "d"}]}}
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_or_equal_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { le: { str: "shivam" } }
              }
            }
          ) {
            list { name }
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
          select(
            expr: {
              property: {
                name: "prop1"
                where: { gt: { i64: 30 } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_greater_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { gt: { str: "shivam" } }
              }
            }
          ) {
            list { name }
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
          select(
            expr: {
              property: {
                name: "prop1"
                where: { lt: { i64: 30 } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_less_than_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { lt: { str: "shivam" } }
              }
            }
          ) {
            list { name }
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
          select(
            expr: {
              property: {
                name: "prop5"
                where: { isNone: true }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_some(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop5"
                where: { isSome: true }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isIn: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}] } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "b"}, {"name": "d"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isIn: { list: [] } }
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
def test_node_property_filter_is_in_no_value(graph):
    # Keeping semantics: value list has no matching elements
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isIn: { list: [{i64: 100}] } }
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
def test_node_property_filter_is_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isIn: { str: "shivam" } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isNotIn: { list: [{i64: 10},{i64: 30},{i64: 50},{i64: 70}] } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"list": [{"name": "a"}, {"name": "c"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_empty_list(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isNotIn: { list: [] } }
              }
            }
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
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_is_not_in_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(
            expr: {
              property: {
                name: "prop1"
                where: { isNotIn: { str: "shivam" } }
              }
            }
          ) {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = (
        "Invalid filter: isNotIn requires a list value, got Str(shivam)"
    )
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_node_property_filter_contains_wrong_value_type_error(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(expr: {
          property: {
            name: "p10"
            where: { contains: { u64: 2 } }
          }
        }) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = "Property p10 does not exist"
    run_graphql_error_test(query, expected_error_message, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            property: {
              name: "prop3"
              where: { startsWith: { str: "abc" } }
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
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_ends_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            property: {
              name: "prop3"
              where: { endsWith: { str: "333" } }
            }
          }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "c"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_temporal_first_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            temporalProperty: {
              name: "prop3"
              where: { first: { startsWith: { str: "abc" } } }
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
                "select": {
                    "list": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_temporal_all_starts_with(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: {
            temporalProperty: {
              name: "prop3"
              where: { any: { startsWith: { str: "abc1" } } }
            }
          }) {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodes": {"select": {"list": [{"name": "a"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_property_filter_list_agg(graph):
    # SUM(list(prop5)) == 6
    query = """
    query {
      graph(path: "g") {
        filterNodes(expr: {
          property: {
            name: "prop5"
            where: { sum: { eq: { i64: 6 } } }
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
def test_nodes_property_filter_list_qualifier(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(expr: {
          property: {
            name: "prop5"
            where: { any: { eq: { i64: 6 } } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {"graph": {"filterNodes": {"nodes": {"list": [{"name": "c"}]}}}}
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_temporal_property_filter_agg(graph):
    query = """
    query {
      graph(path: "g") {
        filterNodes(expr: {
          temporalProperty: {
            name: "p2"
            where: { avg: { lt: { f64: 10.0 } } }
          }
        }) {
          nodes { list { name } }
        }
      }
    }
    """
    expected_output = {
        "graph": {"filterNodes": {"nodes": {"list": [{"name": "2"}, {"name": "3"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_temporal_property_filter_any_avg(graph):
    # ANY timepoint where AVG(list) < 10.0
    query = """
    query {
      graph(path: "g") {
        filterNodes(expr: {
          temporalProperty: {
            name: "prop5"
            where: { any: { avg: { lt: { f64: 10.0 } } } }
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


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_neighbours_selection_with_prop_filter(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
          list {
            neighbours {
              list {
                name
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {
                "list": [
                    {"neighbours": {"list": [{"name": "2"}, {"name": "3"}]}},
                    {
                        "neighbours": {
                            "list": [{"name": "1"}, {"name": "2"}, {"name": "4"}]
                        }
                    },
                ]
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
            list {
              name
            }
          }
        }
      }
    """
    expected_output = {
        "graph": {"nodes": {"list": [{"name": "1"}, {"name": "3"}]}}
    }
    run_graphql_test(query, expected_output, graph)


# The inner nodes filter has no effect on the list of nodes returned from selection filter
@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection_nodes_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
          filter(expr:{
            property: { name: "p9", where: { eq:{ i64: 5 } } }
          }) {
            list {
              name
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"filter": {"list": [{"name": "1"}, {"name": "3"}]}}}
    }
    run_graphql_test(query, expected_output, graph)


# The inner nodes filter has effect on the neighbours list
@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_selection_nodes_filter_paired2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
          filter(expr:{
            property: { name: "p9", where: { eq:{ i64: 5 } } }
          }) {
            list {
              neighbours {
                list {
                  name
                }
              }
            }
          }
        }
      }
    }
    """
    expected_output = {'graph': {'nodes': {'filter': {'list': [
        {'neighbours': {'list': []}},
        {'neighbours': {'list': [{'name': '1'}]}}
    ]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_chained_selection_node_filter_paired(graph):
    query = """
    query {
      graph(path: "g") {
        nodes(select: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
          select(expr: { property: { name: "p9", where: { eq:{ i64: 5 } } } }) {
            filter(expr:{
              node: {
                field: NODE_TYPE
                where: { eq: { str: "fire_nation" } }
              }
            }) {
              list {
                name
              }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {"nodes": {"select": {"filter": {"list": [{"name": "1"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_nodes_chained_selection_node_filter_paired_ver2(graph):
    query = """
    query {
      graph(path: "g") {
        nodes {
          select(expr: { property: { name: "p100", where: { gt: { i64: 30 } } } }) {
            select(expr: { property: { name: "p9", where: { eq:{ i64: 5 } } } }) {
              filter(expr:{
                node: {
                  field: NODE_TYPE
                  where: { eq: { str: "fire_nation" } }
                }
              }) {
                list {
                  name
                }
              }
            }        
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "nodes": {"select": {"select": {"filter": {"list": [{"name": "1"}]}}}}
        }
    }
    run_graphql_test(query, expected_output, graph)
