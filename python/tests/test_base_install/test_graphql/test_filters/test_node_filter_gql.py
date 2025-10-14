import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_graph, init_graph2
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            node: {
              field: NODE_ID
              where: { eq: { str: "1" } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_str_ids_for_node_id_eq_gql2(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            node: {
              field: NODE_ID
              where: { eq: { u64: 1 } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_error_message = "Invalid filter: Filter value type does not match node ID type. Expected Str but got \\"
    run_graphql_error_test(query, expected_error_message, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_nodes_with_num_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        nodeFilter(
          filter: {
            node: {
              field: NODE_ID
              where: { eq: { u64: 1 } }
            }
          }
        ) {
          nodes {
            list { name }
          }
        }
      }
    }
    """
    expected_output = {"graph": {"nodeFilter": {"nodes": {"list": [{"name": "1"}]}}}}
    run_graphql_test(query, expected_output, graph)
