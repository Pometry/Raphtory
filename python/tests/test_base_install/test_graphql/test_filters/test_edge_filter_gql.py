import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_graph, init_graph2
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_edges_with_str_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(filter: {
          src: {
            field: NODE_ID
            where: { eq: { str: "3" } }
          }
        }) {
          edges {
            list {
              src { name }
              dst { name }
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
                        {"dst": {"name": "1"}, "src": {"name": "3"}},
                        {"dst": {"name": "4"}, "src": {"name": "3"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


EVENT_GRAPH = init_graph2(Graph())
PERSISTENT_GRAPH = init_graph2(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_edges_with_num_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        edgeFilter(filter: {
          src: {
            field: NODE_ID
            where: { eq: { u64: 1 } }
          }
        }) {
          edges {
            list {
              src { name }
              dst { name }
            }
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edgeFilter": {
                "edges": {"list": [{"src": {"name": "1"}, "dst": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
