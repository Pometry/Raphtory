import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_graph, init_graph2, init_graph4
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = init_graph(Graph())
PERSISTENT_GRAPH = init_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_filter_edges_with_str_ids_for_node_id_eq_gql(graph):
    query = """
    query {
      graph(path: "g") {
        filterEdges(expr: {
          src: {
            node: {
              field: NODE_ID
            	where: { eq: { str: "3" } }
            }
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
            "filterEdges": {
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
        filterEdges(expr: {
          src: {
            node: {
              field: NODE_ID
              where: { eq: { u64: 1 } }
            }
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
            "filterEdges": {
                "edges": {"list": [{"src": {"name": "1"}, "dst": {"name": "2"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_chained_selection_with_edge_filter(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          select(expr: { dst: { 
            node: {
              field: NODE_ID
              where: { eq: { u64: 2 } }
            }
          } }) {
            select(expr: { property: { name: "p2", where: { gt:{ i64: 2 } } } }) {
              list { src { name } dst { name } }
            }        
          }
        }
      }
    }
    """
    expected_output = {
        "graph": {
            "edges": {
                "select": {
                    "select": {"list": [{"dst": {"name": "2"}, "src": {"name": "1"}}]}
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


PERSISTENT_GRAPH = init_graph4("dummy_graph")


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_edges_filter_window_is_active(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          select(expr: {window: {start: 1, end: 4, expr: {isActive: true}}}) {
            list {
              src {
                name
              }
              dst {
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
            "edges": {
                "select": {
                    "list": [
                        {"dst": {"name": "2"}, "src": {"name": "1"}},
                        {"dst": {"name": "3"}, "src": {"name": "2"}},
                        {"dst": {"name": "1"}, "src": {"name": "3"}},
                        {"dst": {"name": "4"}, "src": {"name": "3"}},
                        {"dst": {"name": "1"}, "src": {"name": "2"}},
                    ]
                }
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [PERSISTENT_GRAPH])
def test_edges_filter_window_is_deleted(graph):
    query = """
    query {
      graph(path: "g") {
        edges {
          select(expr: {window: {start: 1, end: 5, expr: {isDeleted: true}}}) {
            list {
              src {
                name
              }
              dst {
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
            "edges": {
                "select": {"list": [{"dst": {"name": "4"}, "src": {"name": "3"}}]}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)
