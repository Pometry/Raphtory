import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_graphql_test, run_graphql_error_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              filter(expr: {
                and: [
                  {
                    node: {
                      field: NODE_NAME,
                      where: { eq: { str: "d" } }
                    }
                  },
                  {
                    property: {
                      name: "prop1"
                      where: { gt: { i64: 10 } }
                    }
                  }
                ]
              }) {
                outNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {"node": {"filter": {"outNeighbours": {"list": [{"name": "d"}]}}}}
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              filter(expr: {
                node: {
                  field: NODE_NAME,
                  where: { eq: { str: "e" } }
                }
              }) {
                outNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"outNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                property: {
                  name: "prop1"
                  where: { gt: { i64: 10 } }
                }
              }) {
                inNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "node": {
                "filter": {"inNeighbours": {"list": [{"name": "a"}, {"name": "c"}]}}
            }
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                node: {
                  field: NODE_NAME,
                  where: { eq: { str: "e" } }
                }
              }) {
                inNeighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"inNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                node: {
                  field: NODE_NAME,
                  where: { ne: { str: "a" } }
                }
              }) {
                neighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {
        "graph": {
            "node": {"filter": {"neighbours": {"list": [{"name": "b"}, {"name": "c"}]}}}
        }
    }
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              filter(expr: {
                node: {
                  field: NODE_NAME,
                  where: { eq: { str: "e" } }
                }
              }) {
                neighbours {
                  list { name }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"filter": {"neighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)
