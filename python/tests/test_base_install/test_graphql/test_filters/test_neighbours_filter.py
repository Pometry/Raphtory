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
              nodeFilter(filter: {
                and: [
                  {
                    node: {
                      field: NODE_NAME,
                      operator: EQUAL,
                      value:{ str: "d" }
                    }
                  },
                  {
                    property: {
                      name: "prop1"
                      operator: GREATER_THAN
                      value: { i64: 10 }
                    }
                  }
                ]
              }) {
                outNeighbours {
                  list {
                    name
                  }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"nodeFilter": {"outNeighbours": {"list": [{"name": "d"}]}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_out_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "a") {
              nodeFilter(filter: {
                node: {
                  field: NODE_NAME,
                  operator: EQUAL,
                  value:{ str: "e" }
                }
              }) {
                outNeighbours {
                  list {
                    name
                  }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"nodeFilter": {"outNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              nodeFilter(filter: {
                    property: {
                      name: "prop1"
                      operator: GREATER_THAN
                      value: { i64: 10 }
                    }
              }) {
                inNeighbours {
                  list {
                    name
                  }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"nodeFilter": {"inNeighbours": {"list": [{'name': 'a'}, {'name': 'c'}]}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_in_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              nodeFilter(filter: {
                node: {
                  field: NODE_NAME,
                  operator: EQUAL,
                  value:{ str: "e" }
                }
              }) {
                inNeighbours {
                  list {
                    name
                  }
                }
              }
            }
          }
        }
    """
    expected_output = {"graph": {"node": {"nodeFilter": {"inNeighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              nodeFilter(filter: {
                node: {
                  field: NODE_NAME,
                  operator: NOT_EQUAL,
                  value:{ str: "a" }
                }
              }) {
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
    expected_output = {"graph": {"node": {"nodeFilter": {"neighbours": {"list": [{'name': 'b'}, {'name': 'c'}]}}}}}
    run_graphql_test(query, expected_output, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_neighbours_not_found(graph):
    query = """
        query {
          graph(path: "g") {
            node(name: "d") {
              nodeFilter(filter: {
                node: {
                  field: NODE_NAME,
                  operator: EQUAL,
                  value:{ str: "e" }
                }
              }) {
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
    expected_output = {"graph": {"node": {"nodeFilter": {"neighbours": {"list": []}}}}}
    run_graphql_test(query, expected_output, graph)
