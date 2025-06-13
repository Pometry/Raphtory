import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_nodes_graph, init_edges_graph
from utils import run_group_graphql_test

EVENT_GRAPH = init_nodes_graph(Graph())
PERSISTENT_GRAPH = init_nodes_graph(PersistentGraph())


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_create_index_with_custom_spec(graph):
    queries = [
        (
            """
            mutation {
              createIndex(
                path: "g"
                indexSpec: {
                  nodeProps: { all: ALL }
                  edgeProps: { all: ALL_CONSTANT }
                }
                inRam: true
              )
            }
            """,
            {"createIndex": True}
        ),
        (
            """
            query {
              graph(path: "g") {
                getIndexSpec {
                  nodeConstProps
                  nodeTempProps
                  edgeConstProps
                  edgeTempProps
                }
              }
            }
            """,
            {
                'graph': {
                    'getIndexSpec': {
                        'nodeConstProps': ['p1'],
                        'nodeTempProps': ['p1', 'q1'],
                        'edgeConstProps': ['p1'],
                        'edgeTempProps': [],
                    }
                }
            }
        )
    ]

    run_group_graphql_test(queries, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_create_index_with_default_spec(graph):
    queries = [
        (
            """
            mutation {
              createIndex(
                path: "g"
                inRam: true
              )
            }
            """,
            {"createIndex": True}
        ),
        (
            """
            query {
              graph(path: "g") {
                getIndexSpec {
                  nodeConstProps
                  nodeTempProps
                  edgeConstProps
                  edgeTempProps
                }
              }
            }
            """,
            {
                'graph': {
                    'getIndexSpec': {
                        'edgeConstProps': ['p1'],
                        'edgeTempProps': ['p1', 'q1'],
                        'nodeConstProps': ['p1'],
                        'nodeTempProps': ['p1', 'q1']
                    }
                }
            }
        )
    ]

    run_group_graphql_test(queries, graph)