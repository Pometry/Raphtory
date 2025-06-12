import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import create_test_graph
from utils import run_group_graphql_test

EVENT_GRAPH = create_test_graph(Graph())
PERSISTENT_GRAPH = create_test_graph(PersistentGraph())


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
                "graph": {
                    "getIndexSpec": {
                        "nodeConstProps": [],
                        "nodeTempProps": ["prop1", "prop2", "prop3", "prop4", "prop5"],
                        "edgeConstProps": [],
                        "edgeTempProps": [],
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
                "graph": {
                    "getIndexSpec": {
                        "nodeConstProps": [],
                        "nodeTempProps": ["prop1", "prop2", "prop3", "prop4", "prop5"],
                        "edgeConstProps": [],
                        "edgeTempProps": ["eprop1", "eprop2", "eprop3", "eprop4", "eprop5"]
                    }
                }
            }
        )
    ]

    run_group_graphql_test(queries, graph)