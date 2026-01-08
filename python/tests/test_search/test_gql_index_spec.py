import pytest
from raphtory import Graph, PersistentGraph
from filters_setup import init_nodes_graph, init_edges_graph, create_test_graph
from utils import run_group_graphql_test
from raphtory.graphql import (
    GraphServer,
    RaphtoryClient,
    RemoteIndexSpec,
    SomePropertySpec,
    AllPropertySpec,
    PropsInput,
)
import tempfile

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
                  edgeProps: { all: ALL_METADATA }
                }
                inRam: true
              )
            }
            """,
            {"createIndex": True},
        ),
        (
            """
            query {
              graph(path: "g") {
                getIndexSpec {
                  nodeMetadata
                  nodeProperties
                  edgeMetadata
                  edgeProperties
                }
              }
            }
            """,
            {
                "graph": {
                    "getIndexSpec": {
                        "nodeMetadata": ["p1"],
                        "nodeProperties": ["p1", "q1"],
                        "edgeMetadata": ["p1"],
                        "edgeProperties": [],
                    }
                }
            },
        ),
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
            {"createIndex": True},
        ),
        (
            """
            query {
              graph(path: "g") {
                getIndexSpec {
                  nodeMetadata
                  nodeProperties
                  edgeMetadata
                  edgeProperties
                }
              }
            }
            """,
            {
                "graph": {
                    "getIndexSpec": {
                        "edgeMetadata": ["p1"],
                        "edgeProperties": ["p1", "q1"],
                        "nodeMetadata": ["p1"],
                        "nodeProperties": ["p1", "q1"],
                    }
                }
            },
        ),
    ]

    run_group_graphql_test(queries, graph)


@pytest.mark.parametrize("graph", [EVENT_GRAPH, PERSISTENT_GRAPH])
def test_create_index_using_client(graph):

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g", graph=graph)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "N1"},
                        {"name": "N2"},
                        {"name": "N3"},
                        {"name": "N4"},
                        {"name": "N5"},
                        {"name": "N6"},
                        {"name": "N7"},
                        {"name": "N8"},
                        {"name": "N9"},
                        {"name": "N10"},
                        {"name": "N11"},
                        {"name": "N12"},
                        {"name": "N13"},
                        {"name": "N14"},
                        {"name": "N15"},
                    ]
                }
            }
        }

        spec = RemoteIndexSpec(
            node_props=PropsInput(all=AllPropertySpec.AllMetadata),
            edge_props=PropsInput(
                some=SomePropertySpec(metadata=["p1"], properties=["q1"])
            ),
        )
        client.create_index("g", spec, in_ram=True)

        query = """query {
                     graph(path: "g") {
                       getIndexSpec {
                         nodeMetadata
                         nodeProperties
                         edgeMetadata
                         edgeProperties
                       }
                     }
                   }
               """
        assert client.query(query) == {
            "graph": {
                "getIndexSpec": {
                    "edgeMetadata": ["p1"],
                    "edgeProperties": ["q1"],
                    "nodeMetadata": ["p1"],
                    "nodeProperties": [],
                }
            }
        }
