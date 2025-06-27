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
                  edgeProps: { all: ALL_CONSTANT }
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
                        "nodeConstProps": ["p1"],
                        "nodeTempProps": ["p1", "q1"],
                        "edgeConstProps": ["p1"],
                        "edgeTempProps": [],
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
                        "edgeConstProps": ["p1"],
                        "edgeTempProps": ["p1", "q1"],
                        "nodeConstProps": ["p1"],
                        "nodeTempProps": ["p1", "q1"],
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
            node_props=PropsInput(all=AllPropertySpec.AllConstant),
            edge_props=PropsInput(
                some=SomePropertySpec(constant=["p1"], temporal=["q1"])
            ),
        )
        client.create_index("g", spec, in_ram=True)

        query = """query {
                     graph(path: "g") {
                       getIndexSpec {
                         nodeConstProps
                         nodeTempProps
                         edgeConstProps
                         edgeTempProps
                       }
                     }
                   }
               """
        assert client.query(query) == {
            "graph": {
                "getIndexSpec": {
                    "edgeConstProps": ["p1"],
                    "edgeTempProps": ["q1"],
                    "nodeConstProps": ["p1"],
                    "nodeTempProps": [],
                }
            }
        }
