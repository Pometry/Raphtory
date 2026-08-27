"""Shared fixtures for the GraphQL vector tests.

Each module here starts its own mock embedding server on its own port, so the files can be
read — and run — independently.
"""

import threading
import time

from raphtory.vectors import embedding_server


@embedding_server
def embeddings(text: str):
    """Two dimensions: how many a's and b's the document contains."""
    return [text.count("a"), text.count("b")]


EMBEDDING_PORT = 7340


def setup_graph(g):
    g.add_node(1, "aab")
    g.add_edge(1, "aab", "bbb")


def vectorise_query(
    path: str, template: str = "{{ name }}", port: int = EMBEDDING_PORT
) -> str:
    return (
        '{ vectoriseGraph(path: "%s", model: { openAI: { model: "whatever", apiBase: "http://localhost:%d" } }, '
        'nodes: { custom: "%s" }, edges: { enabled: false }) }' % (path, port, template)
    )


def search_query(path: str, query: str, limit: int = 10) -> str:
    return (
        '{ vectorisedGraph(path: "%s") { nodesBySimilarity(query: "%s", limit: %d) '
        "{ getDocuments { content } } } }" % (path, query, limit)
    )


def contents(client, path: str, query: str = "aab") -> list[str]:
    """Document contents a similarity search returns, sorted; empty if there is no index."""
    vg = client.query(search_query(path, query)).get("vectorisedGraph")
    if vg is None:
        return []
    return sorted(d["content"] for d in vg["nodesBySimilarity"]["getDocuments"])


def seed(client, path: str, names: list[str], graph_type: str = "EVENT"):
    remote = client.new_graph(path, graph_type)
    for i, name in enumerate(names, start=1):
        remote.add_node(i, name, {"doc": name})


def query_failed(client, query: str) -> bool:
    """True if the server rejected the query."""
    try:
        client.query(query)
        return False
    except Exception:
        return True


def assert_correct_documents(client):
    query = """{
    vectorisedGraph(path: "abb") {
        entitiesBySimilarity(query: "aab", limit: 1) {
            getDocuments {
                content
                embedding
                entity {
                    __typename
                    ... on Node {
                        name
                    }
                    ... on Edge {
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
    }"""
    result = client.query(query)
    assert result == {
        "vectorisedGraph": {
            "entitiesBySimilarity": {
                "getDocuments": [
                    {
                        "entity": {"__typename": "Node", "name": "aab"},
                        "content": "aab",
                        "embedding": [2.0, 1.0],
                    }
                ]
            }
        },
    }
