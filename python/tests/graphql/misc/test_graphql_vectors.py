import tempfile
from time import sleep
from raphtory.graphql import GraphServer, RaphtoryClient

def embedding(texts):
    return [[text.count("a"), text.count("b")] for text in texts]

def test_embedding():
    result = embedding(texts=["aaa", "b", "ab", "ba"])
    assert result == [[3, 0], [0, 1], [1, 1], [1, 1]]

def test_add_constant_properties():
    work_dir = tempfile.mkdtemp()
    server = GraphServer(work_dir)
    server = server.set_embeddings(
        cache="/tmp/graph-cache",
        embedding=embedding,
        node_template="{{ name }}",
        graph_template="{{ props.name }}"
    )
    with server.start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("abb", "EVENT")
        rg = client.remote_graph("abb")
        rg.update_constant_properties({"name": "abb"})
        node = rg.add_node(1, "aab")

        query = """{
        plugins {
            globalSearch(query: "aab", limit: 1) {
                entityType
                name
                content
                embedding
            }
        }
        vectorisedGraph(path: "abb") {
            algorithms {
              similaritySearch(query:"ab", limit: 1) {
                content
                entityType
                embedding
                name
              }
            }
          }
        }"""
        result = client.query(query)
        assert result == {
            'plugins': {
                'globalSearch': [
                    {
                        'content': 'abb',
                        'embedding': [1.0, 2.0],
                        'entityType': 'graph',
                        'name': ['abb'],
                    },
                ],
            },
            'vectorisedGraph': {
                'algorithms': {
                    'similaritySearch': [{
                        'content': 'aab',
                        'embedding': [2.0, 1.0],
                        'entityType': 'node',
                        'name': ['aab']}]
                }
            }
        }