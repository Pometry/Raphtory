import tempfile
from raphtory.graphql import GraphServer, RaphtoryClient
from raphtory import Graph
from raphtory.vectors import OpenAIEmbeddings, embedding_server


@embedding_server(address="0.0.0.0:7340")
def embeddings(text: str):
    return [text.count("a"), text.count("b")]

def setup_graph(g):
    g.add_node(1, "aab")
    g.add_edge(1, "aab", "bbb")


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


def test_new_graph():
    print("test_new_graph")
    work_dir = tempfile.TemporaryDirectory()
    server = GraphServer(work_dir.name)
    with embeddings.start():
        with server.start():
            client = RaphtoryClient("http://localhost:1736")
            client.new_graph("abb", "EVENT")
            rg = client.remote_graph("abb")
            setup_graph(rg)
            client.query("""
                {
                    vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
                }
                """)
            assert_correct_documents(client)


def test_upload_graph():
    print("test_upload_graph")
    work_dir = tempfile.TemporaryDirectory()
    temp_dir = tempfile.TemporaryDirectory()
    server = GraphServer(work_dir.name)
    with embeddings.start():
        with server.start():
            client = RaphtoryClient("http://localhost:1736")
            g = Graph()
            setup_graph(g)
            g_path = temp_dir.name + "/abb"
            g.save_to_zip(g_path)
            client.upload_graph(path="abb", file_path=g_path, overwrite=True)
            client.query("""
                {
                vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
                }
                """)
            assert_correct_documents(client)

GRAPH_NAME = "abb"

def test_include_graph():
    work_dir = tempfile.TemporaryDirectory()
    g_path = work_dir.name + "/" + GRAPH_NAME
    g = Graph()
    setup_graph(g)
    g.save_to_file(g_path)
    server = GraphServer(work_dir.name)
    with embeddings.start():
        embedding_client = OpenAIEmbeddings(api_base="http://localhost:7340")
        server.vectorise_graph(
            name=GRAPH_NAME,
            embeddings=embedding_client,
            nodes="{{ name }}",
            edges=False
        )
        with server.start():
            client = RaphtoryClient("http://localhost:1736")
            assert_correct_documents(client)
