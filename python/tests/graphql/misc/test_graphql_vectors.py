import tempfile
from time import sleep
from raphtory.graphql import GraphServer, RaphtoryClient
from raphtory import Graph

def embedding(texts):
    return [[text.count("a"), text.count("b")] for text in texts]

def test_embedding():
    result = embedding(texts=["aaa", "b", "ab", "ba"])
    assert result == [[3, 0], [0, 1], [1, 1], [1, 1]]

def setup_graph(g):
    g.update_constant_properties({"name": "abb"})
    g.add_node(1, "aab")
    g.add_edge(1, "aab", "bbb")

def assert_correct_documents(client):
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

def setup_server(work_dir):
    server = GraphServer(work_dir)
    server = server.set_embeddings(
        cache="/tmp/graph-cache",
        embedding=embedding,
        node_template="{{ name }}",
        graph_template="{{ props.name }}"
    )
    return server


def test_new_graph():
    print("test_new_graph")
    work_dir = tempfile.mkdtemp()
    server = setup_server(work_dir)
    with server.start():
        client = RaphtoryClient("http://localhost:1736")
        client.new_graph("abb", "EVENT")
        rg = client.remote_graph("abb")
        setup_graph(rg)
        assert_correct_documents(client)

def test_upload_graph():
    print("test_upload_graph")
    work_dir = tempfile.mkdtemp()
    temp_dir = tempfile.mkdtemp()
    server = setup_server(work_dir)
    with server.start():
        client = RaphtoryClient("http://localhost:1736")
        g = Graph()
        setup_graph(g)
        g_path = temp_dir + "/abb"
        g.save_to_zip(g_path)
        client.upload_graph(path="abb", file_path=g_path, overwrite=True)
        assert_correct_documents(client)

def test_include_graph():
    work_dir = tempfile.mkdtemp()
    g_path = work_dir + "/abb"
    g = Graph()
    setup_graph(g)
    g.save_to_file(g_path)
    server = setup_server(work_dir)
    with server.start():
        client = RaphtoryClient("http://localhost:1736")
        assert_correct_documents(client)

test_upload_graph()
test_include_graph()
