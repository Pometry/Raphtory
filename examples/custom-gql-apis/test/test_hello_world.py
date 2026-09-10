import tempfile

from raphtory.graphql import GraphServer


def test_hello_world():
    work_dir = tempfile.mkdtemp()
    server = GraphServer(work_dir)
    with server.start() as server:
        client = server.get_client()
        query = """query {
                helloQuery(name:"Shivam")
        }"""
        result = client.query(query)
        assert result["helloQuery"] == "Hello, Shivam"

        query = """mutation {
                helloMutation(name:"Shivam")
        }"""
        result = client.query(query)
        assert result["helloMutation"] == "Hello, Shivam"

        client.new_graph("test_graph", "EVENT").add_edge(0, 1, 2)

        query = """query {
                    graph(path: "test_graph") {
                        algorithm {
                            fancyNodeCount
                        }
                    }
                }"""
        result = client.query(query)
        assert result["graph"]["algorithm"]["fancyNodeCount"] == 2
