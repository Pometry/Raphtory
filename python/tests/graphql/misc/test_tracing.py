import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

# TODO this doesn't work currently due to how we terminate with Tracing
def test_server_start_on_default_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir,tracing=True).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g", graph=g)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
            }
        }
    with GraphServer(tmp_work_dir,tracing=True).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g2", graph=g)

        query = """{graph(path: "g2") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
            }
        }
