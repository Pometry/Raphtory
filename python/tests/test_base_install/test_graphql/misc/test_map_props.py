import tempfile
from raphtory.graphql import GraphServer, RaphtoryClient
from raphtory import Graph

TEST_PROPS = {
    "number": 1,
    "string": "text",
    "numbers": [1, 14],
    "strings": ["a", "text"],
    "map": {"a": 1},
}


def test_map_props():
    work_dir = tempfile.mkdtemp()
    server = server = GraphServer(work_dir)
    with server.start():
        temp_dir = tempfile.mkdtemp()
        client = RaphtoryClient("http://localhost:1736")
        g = Graph()
        g.update_constant_properties({"test": TEST_PROPS})
        node = g.add_node(0, "test")
        node.add_constant_properties({"test": TEST_PROPS})
        g_path = temp_dir + "/test"
        g.save_to_zip(g_path)
        client.upload_graph(path="test", file_path=g_path, overwrite=True)
        check_test_prop(client)

    work_dir = tempfile.mkdtemp()
    server = server = GraphServer(work_dir)
    with server.start():
        client.new_graph("test", "EVENT")
        rg = client.remote_graph("test")
        rg.update_constant_properties({"test": TEST_PROPS})
        node = rg.add_node(0, "test")
        node.add_constant_properties({"test": TEST_PROPS})
        check_test_prop(client)


def check_test_prop(client: RaphtoryClient):
    query = """{
        graph(path: "test") {
            properties {
                get(key: "test") {
                    value
                }
            }
            node(name: "test") {
                properties {
                    get(key: "test") {
                        value
                    }
                }
            }
        }
    }"""
    result = client.query(query)
    assert result["graph"]["properties"]["get"]["value"] == TEST_PROPS
    assert result["graph"]["node"]["properties"]["get"]["value"] == TEST_PROPS
