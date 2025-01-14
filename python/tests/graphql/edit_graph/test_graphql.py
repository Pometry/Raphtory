import os
import tempfile

import pytest

from raphtory.graphql import (
    GraphServer,
    RaphtoryClient,
    encode_graph,
    decode_graph,
    RemoteGraph,
)
from raphtory import graph_loader
from raphtory import Graph
import json


def normalize_path(path):
    return path.replace("\\", "/")


def test_encode_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    encoded = encode_graph(g)
    decoded_g = decode_graph(encoded)

    assert g == decoded_g


def test_failed_server_start_in_time():
    tmp_work_dir = tempfile.mkdtemp()
    server = None
    try:
        with pytest.raises(Exception) as excinfo:
            server = GraphServer(tmp_work_dir).start(timeout_ms=1)
        assert str(excinfo.value) == "Failed to start server in 1 milliseconds"
    finally:
        if server:
            server.stop()


def test_wrong_url():
    with pytest.raises(Exception) as excinfo:
        client = RaphtoryClient("http://broken_url.com")
    assert (
        str(excinfo.value)
        == "Could not connect to the given server - no response --error sending request for url (http://broken_url.com/)"
    )


def test_successful_server_start_in_time():
    tmp_work_dir = tempfile.mkdtemp()
    server = GraphServer(tmp_work_dir).start(timeout_ms=3000)
    client = server.get_client()
    assert client.is_server_online()
    server.stop()
    assert not client.is_server_online()


def test_server_start_on_default_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
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


def test_server_start_on_custom_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
            }
        }


def test_namespaces():
    def assert_graph_fetch(path):
        query = f"""{{ graph(path: "{path}") {{ nodes {{ list {{ name }} }} }} }}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
            }
        }

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    path = "g"
    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Default namespace, graph is saved in the work dir
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)

        path = "shivam/g"
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)
        assert_graph_fetch(path)

        path = "shivam/investigation/g"
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)
        assert_graph_fetch(path)

        path = "shivam/investigation/2024/12/12/g"
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)
        assert_graph_fetch(path)

        path = "shivam/investigation/2024-12-12/g"
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)
        assert_graph_fetch(path)

        path = "../shivam/g"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "References to the parent dir are not allowed within the path:" in str(
            excinfo.value
        )

        path = "./shivam/g"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "References to the current dir are not allowed within the path" in str(
            excinfo.value
        )

        path = "shivam/../../../../investigation/g"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "References to the parent dir are not allowed within the path:" in str(
            excinfo.value
        )

        path = "//shivam/investigation/g"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "Double forward slashes are not allowed in path" in str(excinfo.value)

        path = "shivam/investigation//2024-12-12/g"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "Double forward slashes are not allowed in path" in str(excinfo.value)

        path = r"shivam/investigation\2024-12-12"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "Backslash not allowed in path" in str(excinfo.value)

        # Test if we can escape through a symlink
        tmp_dir2 = tempfile.mkdtemp()
        nested_dir = os.path.join(tmp_work_dir, "shivam", "graphs")
        os.makedirs(nested_dir)
        symlink_path = os.path.join(nested_dir, "not_a_symlink_i_promise")
        os.symlink(tmp_dir2, symlink_path)

        path = "shivam/graphs/not_a_symlink_i_promise/escaped"
        with pytest.raises(Exception) as excinfo:
            client.send_graph(path=path, graph=g, overwrite=True)
        assert "A component of the given path was a symlink" in str(excinfo.value)


def test_graph_windows_and_layers_query():
    g1 = graph_loader.lotr_graph()
    g1.add_constant_properties({"name": "lotr"})
    g2 = Graph()
    g2.add_constant_properties({"name": "layers"})
    g2.add_edge(1, 1, 2, layer="layer1")
    g2.add_edge(1, 2, 3, layer="layer2")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start() as server:
        client = server.get_client()
        client.send_graph(path="lotr", graph=g1)
        client.send_graph(path="layers", graph=g2)

        q = """
        query GetEdges {
          graph(path: "lotr") {
            window(start: 200, end: 800) {
              node(name: "Frodo") {
                after(time: 500) {
                  history
                  neighbours {
                    list {
                        name
                        before(time: 300) { history }
                    }
                  }
                }
              }
            }
          }
        }
        """
        ra = """
        {
            "graph": {
              "window": {
                "node": {
                  "after": {
                    "history": [555, 562],
                    "neighbours": {
                      "list": [
                        {"name": "Gandalf", "before": {"history": [270]}},
                        {"name": "Bilbo", "before": {"history": [205, 270, 286]}}
                      ]
                    }
                  }
                }
              }
            }
        }
        """
        a = json.dumps(client.query(q))
        json_a = json.loads(a)
        json_ra = json.loads(ra)
        assert json_a == json_ra

        q = """
            query GetEdges {
              graph(path: "layers") {
                node(name: "1") {
                  layer(name: "layer1") {
                    name
                    neighbours {
                      list {
                        name
                        layer(name: "layer2") { neighbours { list { name } } }
                      }
                    }
                  }
                }
              }
            }
        """
        ra = """
        {
            "graph": {
              "node": {
                "layer": {
                  "name": "1",
                  "neighbours": {
                    "list": [{
                        "name": "2",
                        "layer": {"neighbours": {"list": [{ "name": "3" }]}}
                      }]
                  }
                }
              }
            }
        }
          """

        a = json.dumps(client.query(q))
        json_a = json.loads(a)
        json_ra = json.loads(ra)
        assert json_a == json_ra


def test_graph_properties_query():
    g = Graph()
    g.add_constant_properties({"name": "g"})
    g.add_node(1, 1, {"prop1": "val1", "prop2": "val1"})
    g.add_node(2, 1, {"prop1": "val2", "prop2": "val2"})
    n = g.add_node(3, 1, {"prop1": "val3", "prop2": "val3"})
    n.add_constant_properties({"prop5": "val4"})

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        q = """
        query GetEdges {
          graph(path: "g") {
              nodes {
                list {
                  properties {
                    values(keys:["prop1"]) {
                      key
                      asString
                    }
                    temporal {
                      values(keys:["prop2"]) {
                        key
                        history
                      }
                    }
                    constant {
                      values(keys:["prop5"]) {
                        key
                        value
                      }
                    }
                  }
                }
            }
          }
        }
        """
        r = """
        {
            "graph": {
              "nodes": {
                "list": [
                  {
                    "properties": {
                      "values": [{ "key": "prop1", "asString": "val3" }],
                      "temporal": {
                        "values": [{"key": "prop2", "history": [1, 2, 3]}]
                      },
                      "constant": {
                        "values": [{"key": "prop5", "value": "val4"}]
                      }
                    }
                  }
                ]
              }
            }
        }
        """
        s = client.query(q)
        json_a = json.loads(json.dumps(s))
        json_ra = json.loads(r)
        assert sorted(
            json_a["graph"]["nodes"]["list"][0]["properties"]["constant"]["values"],
            key=lambda x: x["key"],
        ) == sorted(
            json_ra["graph"]["nodes"]["list"][0]["properties"]["constant"]["values"],
            key=lambda x: x["key"],
        )
        assert sorted(
            json_a["graph"]["nodes"]["list"][0]["properties"]["values"],
            key=lambda x: x["key"],
        ) == sorted(
            json_ra["graph"]["nodes"]["list"][0]["properties"]["values"],
            key=lambda x: x["key"],
        )
        assert sorted(
            json_a["graph"]["nodes"]["list"][0]["properties"]["temporal"]["values"],
            key=lambda x: x["key"],
        ) == sorted(
            json_ra["graph"]["nodes"]["list"][0]["properties"]["temporal"]["values"],
            key=lambda x: x["key"],
        )


def test_create_node():
    g = Graph()
    g.add_edge(1, "ben", "shivam")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query_nodes) == {
            "graph": {"nodes": {"list": [{"name": "ben"}, {"name": "shivam"}]}}
        }

        create_node_query = """{updateGraph(path: "g") { createNode(time: 0, name: "oogway") {  success } }}"""

        assert client.query(create_node_query) == {
            "updateGraph": {"createNode": {"success": True}}
        }
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "shivam"}, {"name": "oogway"}]
                }
            }
        }

        with pytest.raises(Exception) as excinfo:
            client.query(create_node_query)

        assert "Node already exists" in str(excinfo.value)


def test_create_node_using_client():
    g = Graph()
    g.add_edge(1, "ben", "shivam")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query_nodes) == {
            "graph": {"nodes": {"list": [{"name": "ben"}, {"name": "shivam"}]}}
        }

        remote_graph = client.remote_graph(path="g")
        remote_graph.create_node(timestamp=0, id="oogway")
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "shivam"}, {"name": "oogway"}]
                }
            }
        }

        with pytest.raises(Exception) as excinfo:
            remote_graph.create_node(timestamp=0, id="oogway")

        assert "Node already exists" in str(excinfo.value)


def test_create_node_using_client_with_properties():
    g = Graph()
    g.add_edge(1, "ben", "shivam")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = (
            """{graph(path: "g") {nodes {list {name, properties { keys }}}}}"""
        )
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben", "properties": {"keys": []}},
                        {"name": "shivam", "properties": {"keys": []}},
                    ]
                }
            }
        }

        remote_graph = client.remote_graph(path="g")
        remote_graph.create_node(
            timestamp=0,
            id="oogway",
            properties={
                "prop1": 60,
                "prop2": 31.3,
                "prop3": "abc123",
                "prop4": True,
                "prop5": [1, 2, 3],
            },
        )
        nodes = json.loads(json.dumps(client.query(query_nodes)))["graph"]["nodes"][
            "list"
        ]
        node_oogway = next(node for node in nodes if node["name"] == "oogway")
        assert sorted(node_oogway["properties"]["keys"]) == [
            "prop1",
            "prop2",
            "prop3",
            "prop4",
            "prop5",
        ]

        with pytest.raises(Exception) as excinfo:
            remote_graph.create_node(
                timestamp=0,
                id="oogway",
                properties={
                    "prop1": 60,
                    "prop2": 31.3,
                    "prop3": "abc123",
                    "prop4": True,
                    "prop5": [1, 2, 3],
                },
            )

        assert "Node already exists" in str(excinfo.value)


def test_create_node_using_client_with_properties_node_type():
    g = Graph()
    g.add_edge(1, "ben", "shivam")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = """{graph(path: "g") {nodes {list {name, nodeType, properties { keys }}}}}"""
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben", "nodeType": None, "properties": {"keys": []}},
                        {
                            "name": "shivam",
                            "nodeType": None,
                            "properties": {"keys": []},
                        },
                    ]
                }
            }
        }

        remote_graph = client.remote_graph(path="g")
        remote_graph.create_node(
            timestamp=0,
            id="oogway",
            properties={
                "prop1": 60,
                "prop2": 31.3,
                "prop3": "abc123",
                "prop4": True,
                "prop5": [1, 2, 3],
            },
            node_type="master",
        )
        nodes = json.loads(json.dumps(client.query(query_nodes)))["graph"]["nodes"][
            "list"
        ]
        node_oogway = next(node for node in nodes if node["name"] == "oogway")
        assert node_oogway["nodeType"] == "master"
        assert sorted(node_oogway["properties"]["keys"]) == [
            "prop1",
            "prop2",
            "prop3",
            "prop4",
            "prop5",
        ]

        with pytest.raises(Exception) as excinfo:
            remote_graph.create_node(
                timestamp=0,
                id="oogway",
                properties={
                    "prop1": 60,
                    "prop2": 31.3,
                    "prop3": "abc123",
                    "prop4": True,
                    "prop5": [1, 2, 3],
                },
                node_type="master",
            )

        assert "Node already exists" in str(excinfo.value)


def test_create_node_using_client_with_node_type():
    g = Graph()
    g.add_edge(1, "ben", "shivam")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = """{graph(path: "g") {nodes {list {name, nodeType}}}}"""
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben", "nodeType": None},
                        {"name": "shivam", "nodeType": None},
                    ]
                }
            }
        }

        remote_graph = client.remote_graph(path="g")
        remote_graph.create_node(timestamp=0, id="oogway", node_type="master")
        assert client.query(query_nodes) == {
            "graph": {
                "nodes": {
                    "list": [
                        {"name": "ben", "nodeType": None},
                        {"name": "shivam", "nodeType": None},
                        {"name": "oogway", "nodeType": "master"},
                    ]
                }
            }
        }

        with pytest.raises(Exception) as excinfo:
            remote_graph.create_node(timestamp=0, id="oogway", node_type="master")

        assert "Node already exists" in str(excinfo.value)


def test_edge_id():
    g = Graph()
    g.add_edge(1, "ben", "shivam")
    g.add_edge(2, "oogway", "po")
    g.add_edge(3, "po", "ben")

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query_nodes = """{graph(path: "g") {edges {list {id}}}}"""
        assert client.query(query_nodes) == {
            "graph": {
                "edges": {
                    "list": [
                        {"id": ["ben", "shivam"]},
                        {"id": ["oogway", "po"]},
                        {"id": ["po", "ben"]},
                    ]
                }
            }
        }


# def test_disk_graph_name():
#     import pandas as pd
#     from raphtory import DiskGraphStorage
#     edges = pd.DataFrame(
#         {
#             "src": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
#             "dst": [2, 3, 4, 5, 1, 3, 4, 5, 1, 2, 4, 5, 1, 2, 3, 5, 1, 2, 3, 4],
#             "time": [
#                 10,
#                 20,
#                 30,
#                 40,
#                 50,
#                 60,
#                 70,
#                 80,
#                 90,
#                 100,
#                 110,
#                 120,
#                 130,
#                 140,
#                 150,
#                 160,
#                 170,
#                 180,
#                 190,
#                 200,
#             ],
#         }
#     ).sort_values(["src", "dst", "time"])
#     g= DiskGraphStorage.load_from_pandas(dir, edges, "src", "dst", "time")
#     tmp_work_dir = tempfile.mkdtemp()
#     with GraphServer(tmp_work_dir).start() as server:
#         client = server.get_client()
#         client.upload_graph(path="g", graph=g)
