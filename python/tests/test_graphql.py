import tempfile
from raphtory.graphql import RaphtoryServer, RaphtoryClient
from raphtory import graph_loader
from raphtory import Graph
import json


def test_failed_server_start_in_time():
    tmp_work_dir = tempfile.mkdtemp()
    server = None
    try:
        server = RaphtoryServer(tmp_work_dir).start(timeout_in_milliseconds=1)
    except Exception as e:
        assert str(e) == "Failed to start server in 1 milliseconds"
    finally:
        if server:
            server.stop()


def test_successful_server_start_in_time():
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start(timeout_in_milliseconds=3000)
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
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(name="g", graph=g)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    
    server.stop()


def test_server_start_on_custom_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start(port=1737)
    client = RaphtoryClient("http://localhost:1737")
    client.send_graph(name="g", graph=g)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    server.stop()
    

def test_send_graphs_to_server():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(name="g", graph=g)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    
    server.stop()
    

def test_load_graph():
    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")
    g2 = Graph()
    g2.add_edge(1, "Naomi", "Shivam")
    g2.add_edge(2, "Shivam", "Pedro")
    g2.add_edge(3, "Pedro", "Rachel")

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = server.get_client()
    client.send_graph(name="g1", graph=g1)
    client.send_graph(name="g2", graph=g2)

    g2 = Graph()
    g2.add_edge(1, "shifu", "po")
    g2.add_edge(2, "oogway", "phi")
    g2.add_edge(3, "phi", "po")
    tmp_dir = tempfile.mkdtemp()
    g2_file_path = tmp_dir + "/g2"
    g2.save_to_file(g2_file_path)

    # Since overwrite is False by default, it will not overwrite the existing graph g2 but will instead fail
    try:
        client.load_graph(tmp_dir + "/g2")
    except Exception as e:
        assert "Graph already exists by name = g2" in str(e), f"Unexpected exception message: {e}"

    # Path is not a valid disk graph path
    try:
        client.load_graph(tmp_dir)
    except Exception as e:
        assert "Invalid path" in str(e), f"Unexpected exception message: {e}"

    query_g1 = """{graph(name: "g1") {nodes {list {name}}}}"""
    query_g2 = """{graph(name: "g2") {nodes {list {name}}}}"""
    assert client.query(query_g1) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    assert client.query(query_g2) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "Naomi"},
                    {"name": "Shivam"},
                    {"name": "Pedro"},
                    {"name": "Rachel"},
                ]
            }
        }
    }

    server.stop()


def test_load_graph_overwrite():
    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")
    g2 = Graph()
    g2.add_edge(1, "Naomi", "Shivam")
    g2.add_edge(2, "Shivam", "Pedro")
    g2.add_edge(3, "Pedro", "Rachel")
    tmp_dir = tempfile.mkdtemp()
    g2_file_path = tmp_dir + "/g2"
    g2.save_to_file(g2_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = server.get_client()
    client.send_graph(name="g1", graph=g1)
    client.send_graph(name="g2", graph=g2)

    client.load_graph(tmp_dir + "/g2", True)

    query_g1 = """{graph(name: "g1") {nodes {list {name}}}}"""
    query_g2 = """{graph(name: "g2") {nodes {list {name}}}}"""
    assert client.query(query_g1) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    assert client.query(query_g2) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "Naomi"},
                    {"name": "Shivam"},
                    {"name": "Pedro"},
                    {"name": "Rachel"},
                ]
            }
        }
    }

    server.stop()
    

def test_graph_windows_and_layers_query():
    g1 = graph_loader.lotr_graph()
    g1.add_constant_properties({"name": "lotr"})
    g2 = Graph()
    g2.add_constant_properties({"name": "layers"})
    g2.add_edge(1, 1, 2, layer="layer1")
    g2.add_edge(1, 2, 3, layer="layer2")
    
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = server.get_client()
    client.send_graph(name="lotr", graph=g1)
    client.send_graph(name="layers", graph=g2)

    q = """
    query GetEdges {
      graph(name: "lotr") {
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
          graph(name: "layers") {
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
    
    server.stop()


def test_graph_properties_query():
    g = Graph()
    g.add_constant_properties({"name": "g"})
    g.add_node(1, 1, {"prop1": "val1", "prop2": "val1"})
    g.add_node(2, 1, {"prop1": "val2", "prop2": "val2"})
    n = g.add_node(3, 1, {"prop1": "val3", "prop2": "val3"})
    n.add_constant_properties({"prop5": "val4"})

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = server.get_client()
    client.send_graph(name="g", graph=g)
    q = """
    query GetEdges {
      graph(name: "g") {
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
    server.stop()
