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
    server.wait()


def test_server_start_on_default_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    
    graphs = {"g": g}
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graphs=graphs).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    
    server.stop()
    server.wait()


def test_server_start_on_custom_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    
    graphs = {"g": g}
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graphs=graphs).start(port=1737)
    client = RaphtoryClient("http://localhost:1737")

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    server.stop()
    server.wait()
    

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
    server.wait()
    

def test_load_graphs_from_path():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g.save_to_file(tmp_dir + "/g")

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.load_graphs_from_path(tmp_dir)

    query = """{graph(name: "g") {nodes {before(time: 2) {list {name}}}}}"""
    assert client.query(query) == {
        "graph": {"nodes": {"before": {"list": [{"name": "ben"}, {"name": "hamza"}]}}}
    }
    
    server.stop()
    server.wait()
    

def test_graphql2():
    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")
    g2 = Graph()
    g2.add_edge(1, "Naomi", "Shivam")
    g2.add_edge(2, "Shivam", "Pedro")
    g2.add_edge(3, "Pedro", "Rachel")
    graphs = {"g1": g1, "g2": g2}

    g3 = Graph()
    g3.add_edge(1, "ben_saved", "hamza_saved")
    g3.add_edge(2, "haaroon_saved", "hamza_saved")
    g3.add_edge(3, "ben_saved", "haaroon_saved")
    g4 = Graph()
    g4.add_edge(1, "Naomi_saved", "Shivam_saved")
    g4.add_edge(2, "Shivam_saved", "Pedro_saved")
    g4.add_edge(3, "Pedro_saved", "Rachel_saved")
    temp_dir = tempfile.mkdtemp()
    g3.save_to_file(temp_dir + "/g3")
    g4.save_to_file(temp_dir + "/g4")

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graphs=graphs).start(port=1751)
    client = server.get_client()

    query_g1 = """{graph(name: "g1") {nodes {list {name}}}}"""
    query_g1_window = """{graph(name: "g1") {nodes {before(time: 2) {list {name}}}}}"""
    query_g2 = """{graph(name: "g2") {nodes {list {name}}}}"""

    assert client.query(query_g1) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    assert client.query(query_g1_window) == {
        "graph": {"nodes": {"before": {"list": [{"name": "ben"}, {"name": "hamza"}]}}}
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
    server.wait()


def generic_client_test(client, temp_dir):
    # load a graph into the client from a path
    res = client.load_graphs_from_path(temp_dir, True)
    assert res == {"loadGraphsFromPath": ["g1.bincode"]}

    # run a get nodes query and check the results
    query = """query GetNodes($graphname: String!) {
        graph(name: $graphname) {
            nodes {
                list {
                    name
                }
            }
        }
    }"""
    variables = {"graphname": "g1.bincode"}
    res = client.query(query, variables)
    assert res == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    # load a new graph into the client from a path
    multi_graph_temp_dir = tempfile.mkdtemp()
    g2 = Graph()
    g2.add_edge(1, "ben", "hamza")
    g2.add_edge(2, "haaroon", "hamza")
    g2.save_to_file(multi_graph_temp_dir + "/g2.bincode")
    g3 = Graph()
    g3.add_edge(1, "shivam", "rachel")
    g3.add_edge(2, "lucas", "shivam")
    g3.save_to_file(multi_graph_temp_dir + "/g3.bincode")
    res = client.load_graphs_from_path(multi_graph_temp_dir, True)
    result_sorted = {"loadGraphsFromPath": sorted(res["loadGraphsFromPath"])}
    assert result_sorted == {"loadGraphsFromPath": ["g2.bincode", "g3.bincode"]}

    # upload a graph
    g4 = Graph()
    g4.add_node(0, 1)
    res = client.send_graph("hello", g4)
    assert res == {"sendGraph": "hello"}
    # Ensure the sent graph can be queried
    query = """query GetNodes($graphname: String!) {
        graph(name: $graphname) {
            nodes {
                list {
                    name
                }
            }
        }
    }"""
    variables = {"graphname": "hello"}
    res = client.query(query, variables)
    assert res == {"graph": {"nodes": {"list": [{"name": "1"}]}}}
    

def test_graphqlclient():
    temp_dir = tempfile.mkdtemp()

    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")
    graph_path = temp_dir + "/g1.bincode"
    g1.save_to_file(graph_path)

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graph_paths=[graph_path]).start(port=1740)
    client = server.get_client()
    generic_client_test(client, temp_dir)
    server.stop()
    server.wait()

    server = RaphtoryServer(tmp_work_dir, graph_paths=[graph_path]).start(port=1741)
    client = server.get_client()
    generic_client_test(client, temp_dir)
    server.stop()
    server.wait()

    server = RaphtoryServer(tmp_work_dir, graph_paths=[graph_path]).start(port=1742)
    client = RaphtoryClient("http://localhost:1742")
    generic_client_test(client, temp_dir)
    server.stop()
    server.wait()


def test_windows_and_layers():
    g_lotr = graph_loader.lotr_graph()
    g_lotr.add_constant_properties({"name": "lotr"})
    g_layers = Graph()
    g_layers.add_constant_properties({"name": "layers"})
    g_layers.add_edge(1, 1, 2, layer="layer1")
    g_layers.add_edge(1, 2, 3, layer="layer2")
    hm = {"lotr": g_lotr, "layers": g_layers}
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graphs=hm).start()
    client = RaphtoryClient("http://localhost:1736")
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
                    before(time: 300) {
                      history
                    }
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
                "history": [
                  555,
                  562
                ],
                "neighbours": {
                  "list": [
                    {
                      "name": "Gandalf",
                      "before": {
                        "history": [
                          270
                        ]
                      }
                    },
                    {
                      "name": "Bilbo",
                      "before": {
                        "history": [
                          205,
                          270,
                          286
                        ]
                      }
                    }
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
                    layer(name: "layer2") {
                      neighbours {
                        list {
                          name
                        }
                      }
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
          "node": {
            "layer": {
              "name": "1",
              "neighbours": {
                "list": [
                  {
                    "name": "2",
                    "layer": {
                      "neighbours": {
                        "list": [
                          {
                            "name": "3"
                          }
                        ]
                      }
                    }
                  }
                ]
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
    server.wait()


def test_properties():
    g = Graph()
    g.add_constant_properties({"name": "graph"})
    g.add_node(
        1,
        1,
        {
            "prop1": "val1",
            "prop2": "val1",
            "prop3": "val1",
            "prop4": "val1",
        },
    )
    g.add_node(
        2,
        1,
        {
            "prop1": "val2",
            "prop2": "val2",
            "prop3": "val2",
            "prop4": "val2",
        },
    )
    n = g.add_node(
        3,
        1,
        {
            "prop1": "val3",
            "prop2": "val3",
            "prop3": "val3",
            "prop4": "val3",
        },
    )
    n.add_constant_properties(
        {"prop5": "val4", "prop6": "val4", "prop7": "val4", "prop8": "val4"}
    )

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir, graphs={"graph": g}).start()
    client = RaphtoryClient("http://localhost:1736")
    q = """
    query GetEdges {
      graph(name: "graph") {
          nodes {
            list{
              properties {
                values(keys:["prop1","prop2"]){
                  key
                  asString
                }
                temporal{
                  values(keys:["prop3","prop4"]){
                    key
                    history
                  }
                }
                constant{
                  values(keys:["prop4","prop5","prop6"]){
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
                  "values": [
                    {
                      "key": "prop2",
                      "asString": "val3"
                    },
                    {
                      "key": "prop1",
                      "asString": "val3"
                    }
                  ],
                  "temporal": {
                    "values": [
                      {
                        "key": "prop4",
                        "history": [
                          1,
                          2,
                          3
                        ]
                      },
                      {
                        "key": "prop3",
                        "history": [
                          1,
                          2,
                          3
                        ]
                      }
                    ]
                  },
                  "constant": {
                    "values": [
                      {
                        "key": "prop5",
                        "value": "val4"
                      },
                      {
                        "key": "prop6",
                        "value": "val4"
                      }
                    ]
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
    server.wait()
