import sys
import tempfile
from raphtory import Graph
from raphtory.graphql import RaphtoryServer, RaphtoryClient


def test_graphql():
    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")

    g2 = Graph()
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

    map_server = RaphtoryServer(graphs=graphs, graph_dir=temp_dir).start(port=1737)

    map_server.wait_for_online()

    query_g1 = """{graph(name: "g1") {nodes {list {name}}}}"""
    query_g1_window = """{graph(name: "g1") {nodes {before(time: 2) {list {name}}}}}"""
    query_g2 = """{graph(name: "g2") {nodes {list {name}}}}"""
    query_g3 = """{graph(name: "g3") {nodes {list {name}}}}"""
    query_g4 = """{graph(name: "g4") {nodes {list {name}}}}"""

    assert map_server.query(query_g1) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    assert map_server.query(query_g1_window) == {
        "graph": {"nodes": {"before": {"list": [{"name": "ben"}, {"name": "hamza"}]}}}
    }
    assert map_server.query(query_g2) == {
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
    assert map_server.query(query_g3) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "ben_saved"},
                    {"name": "hamza_saved"},
                    {"name": "haaroon_saved"},
                ]
            }
        }
    }
    assert map_server.query(query_g4) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "Naomi_saved"},
                    {"name": "Shivam_saved"},
                    {"name": "Pedro_saved"},
                    {"name": "Rachel_saved"},
                ]
            }
        }
    }

    assert map_server.query(query_g1) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    assert map_server.query(query_g2) == {
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
    assert map_server.query(query_g4) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "Naomi_saved"},
                    {"name": "Shivam_saved"},
                    {"name": "Pedro_saved"},
                    {"name": "Rachel_saved"},
                ]
            }
        }
    }
    assert map_server.query(query_g3) == {
        "graph": {
            "nodes": {
                "list": [
                    {"name": "ben_saved"},
                    {"name": "hamza_saved"},
                    {"name": "haaroon_saved"},
                ]
            }
        }
    }

    g5 = Graph()
    g5.add_edge(1, "ben", "hamza")
    g5.add_edge(2, "haaroon", "hamza")
    g5.add_edge(3, "ben", "haaroon")
    g5.save_to_file(temp_dir + "/g1.bincode")

    raphtory_client = RaphtoryClient("http://localhost:1737")
    generic_client_test(raphtory_client, temp_dir)

    map_server.stop()
    map_server.wait()


def generic_client_test(raphtory_client, temp_dir):
    raphtory_client.wait_for_online()

    res = raphtory_client.load_graphs_from_path(temp_dir, overwrite=True)
    expected_result = ["g1.bincode", "g4", "g3"]

    # Extract the array from the res dictionary
    actual_result = sorted(res["loadGraphsFromPath"])

    # Sort the expected result for comparison
    expected_result_sorted = sorted(expected_result)

    # Assert that the actual result matches the sorted expected result
    assert actual_result == expected_result_sorted

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
    res = raphtory_client.query(query, variables)
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
    res = raphtory_client.load_graphs_from_path(multi_graph_temp_dir, overwrite=False)
    result_sorted = {"loadNewGraphsFromPath": sorted(res["loadNewGraphsFromPath"])}
    assert result_sorted == {"loadNewGraphsFromPath": ["g2.bincode", "g3.bincode"]}

    # upload a graph
    g4 = Graph()
    g4.add_node(0, 1)
    res = raphtory_client.send_graph("hello", g4)
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
    res = raphtory_client.query(query, variables)
    assert res == {"graph": {"nodes": {"list": [{"name": "1"}]}}}

