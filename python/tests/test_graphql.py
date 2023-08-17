import sys
import tempfile

def test_graphQL():
    from raphtory import Graph
    from raphtory import graphqlserver
    import random
    import string
    import os

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

    map_server = graphqlserver.run_server(graphs=graphs, port=1736, daemon=True)
    dir_server = graphqlserver.run_server(graph_dir=temp_dir, port=1737, daemon=True)
    map_dir_server = graphqlserver.run_server(graphs=graphs, graph_dir=temp_dir, port=1738, daemon=True)

    query_g1 = """{graph(name: "g1") {nodes {name}}}"""
    query_g2 = """{graph(name: "g2") {nodes {name}}}"""
    query_g3 = """{graph(name: "g3") {nodes {name}}}"""
    query_g4 = """{graph(name: "g4") {nodes {name}}}"""

    assert str(map_server.query(query_g1)).replace(" ",
                                                   "") == "{'graph': {'nodes': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}}".replace(
        " ", "")
    assert str(map_server.query(query_g2)).replace(" ",
                                                   "") == "{'graph': {'nodes': [{'name': 'Naomi'}, {'name': 'Shivam'}, {'name': 'Pedro'}, {'name': 'Rachel'}]}}".replace(
        " ", "")
    assert str(dir_server.query(query_g3)).replace(" ",
                                                   "") == "{'graph': {'nodes': [{'name': 'ben_saved'}, {'name': 'hamza_saved'}, {'name': 'haaroon_saved'}]}}".replace(
        " ", "")
    assert str(dir_server.query(query_g4)).replace(" ",
                                                   "") == "{'graph': {'nodes': [{'name': 'Naomi_saved'}, {'name': 'Shivam_saved'}, {'name': 'Pedro_saved'}, {'name': 'Rachel_saved'}]}}".replace(
        " ", "")

    assert str(map_dir_server.query(query_g1)).replace(" ",
                                                       "") == "{'graph': {'nodes': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}}".replace(
        " ", "")
    assert str(map_dir_server.query(query_g2)).replace(" ",
                                                       "") == "{'graph': {'nodes': [{'name': 'Naomi'}, {'name': 'Shivam'}, {'name': 'Pedro'}, {'name': 'Rachel'}]}}".replace(
        " ", "")
    assert str(map_dir_server.query(query_g4)).replace(" ",
                                                       "") == "{'graph': {'nodes': [{'name': 'Naomi_saved'}, {'name': 'Shivam_saved'}, {'name': 'Pedro_saved'}, {'name': 'Rachel_saved'}]}}".replace(
        " ", "")
    assert str(map_dir_server.query(query_g3)).replace(" ",
                                                       "") == "{'graph': {'nodes': [{'name': 'ben_saved'}, {'name': 'hamza_saved'}, {'name': 'haaroon_saved'}]}}".replace(
        " ", "")


def test_graphqlclient():
    from raphtory import Graph
    from raphtory import graphqlserver
    from raphtory import graphqlclient
    import os

    temp_dir = tempfile.mkdtemp()

    g1 = Graph()
    g1.add_edge(1, "ben", "hamza")
    g1.add_edge(2, "haaroon", "hamza")
    g1.add_edge(3, "ben", "haaroon")
    g1.save_to_file(temp_dir + "/g1.bincode")

    dir_server = graphqlserver.run_server(graph_dir=temp_dir, port=1737, daemon=True)

    # create a client 
    raphtory_client = graphqlclient.RaphtoryGraphQLClient(url="http://localhost:1737/")

    # load a graph into the client from a path
    res = raphtory_client.load_graphs_from_path(temp_dir)
    assert res == {'loadGraphsFromPath': ['g1.bincode']}

    # run a get nodes query and check the results
    query = """query GetNodes($graphname: String!) {
        graph(name: $graphname) {
            nodes {
            name
            }
        }
    }"""
    variables = {
        "graphname": "g1.bincode"
    }
    res = raphtory_client.query(query, variables)
    assert res == {'graph': {'nodes': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}}
    
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
    res = raphtory_client.load_new_graphs_from_path(multi_graph_temp_dir)
    result_sorted = {'loadNewGraphsFromPath': sorted(res['loadNewGraphsFromPath']) }
    assert result_sorted == {'loadNewGraphsFromPath': ['g2.bincode', 'g3.bincode']}

    # upload a graph
    g4 = Graph()
    g4.add_vertex(0, 1)
    res = raphtory_client.send_graph("hello", g4)
    assert res == { "sendGraph": "hello" }
    # Ensure the sent graph can be queried 
    query = """query GetNodes($graphname: String!) {
        graph(name: $graphname) {
            nodes {
                name
            }
        }
    }"""
    variables = {
        "graphname": "hello"
    }
    res = raphtory_client.query(query, variables)
    assert res == {'graph': {'nodes': [{'name': '1'}]}}
