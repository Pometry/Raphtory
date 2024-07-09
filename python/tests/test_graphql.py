import os
import tempfile
import time

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
    

def test_send_graph_to_server():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    name = "g"
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(name=name, graph=g)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    try:
        client.send_graph(name=name, graph=g)
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    client.send_graph(name=name, graph=g, overwrite=True)
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    namespace = "shivam"
    client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    expected_path = os.path.join(tmp_work_dir, namespace, name)
    assert os.path.exists(expected_path)

    server.stop()


def test_send_graph_to_server_with_namespace():
    def assert_graph_fetch(name, namespace):
        query = f"""{{ graph(name: "{name}", namespace: "{namespace}") {{ nodes {{ list {{ name }} }} }} }}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    name = "g"
    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Default namespace, graph is saved in the work dir
    client.send_graph(name=name, graph=g, overwrite=True)
    expected_path = os.path.join(tmp_work_dir, name)
    assert os.path.exists(expected_path)

    namespace = "shivam"
    client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    expected_path = os.path.join(tmp_work_dir, namespace, name)
    assert os.path.exists(expected_path)
    assert_graph_fetch(name, namespace)

    namespace = "./shivam/investigation"
    client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    expected_path = os.path.join(tmp_work_dir, namespace, name)
    assert os.path.exists(expected_path)
    assert_graph_fetch(name, namespace)

    namespace = "./shivam/investigation/2024/12/12"
    client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    expected_path = os.path.join(tmp_work_dir, namespace, name)
    assert os.path.exists(expected_path)
    assert_graph_fetch(name, namespace)

    namespace = "shivam/investigation/2024-12-12"
    client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    expected_path = os.path.join(tmp_work_dir, namespace, name)
    assert os.path.exists(expected_path)
    assert_graph_fetch(name, namespace)

    namespace = "../shivam"
    try:
        client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    except Exception as e:
        assert "Invalid namespace: ../shivam" in str(e), f"Unexpected exception message: {e}"

    namespace = "./shivam/../investigation"
    try:
        client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    except Exception as e:
        assert "Invalid namespace: ./shivam/../investigation" in str(e), f"Unexpected exception message: {e}"

    namespace = "//shivam/investigation"
    try:
        client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    except Exception as e:
        assert "//shivam/investigation" in str(e), f"Unexpected exception message: {e}"

    namespace = "shivam/investigation//2024-12-12"
    try:
        client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    except Exception as e:
        assert "shivam/investigation//2024-12-12" in str(e), f"Unexpected exception message: {e}"

    namespace = "shivam/investigation\2024-12-12"
    try:
        client.send_graph(name=name, graph=g, overwrite=True, namespace=namespace)
    except Exception as e:
        assert "shivam/investigation\2024-12-12" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_upload_graph_to_server():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.upload_graph(name="g", file_path=g_file_path, overwrite=False)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    try:
        client.upload_graph(name="g", file_path=g_file_path)
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    client.upload_graph(name="g", file_path=g_file_path, overwrite=True)
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


def test_get_graph():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ graph(name: "g1") { name, path, nodes { list { name } } } }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    query = """{ graph(name: "g1", namespace: "shivam") { name, path, nodes { list { name } } } }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    query = """{ graph(name: "g1") { name, path, nodes { list { name } } } }"""
    assert client.query(query) == {'graph': {'name': 'g1', 'nodes': {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}, 'path': 'g1'}}

    query = """{ graph(name: "g2", namespace: "shivam") { name, path, nodes { list { name } } } }"""
    assert client.query(query) == {'graph': {'name': 'g2', 'nodes': {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}, 'path': 'shivam/g2'}}

    server.stop()


def test_get_graphs():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    # Assert if no graphs are discoverable
    query = """{ graphs { name, path } }"""
    assert client.query(query) == {
        'graphs': {'name': [], 'path': []}
    }

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    # Assert if all graphs present in the work_dir are discoverable
    query = """{ graphs { name, path } }"""
    assert client.query(query) == {
        'graphs': {'name': ['g1', 'g2', 'g3'], 'path': ['g1', 'shivam/g2', 'shivam/g3']}
    }

    server.stop()


def test_receive_graph():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ receiveGraph(name: "g2") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g2" in str(e), f"Unexpected exception message: {e}"

    query = """{ receiveGraph(name: "g2", namespace: "shivam") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g2" in str(e), f"Unexpected exception message: {e}"

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    received_graph = 'AAAAAAMAAAAAAAAAlNcECxwg2OUBAAAAAAAAAO_T5CcRykTMAgAAAAAAAACtSFF47MOYUAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAEAAAAAAAAArUhReOzDmFABAwAAAAAAAABiZW4AAAAAAAAAAAIAAAACAAAAAAAAAAEAAAAAAAAAAwAAAAAAAAABAAAAAAAAAAEAAAACAAAAAgAAAAAAAAABAAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAlNcECxwg2OUBBQAAAAAAAABoYW16YQEAAAAAAAAAAgAAAAIAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAACAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAADv0-QnEcpEzAEHAAAAAAAAAGhhYXJvb24CAAAAAAAAAAIAAAACAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAABAAAAAAAAAAEAAAABAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAACAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAEAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAAAQAAAAAAAAABAAAAAgAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAABAAAAAAAAAAEAAAADAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwAAAAAAAAAJAAAAAAAAAAEAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQBAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'

    query = """{ receiveGraph(name: "g1") }"""
    assert client.query(query) == {
        'receiveGraph': received_graph
    }

    query = """{ receiveGraph(name: "g2", namespace: "shivam") }"""
    assert client.query(query) == {
        'receiveGraph': received_graph
    }

    server.stop()


def test_rename_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))
    
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if all graphs present in the work_dir are discoverable
    query = """{graph(name: "g1") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    query = """{graph(name: "g2", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }
    query = """{graph(name: "g3", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    # Assert if attempt to rename to an existing graph fails
    query = """mutation {
      renameGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g3",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = shivam/g3" in str(e), f"Unexpected exception message: {e}"
        
    # Assert if rename graph succeeds and old graph is deleted, when parent graph belongs to different namespace
    query = """mutation {
      renameGraph(
        parentGraphName: "g1",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g4",
      )
    }"""
    client.query(query)
    
    query = """{graph(name: "g3", namespace: "shivam") {nodes {list {name}}}}"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g3" in str(e), f"Unexpected exception message: {e}"
        
    query = """{graph(name: "g4", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    # Assert if rename graph succeeds and old graph is deleted, when parent graph belongs to same namespace
    query = """mutation {
      renameGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g4",
        graphNamespace: "shivam",
        newGraphName: "g5",
      )
    }"""
    client.query(query)

    query = """{graph(name: "g4", namespace: "shivam") {nodes {list {name}}}}"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g4" in str(e), f"Unexpected exception message: {e}"

    query = """{graph(name: "g5", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    # Assert if rename graph fails when pointing to incorrect graphNamespace
    query = """mutation {
      renameGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"

    # Assert if rename graph fails when pointing to incorrect parentGraphNamespace
    query = """mutation {
      renameGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "ben",
        graphName: "g5",
        graphNamespace: "shivam",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g2" in str(e), f"Unexpected exception message: {e}"
    
    server.stop()


def test_save_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    # work_dir = "/tmp/graphs"
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    # Assert if attempt to "save as" to an existing graph fails
    query = """mutation {
        saveGraph(
          parentGraphName: "g1",
          graphName: "g2",
          graphNamespace: "shivam",
          newGraphName: "g3",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: "{ \\"ben\\": {} }"
        )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = shivam/g3" in str(e), f"Unexpected exception message: {e}"

    # Assert if "save as" graph succeeds, when parent graph belongs to different namespace
    query = """mutation {
      saveGraph(
        parentGraphName: "g1",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g4",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: "{ \\"ben\\": {} }"
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g4", namespace: "shivam") { 
            nodes { list { name } }
            properties { constant {
              creationTime: get(key: "creationTime") { value }
              lastUpdated: get(key: "lastUpdated") { value }
              lastOpened: get(key: "lastOpened") { value }
              uiProps: get(key: "uiProps") { value }
              isArchive: get(key: "isArchive") { value }
            }}
        }
    }"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}]
    assert result['graph']['properties']['constant']['creationTime']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    # Assert if "save as" graph succeeds, when parent graph belongs to same namespace
    query = """mutation {
      saveGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g5",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: "{ \\"ben\\": {} }"
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g4", namespace: "shivam") { 
            nodes { list { name } }
            properties { constant {
              creationTime: get(key: "creationTime") { value }
              lastUpdated: get(key: "lastUpdated") { value }
              lastOpened: get(key: "lastOpened") { value }
              uiProps: get(key: "uiProps") { value }
              isArchive: get(key: "isArchive") { value }
            }}
        }
    }"""
    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}]
    assert result['graph']['properties']['constant']['creationTime']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_save_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    # Assert if "save" graph succeeds, when parent graph belongs to different namespace
    query = """{
        graph(name: "g3", namespace: "shivam") { 
            nodes { list { name } }
            properties { constant {
              creationTime: get(key: "creationTime") { value }
              lastUpdated: get(key: "lastUpdated") { value }
              lastOpened: get(key: "lastOpened") { value }
              uiProps: get(key: "uiProps") { value }
              isArchive: get(key: "isArchive") { value }
            }}
        }
    }"""
    result = client.query(query)
    # Assertions pre-save graph
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['creationTime'] is None
    assert result['graph']['properties']['constant']['lastOpened'] is None
    assert result['graph']['properties']['constant']['lastUpdated'] is None
    assert result['graph']['properties']['constant']['uiProps'] is None
    assert result['graph']['properties']['constant']['isArchive'] is None

    save_graph = """mutation {
      saveGraph(
        parentGraphName: "g1",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: "{ \\"ben\\": {}, \\"haaroon\\": {} }"
      )
    }"""
    client.query(save_graph)

    result = client.query(query)
    # Assertions post-save graph
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['creationTime'] is None   # Since it is an existing graph
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    # Assert if "save" graph succeeds, when parent graph belongs to same namespace
    save_graph = """mutation {
      saveGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: "{ \\"ben\\": {} }"
      )
    }"""
    client.query(save_graph)

    query = """{
        graph(name: "g3", namespace: "shivam") { 
            nodes { list { name } }
            properties { constant {
              creationTime: get(key: "creationTime") { value }
              lastUpdated: get(key: "lastUpdated") { value }
              lastOpened: get(key: "lastOpened") { value }
              uiProps: get(key: "uiProps") { value }
              isArchive: get(key: "isArchive") { value }
            }}
        }
    }"""
    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}]
    assert result['graph']['properties']['constant']['creationTime'] is None   # Since it is an existing graph
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    # # Assert if rename graph fails when pointing to incorrect graphNamespace
    # query = """mutation {
    #   renameGraph(
    #     parentGraphName: "g2",
    #     parentGraphNamespace: "shivam",
    #     graphName: "g5",
    #     graphNamespace: "ben",
    #     newGraphName: "g6",
    #   )
    # }"""
    # try:
    #     client.query(query)
    # except Exception as e:
    #     assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"
    #
    # # Assert if rename graph fails when pointing to incorrect parentGraphNamespace
    # query = """mutation {
    #   renameGraph(
    #     parentGraphName: "g2",
    #     parentGraphNamespace: "ben",
    #     graphName: "g5",
    #     graphNamespace: "shivam",
    #     newGraphName: "g6",
    #   )
    # }"""
    # try:
    #     client.query(query)
    # except Exception as e:
    #     assert "Graph not found ben/g2" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_last_opened():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { updateGraphLastOpened(graphName: "g1") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    query = """mutation { updateGraphLastOpened(graphName: "g1", namespace: "shivam") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    query_last_opened = """{ graph(name: "g1") { properties { constant { get(key: "lastOpened") { value } } } } }"""
    mutate_last_opened = """mutation { updateGraphLastOpened(graphName: "g1") }"""
    assert client.query(query_last_opened) == {'graph': {'properties': {'constant': {'get': None}}}}
    assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
    updated_last_opened1 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
    time.sleep(1)
    assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
    updated_last_opened2 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
    assert updated_last_opened2 > updated_last_opened1

    query_last_opened = """{ graph(name: "g2", namespace: "shivam") { properties { constant { get(key: "lastOpened") { value } } } } }"""
    mutate_last_opened = """mutation { updateGraphLastOpened(graphName: "g2", namespace: "shivam") }"""
    assert client.query(query_last_opened) == {'graph': {'properties': {'constant': {'get': None}}}}
    assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
    updated_last_opened1 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
    time.sleep(1)
    assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
    updated_last_opened2 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
    assert updated_last_opened2 > updated_last_opened1

    server.stop()


def test_archive_graph():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { archiveGraph(graphName: "g1", isArchive: 0) }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    query = """mutation { archiveGraph(graphName: "g1", namespace: "shivam", isArchive: 0) }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    query_is_archive = """{ graph(name: "g1") { properties { constant { get(key: "isArchive") { value } } } } }"""
    assert client.query(query_is_archive) == {'graph': {'properties': {'constant': {'get': None}}}}
    update_archive_graph = """mutation { archiveGraph(graphName: "g1", isArchive: 0) }"""
    assert client.query(update_archive_graph) == {"archiveGraph": True}
    assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 0
    update_archive_graph = """mutation { archiveGraph(graphName: "g1", isArchive: 1) }"""
    assert client.query(update_archive_graph) == {"archiveGraph": True}
    assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 1

    query_is_archive = """{ graph(name: "g2", namespace: "shivam") { properties { constant { get(key: "isArchive") { value } } } } }"""
    assert client.query(query_is_archive) == {'graph': {'properties': {'constant': {'get': None}}}}
    update_archive_graph = """mutation { archiveGraph(graphName: "g2", namespace: "shivam", isArchive: 0) }"""
    assert client.query(update_archive_graph) == {"archiveGraph": True}
    assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 0
    update_archive_graph = """mutation { archiveGraph(graphName: "g2", namespace: "shivam", isArchive: 1) }"""
    assert client.query(update_archive_graph) == {"archiveGraph": True}
    assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 1

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
