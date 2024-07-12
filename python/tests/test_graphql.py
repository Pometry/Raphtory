import base64
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


def test_send_graph_succeeds_if_no_graph_found_with_same_name():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(name="g", graph=g)

    server.stop()


def test_send_graph_fails_if_graph_already_exists():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.send_graph(name="g", graph=g)
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_send_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    client.send_graph(name="g", graph=g, overwrite=True)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


def test_send_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")
    client.send_graph(name="g", graph=g, namespace="shivam")

    server.stop()


def test_send_graph_fails_if_graph_already_exists_at_namespace():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.send_graph(name="g", graph=g, namespace="shivam")
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_send_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    client.send_graph(name="g", graph=g, overwrite=True, namespace="shivam")

    query = """{graph(name: "g", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


def test_namespaces():
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


# Test upload graph
def test_upload_graph_succeeds_if_no_graph_found_with_same_name():
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

    server.stop()


def test_upload_graph_fails_if_graph_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(tmp_work_dir, "g"))
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.upload_graph(name="g", file_path=g_file_path)
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_upload_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
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

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    client.upload_graph(name="g", file_path=g_file_path, overwrite=True)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


# Test upload graph at namespace
def test_upload_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
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
    client.upload_graph(name="g", file_path=g_file_path, overwrite=False, namespace="shivam")

    query = """{graph(name: "g", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    server.stop()


def test_upload_graph_fails_if_graph_already_exists_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.upload_graph(name="g", file_path=g_file_path, overwrite=False, namespace="shivam")
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_upload_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    client.upload_graph(name="g", file_path=g_file_path, overwrite=True, namespace="shivam")

    query = """{graph(name: "g", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


def test_load_graph_succeeds_if_no_graph_found_with_same_name():
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
    client.load_graph(file_path=g_file_path, overwrite=False)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    server.stop()


def test_load_graph_fails_if_graph_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(tmp_work_dir, "g"))
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.load_graph(file_path=g_file_path)
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_load_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
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

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    client.load_graph(file_path=g_file_path, overwrite=True)

    query = """{graph(name: "g") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


# Test load graph at namespace
def test_load_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
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
    client.load_graph(file_path=g_file_path, overwrite=False, namespace="shivam")

    query = """{graph(name: "g", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
        }
    }

    server.stop()


def test_load_graph_fails_if_graph_already_exists_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))
    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    try:
        client.load_graph(file_path=g_file_path, overwrite=False, namespace="shivam")
    except Exception as e:
        assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_load_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    server = RaphtoryServer(tmp_work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    client.load_graph(file_path=g_file_path, overwrite=True, namespace="shivam")

    query = """{graph(name: "g", namespace: "shivam") {nodes {list {name}}}}"""
    assert client.query(query) == {
        "graph": {
            "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
        }
    }

    server.stop()


def test_get_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ graph(name: "g1") { name, path, nodes { list { name } } } }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_get_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ graph(name: "g1", namespace: "shivam") { name, path, nodes { list { name } } } }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_get_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))

    query = """{ graph(name: "g1") { name, path, nodes { list { name } } } }"""
    assert client.query(query) == {
        'graph': {'name': 'g1', 'nodes': {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]},
                  'path': 'g1'}}

    server.stop()


def test_get_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    query = """{ graph(name: "g2", namespace: "shivam") { name, path, nodes { list { name } } } }"""
    assert client.query(query) == {
        'graph': {'name': 'g2', 'nodes': {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]},
                  'path': 'shivam/g2'}}

    server.stop()


def test_get_graphs_returns_emtpy_list_if_no_graphs_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    # Assert if no graphs are discoverable
    query = """{ graphs { name, path } }"""
    assert client.query(query) == {
        'graphs': {'name': [], 'path': []}
    }

    server.stop()


def test_get_graphs_returns_graph_list_if_graphs_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

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


def test_receive_graph_fails_if_no_graph_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ receiveGraph(name: "g2") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g2" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_receive_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    g.save_to_file(os.path.join(work_dir, "g1"))

    received_graph = 'AQAAAAAAAAADAAAAAAAAAJTXBAscINjlAQAAAAAAAADv0+QnEcpEzAIAAAAAAAAArUhReOzDmFAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABAAAAAAAAAK1IUXjsw5hQAQMAAAAAAAAAYmVuAAAAAAAAAAACAAAAAgAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAQAAAAAAAAABAAAAAgAAAAIAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAJTXBAscINjlAQUAAAAAAAAAaGFtemEBAAAAAAAAAAIAAAACAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAgAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAA79PkJxHKRMwBBwAAAAAAAABoYWFyb29uAgAAAAAAAAACAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAAAQAAAAAAAAABAAAAAQAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAgAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAABAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAEAAAAAAAAAAQAAAAIAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAQAAAAAAAAAAAQAAAAAAAAABAAAAAwAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAACQAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQBAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='

    query = """{ receiveGraph(name: "g1") }"""
    assert client.query(query) == {
        'receiveGraph': received_graph
    }

    decoded_bytes = base64.b64decode(received_graph)

    g = Graph.from_bincode(decoded_bytes)
    assert g.nodes.name == ["ben", "hamza", "haaroon"]

    server.stop()


def test_receive_graph_fails_if_no_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """{ receiveGraph(name: "g2", namespace: "shivam") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g2" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_receive_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    received_graph = 'AQAAAAAAAAADAAAAAAAAAJTXBAscINjlAQAAAAAAAADv0+QnEcpEzAIAAAAAAAAArUhReOzDmFAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABAAAAAAAAAK1IUXjsw5hQAQMAAAAAAAAAYmVuAAAAAAAAAAACAAAAAgAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAQAAAAAAAAABAAAAAgAAAAIAAAAAAAAAAQAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAJTXBAscINjlAQUAAAAAAAAAaGFtemEBAAAAAAAAAAIAAAACAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAgAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAA79PkJxHKRMwBBwAAAAAAAABoYWFyb29uAgAAAAAAAAACAAAAAgAAAAAAAAACAAAAAAAAAAMAAAAAAAAAAQAAAAAAAAABAAAAAQAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAgAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAABAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAEAAAAAAAAAAQAAAAIAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAQAAAAAAAAAAAQAAAAAAAAABAAAAAwAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAACQAAAAAAAAABAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQBAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAABfZGVmYXVsdAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AQAAAAAAAAAIAAAAAAAAAF9kZWZhdWx0AAAAAAAAAAABAAAAAAAAAAgAAAAAAAAAX2RlZmF1bHQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='

    query = """{ receiveGraph(name: "g2", namespace: "shivam") }"""
    assert client.query(query) == {
        'receiveGraph': received_graph
    }

    decoded_bytes = base64.b64decode(received_graph)

    g = Graph.from_bincode(decoded_bytes)
    assert g.nodes.name == ["ben", "hamza", "haaroon"]

    server.stop()


def test_move_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      moveGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_move_graph_fails_if_graph_with_same_name_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      moveGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_move_graph_fails_if_graph_with_same_name_already_exists_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "ben", "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      moveGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
        newGraphNamespace: "ben",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = ben/g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_move_graph_fails_if_graph_with_same_name_already_exists_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      moveGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
        newGraphNamespace: "shivam",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = shivam/g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()
    

def test_move_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if rename graph succeeds and old graph is deleted
    query = """mutation {
      moveGraph(
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

    query = """{graph(name: "g4") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_move_graph_succeeds_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if rename graph succeeds and old graph is deleted
    query = """mutation {
      moveGraph(
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g4",
        newGraphNamespace: "shivam"
      )
    }"""
    client.query(query)

    query = """{graph(name: "g3", namespace: "shivam") {nodes {list {name}}}}"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g3" in str(e), f"Unexpected exception message: {e}"

    query = """{graph(name: "g4", namespace: "shivam") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_move_graph_succeeds_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "ben", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if rename graph succeeds and old graph is deleted
    query = """mutation {
      moveGraph(
        graphName: "g3",
        graphNamespace: "ben",
        newGraphName: "g4",
        newGraphNamespace: "shivam",
      )
    }"""
    client.query(query)

    query = """{graph(name: "g3", namespace: "ben") {nodes {list {name}}}}"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g3" in str(e), f"Unexpected exception message: {e}"

    query = """{graph(name: "g4", namespace: "shivam") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_copy_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      copyGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_copy_graph_fails_if_graph_with_same_name_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      copyGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_copy_graph_fails_if_graph_with_same_name_already_exists_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "ben", "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      copyGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
        newGraphNamespace: "ben",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = ben/g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_copy_graph_fails_if_graph_with_same_name_already_exists_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g6"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      copyGraph(
        graphName: "g5",
        graphNamespace: "ben",
        newGraphName: "g6",
        newGraphNamespace: "shivam",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = shivam/g6" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_copy_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if copy graph succeeds and old graph is retained
    query = """mutation {
      copyGraph(
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g4",
      )
    }"""
    client.query(query)

    query = """{graph(name: "g3", namespace: "shivam") { nodes {list {name}} }}"""
    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

    query = """{graph(name: "g4") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_copy_graph_succeeds_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if rename graph succeeds and old graph is deleted
    query = """mutation {
      copyGraph(
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g4",
        newGraphNamespace: "shivam"
      )
    }"""
    client.query(query)

    query = """{graph(name: "g3", namespace: "shivam") { nodes {list {name}} }}"""
    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

    query = """{graph(name: "g4", namespace: "shivam") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_copy_graph_succeeds_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "ben", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    # Assert if rename graph succeeds and old graph is deleted
    query = """mutation {
      copyGraph(
        graphName: "g3",
        graphNamespace: "ben",
        newGraphName: "g4",
        newGraphNamespace: "shivam",
      )
    }"""
    client.query(query)

    query = """{graph(name: "g3", namespace: "ben") { nodes {list {name}} }}"""
    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

    query = """{graph(name: "g4", namespace: "shivam") {
            nodes {list {name}}
            properties {
                constant {
                    name: get(key: "name") { value }
                    lastUpdated: get(key: "lastUpdated") { value }
                    lastOpened: get(key: "lastOpened") { value }
                }
            }
        }}"""

    result = client.query(query)
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
    assert result['graph']['properties']['constant']['name']['value'] == "g4"
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None

    server.stop()


def test_delete_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    query = """mutation {
      deleteGraph(
        graphName: "g5",
        graphNamespace: "ben",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_delete_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    g.save_to_file(os.path.join(work_dir, "g1"))

    query = """mutation {
      deleteGraph(
        graphName: "g1",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found ben/g5" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_delete_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = RaphtoryClient("http://localhost:1736")

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g1"))

    query = """mutation {
      deleteGraph(
        graphName: "g1",
        graphNamespace: "shivam",
      )
    }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()


# Update Graph with new graph name tests
def test_update_graph_with_new_graph_name_fails_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()
    query = """mutation {
        updateGraph(
          parentGraphName: "g0",
          graphName: "g2",
          graphNamespace: "shivam",
          newGraphName: "g3",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: ["ben"]
        )
    }"""

    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g0" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_with_new_graph_name_fails_if_current_graph_not_found():
    g = Graph()
    work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(work_dir, "g1"))
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()
    query = """mutation {
        updateGraph(
          parentGraphName: "g1",
          graphName: "g0",
          graphNamespace: "shivam",
          newGraphName: "g3",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: ["ben"]
        )
    }"""

    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g0" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_with_new_graph_name_fails_if_new_graph_already_exists():
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

    query = """mutation {
        updateGraph(
          parentGraphName: "g1",
          graphName: "g2",
          graphNamespace: "shivam",
          newGraphName: "g3",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: ["ben"]
        )
    }"""

    try:
        client.query(query)
    except Exception as e:
        assert "Graph already exists by name = shivam/g3" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_with_new_graph_name_succeeds_if_parent_graph_belongs_to_different_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

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
    assert result['graph']['properties']['constant']['creationTime']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_with_new_graph_name_succeeds_if_parent_graph_belongs_to_same_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g5",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g5", namespace: "shivam") { 
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


def test_update_graph_with_new_graph_name_succeeds_with_new_node_from_parent_graph_added_to_new_graph():
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    g.save_to_file(os.path.join(work_dir, "g1"))

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben", "shivam"]
      )
    }"""
    client.query(query)

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
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {'name': 'shivam'}]
    assert result['graph']['properties']['constant']['creationTime']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_with_new_graph_name_succeeds_with_new_node_removed_from_new_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

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
    assert result['graph']['properties']['constant']['creationTime']['value'] is not None
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


# Update Graph tests
def test_update_graph_fails_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()
    query = """mutation {
        updateGraph(
          parentGraphName: "g0",
          graphName: "g2",
          graphNamespace: "shivam",
          newGraphName: "g2",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: ["ben"]
        )
    }"""

    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g0" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_fails_if_current_graph_not_found():
    g = Graph()
    work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(work_dir, "g1"))
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()
    query = """mutation {
        updateGraph(
          parentGraphName: "g1",
          graphName: "g0",
          graphNamespace: "shivam",
          newGraphName: "g0",
          props: "{{ \\"target\\": 6 : }}",
          isArchive: 0,
          graphNodes: ["ben"]
        )
    }"""

    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g0" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_succeeds_if_parent_graph_belongs_to_different_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g2",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g2", namespace: "shivam") { 
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
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_succeeds_if_parent_graph_belongs_to_same_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g2",
        parentGraphNamespace: "shivam",
        graphName: "g3",
        graphNamespace: "shivam",
        newGraphName: "g3",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

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
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_succeeds_with_new_node_from_parent_graph_added_to_new_graph():
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.add_edge(4, "ben", "shivam")
    g.save_to_file(os.path.join(work_dir, "g1"))

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g2",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben", "shivam"]
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g2", namespace: "shivam") { 
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
    assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {'name': 'shivam'}]
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_succeeds_with_new_node_removed_from_new_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation {
      updateGraph(
        parentGraphName: "g1",
        graphName: "g2",
        graphNamespace: "shivam",
        newGraphName: "g2",
        props: "{ \\"target\\": 6 : }",
        isArchive: 1,
        graphNodes: ["ben"]
      )
    }"""
    client.query(query)

    query = """{
        graph(name: "g2", namespace: "shivam") { 
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
    assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
    assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
    assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
    assert result['graph']['properties']['constant']['isArchive']['value'] == 1

    server.stop()


def test_update_graph_last_opened_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { updateGraphLastOpened(graphName: "g1") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()
    

def test_update_graph_last_opened_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { updateGraphLastOpened(graphName: "g1", namespace: "shivam") }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()


def test_update_graph_last_opened_succeeds():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    g.save_to_file(os.path.join(work_dir, "g1"))
    
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
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

    server.stop()


def test_update_graph_last_opened_succeeds_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

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


def test_archive_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { archiveGraph(graphName: "g1", isArchive: 0) }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()
    

def test_archive_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    query = """mutation { archiveGraph(graphName: "g1", namespace: "shivam", isArchive: 0) }"""
    try:
        client.query(query)
    except Exception as e:
        assert "Graph not found shivam/g1" in str(e), f"Unexpected exception message: {e}"

    server.stop()
    

def test_archive_graph_succeeds():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

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

    server.stop()


def test_archive_graph_succeeds_at_namespace():
    work_dir = tempfile.mkdtemp()
    server = RaphtoryServer(work_dir).start()
    client = server.get_client()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

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
