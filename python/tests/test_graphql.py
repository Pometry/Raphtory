import base64
import os
import tempfile
import time

from raphtory.graphql import RaphtoryServer, RaphtoryClient
from raphtory import graph_loader
from raphtory import Graph
import json


def normalize_path(path):
    return path.replace('\\', '/')


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
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g", graph=g)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }


def test_server_start_on_custom_port():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start(port=1737):
        client = RaphtoryClient("http://localhost:1737")
        client.send_graph(path="g", graph=g)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }


def test_send_graph_succeeds_if_no_graph_found_with_same_name():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="g", graph=g)


def test_send_graph_fails_if_graph_already_exists():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        try:
            client.send_graph(path="g", graph=g)
        except Exception as e:
            assert "Graph already exists by name = g" in str(e), f"Unexpected exception message: {e}"


def test_send_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    g.save_to_file(os.path.join(tmp_work_dir, "g"))

    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        client.send_graph(path="g", graph=g, overwrite=True)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
            }
        }


def test_send_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.send_graph(path="shivam/g", graph=g)


def test_send_graph_fails_if_graph_already_exists_at_namespace():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        try:
            client.send_graph(path="shivam/g", graph=g)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_send_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        client.send_graph(path="shivam/g", graph=g, overwrite=True)

        query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
            }
        }


def test_namespaces():
    def assert_graph_fetch(path):
        query = f"""{{ graph(path: "{path}") {{ nodes {{ list {{ name }} }} }} }}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }

    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    path = "g"
    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start():
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

        path = "./shivam/investigation/g"
        client.send_graph(path=path, graph=g, overwrite=True)
        expected_path = os.path.join(tmp_work_dir, path)
        assert os.path.exists(expected_path)
        assert_graph_fetch(path)

        path = "./shivam/investigation/2024/12/12/g"
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
        try:
            client.send_graph(path=path, graph=g, overwrite=True)
        except Exception as e:
            assert "Invalid path" in str(e), f"Unexpected exception message: {e}"

        path = "./shivam/../investigation/g"
        try:
            client.send_graph(path=path, graph=g, overwrite=True)
        except Exception as e:
            assert "Invalid path" in str(e), f"Unexpected exception message: {e}"

        path = "//shivam/investigation/g"
        try:
            client.send_graph(path=path, graph=g, overwrite=True)
        except Exception as e:
            assert "Invalid path" in str(e), f"Unexpected exception message: {e}"

        path = "shivam/investigation//2024-12-12/g"
        try:
            client.send_graph(path=path, graph=g, overwrite=True)
        except Exception as e:
            assert "Invalid path" in str(e), f"Unexpected exception message: {e}"

        path = "shivam/investigation\2024-12-12"
        try:
            client.send_graph(path=path, graph=g, overwrite=True)
        except Exception as e:
            assert "Invalid path" in str(e), f"Unexpected exception message: {e}"


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
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.upload_graph(path="g", file_path=g_file_path, overwrite=False)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }


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
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        try:
            client.upload_graph(path="g", file_path=g_file_path)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_upload_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        tmp_dir = tempfile.mkdtemp()
        g_file_path = tmp_dir + "/g"
        g.save_to_file(g_file_path)

        client.upload_graph(path="g", file_path=g_file_path, overwrite=True)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
            }
        }


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
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.upload_graph(path="shivam/g", file_path=g_file_path, overwrite=False)

        query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
            }
        }


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
    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        try:
            client.upload_graph(path="shivam/g", file_path=g_file_path, overwrite=False)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_upload_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with RaphtoryServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")
        g.add_edge(4, "ben", "shivam")
        tmp_dir = tempfile.mkdtemp()
        g_file_path = tmp_dir + "/g"
        g.save_to_file(g_file_path)

        client.upload_graph(path="shivam/g", file_path=g_file_path, overwrite=True)

        query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
            }
        }


# def test_load_graph_succeeds_if_no_graph_found_with_same_name():
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     tmp_dir = tempfile.mkdtemp()
#     g_file_path = tmp_dir + "/g"
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#         client.load_graph(file_path=g_file_path, overwrite=False)
#
#         query = """{graph(path: "g") {nodes {list {name}}}}"""
#         assert client.query(query) == {
#             "graph": {
#                 "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
#             }
#         }


# def test_load_graph_fails_if_graph_already_exists():
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     tmp_dir = tempfile.mkdtemp()
#     g_file_path = tmp_dir + "/g"
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     path = os.path.join(tmp_work_dir, "g")
#     g.save_to_file(path)
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#         try:
#             client.load_graph(file_path=g_file_path)
#         except Exception as e:
#             assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


# def test_load_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     tmp_dir = tempfile.mkdtemp()
#     g_file_path = tmp_dir + "/g"
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#
#         g = Graph()
#         g.add_edge(1, "ben", "hamza")
#         g.add_edge(2, "haaroon", "hamza")
#         g.add_edge(3, "ben", "haaroon")
#         g.add_edge(4, "ben", "shivam")
#         tmp_dir = tempfile.mkdtemp()
#         g_file_path = tmp_dir + "/g"
#         g.save_to_file(g_file_path)
#
#         client.load_graph(file_path=g_file_path, overwrite=True)
#
#         query = """{graph(path: "g") {nodes {list {name}}}}"""
#         assert client.query(query) == {
#             "graph": {
#                 "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
#             }
#         }


# Test load graph at namespace
# def test_load_graph_succeeds_if_no_graph_found_with_same_name_at_namespace():
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     tmp_dir = tempfile.mkdtemp()
#     g_file_path = tmp_dir + "/g"
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#         client.load_graph(file_path=g_file_path, overwrite=False, namespace="shivam")
#
#         query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
#         assert client.query(query) == {
#             "graph": {
#                 "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
#             }
#         }


# def test_load_graph_fails_if_graph_already_exists_at_namespace():
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     tmp_dir = tempfile.mkdtemp()
#     g_file_path = tmp_dir + "/g"
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
#     path = os.path.join(tmp_work_dir, "shivam", "g")
#     g.save_to_file(path)
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#
#         try:
#             client.load_graph(file_path=g_file_path, overwrite=False, namespace="shivam")
#         except Exception as e:
#             assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


# def test_load_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
#     tmp_work_dir = tempfile.mkdtemp()
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
#     g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))
#
#     with RaphtoryServer(tmp_work_dir).start():
#         client = RaphtoryClient("http://localhost:1736")
#
#         g = Graph()
#         g.add_edge(1, "ben", "hamza")
#         g.add_edge(2, "haaroon", "hamza")
#         g.add_edge(3, "ben", "haaroon")
#         g.add_edge(4, "ben", "shivam")
#         tmp_dir = tempfile.mkdtemp()
#         g_file_path = tmp_dir + "/g"
#         g.save_to_file(g_file_path)
#
#         client.load_graph(file_path=g_file_path, overwrite=True, namespace="shivam")
#
#         query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
#         assert client.query(query) == {
#             "graph": {
#                 "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}, {"name": "shivam"}]}
#             }
#         }


def test_get_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ graph(path: "g1") { name, path, nodes { list { name } } } }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_get_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ graph(path: "shivam/g1") { name, path, nodes { list { name } } } }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_get_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """{ graph(path: "g1") { name, path, nodes { list { name } } } }"""
        assert client.query(query) == {
            'graph': {'name': 'g1', 'nodes': {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]},
                      'path': 'g1'}}


def test_get_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query = """{ graph(path: "shivam/g2") { name, path, nodes { list { name } } } }"""
        response = client.query(query)
        assert response['graph']['name'] == 'g2'
        assert response['graph']['nodes'] == {'list': [{'name': 'ben'}, {'name': 'hamza'}, {'name': 'haaroon'}]}
        assert normalize_path(response['graph']['path']) == 'shivam/g2'


def test_get_graphs_returns_emtpy_list_if_no_graphs_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        # Assert if no graphs are discoverable
        query = """{ graphs { name, path } }"""
        assert client.query(query) == {
            'graphs': {'name': [], 'path': []}
        }


def test_get_graphs_returns_graph_list_if_graphs_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
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
        response = client.query(query)
        sorted_response = {
            'graphs': {
                'name': sorted(response['graphs']['name']),
                'path': sorted(normalize_path(p) for p in response['graphs']['path'])
            }
        }
        assert sorted_response == {
            'graphs': {
                'name': ['g1', 'g2', 'g3'],
                'path': ['g1', 'shivam/g2', 'shivam/g3']
            }
        }


def test_receive_graph_fails_if_no_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ receiveGraph(path: "g2") }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found g2" in str(e), f"Unexpected exception message: {e}"


def test_receive_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """{ receiveGraph(path: "g1") }"""
        received_graph = client.query(query)['receiveGraph']

        decoded_bytes = base64.b64decode(received_graph)

        g = Graph.from_bincode(decoded_bytes)
        assert g.nodes.name == ["ben", "hamza", "haaroon"]


def test_receive_graph_using_client_api_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        received_graph = client.receive_graph("g1")
        g = Graph.from_bincode(received_graph)

        assert g.nodes.name == ["ben", "hamza", "haaroon"]


def test_receive_graph_fails_if_no_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ receiveGraph(path: "shivam/g2") }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_receive_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query = """{ receiveGraph(path: "shivam/g2") }"""
        received_graph = client.query(query)['receiveGraph']

        decoded_bytes = base64.b64decode(received_graph)

        g = Graph.from_bincode(decoded_bytes)
        assert g.nodes.name == ["ben", "hamza", "haaroon"]


def test_move_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          moveGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_move_graph_fails_if_graph_with_same_name_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "g6"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          moveGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_move_graph_fails_if_graph_with_same_name_already_exists_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "ben", "g6"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          moveGraph(
            path: "ben/g5",
            newPath: "ben/g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


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

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          moveGraph(
            path: "ben/g5",
            newPath: "shivam/g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_move_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          moveGraph(
            path: "shivam/g3",
            newPath: "g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") {nodes {list {name}}}}"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"

        query = """{graph(path: "g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastUpdated: get(key: "lastUpdated") { value }
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_move_graph_using_client_api_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        client.move_graph("shivam/g3", "ben/g4")

        query = """{graph(path: "shivam/g3") {nodes {list {name}}}}"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"

        query = """{graph(path: "ben/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastUpdated: get(key: "lastUpdated") { value }
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_move_graph_succeeds_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          moveGraph(
            path: "shivam/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") {nodes {list {name}}}}"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastUpdated: get(key: "lastUpdated") { value }
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_move_graph_succeeds_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "ben", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          moveGraph(
            path: "ben/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "ben/g3") {nodes {list {name}}}}"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastUpdated: get(key: "lastUpdated") { value }
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_copy_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_copy_graph_fails_if_graph_with_same_name_already_exists():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "g6"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_copy_graph_fails_if_graph_with_same_name_already_exists_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "ben", "g5"))
    g.save_to_file(os.path.join(work_dir, "ben", "g6"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "ben/g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


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

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          copyGraph(
            path: "ben/g5",
            newPath: "shivam/g6",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_copy_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if copy graph succeeds and old graph is retained
        query = """mutation {
          copyGraph(
            path: "shivam/g3",
            newPath: "g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

        query = """{graph(path: "g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_copy_graph_using_client_api_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if copy graph succeeds and old graph is retained
        client.copy_graph("shivam/g3", "ben/g4")

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

        query = """{graph(path: "ben/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_copy_graph_succeeds_at_same_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          copyGraph(
            path: "shivam/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "shivam/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_copy_graph_succeeds_at_diff_namespace_as_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "ben"), exist_ok=True)
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "ben", "g3"))

    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        # Assert if rename graph succeeds and old graph is deleted
        query = """mutation {
          copyGraph(
            path: "ben/g3",
            newPath: "shivam/g4",
          )
        }"""
        client.query(query)

        query = """{graph(path: "ben/g3") { nodes {list {name}} }}"""
        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]

        query = """{graph(path: "shivam/g4") {
                nodes {list {name}}
                properties {
                    constant {
                        lastOpened: get(key: "lastOpened") { value }
                    }
                }
            }}"""

        result = client.query(query)
        assert result['graph']['nodes']['list'] == [{'name': 'ben'}, {"name": "hamza"}, {'name': 'haaroon'}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None


def test_delete_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """mutation {
          deleteGraph(
            path: "ben/g5",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_delete_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """mutation {
          deleteGraph(
            path: "g1",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_delete_graph_using_client_api_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        try:
            client.delete_graph("g1")
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_delete_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g1"))

        query = """mutation {
          deleteGraph(
            path: "shivam/g1",
          )
        }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_create_graph_fail_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            createGraph(
              parentGraphPath: "g0",
              newGraphPath: "shivam/g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_create_graph_fail_if_parent_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            createGraph(
              parentGraphPath: "shivam/g0",
              newGraphPath: "shivam/g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_create_graph_fail_if_graph_already_exists():
    work_dir = tempfile.mkdtemp()

    g = Graph()
    g.save_to_file(os.path.join(work_dir, "g0"))
    g.save_to_file(os.path.join(work_dir, "g3"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            createGraph(
              parentGraphPath: "g0",
              newGraphPath: "g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_create_graph_fail_if_graph_already_exists_at_namespace():
    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    
    g = Graph()
    g.save_to_file(os.path.join(work_dir, "g0"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))
    
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            createGraph(
              parentGraphPath: "g0",
              newGraphPath: "shivam/g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_create_graph_succeeds():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()

    g.save_to_file(os.path.join(work_dir, "g1"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          createGraph(
            parentGraphPath: "g1",
            newGraphPath: "g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g3") {
                nodes { list {
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_create_graph_succeeds_at_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()

    g.save_to_file(os.path.join(work_dir, "g1"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          createGraph(
            parentGraphPath: "g1",
            newGraphPath: "shivam/g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "shivam/g3") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


# Update Graph with new graph name tests (save as new graph name)
def test_update_graph_with_new_graph_name_fails_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            updateGraph(
              parentGraphPath: "g0",
              graphPath: "shivam/g2",
              newGraphPath: "g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_with_new_graph_name_fails_if_current_graph_not_found():
    g = Graph()
    work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(work_dir, "g1"))
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            updateGraph(
              parentGraphPath: "g1",
              graphPath: "shivam/g0",
              newGraphPath: "g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


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

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
            updateGraph(
              parentGraphPath: "g1",
              graphPath: "shivam/g2",
              newGraphPath: "g3",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph already exists by name" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_with_new_graph_name_succeeds_if_parent_graph_belongs_to_different_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g3") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_with_new_graph_name_succeeds_if_parent_graph_belongs_to_same_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "shivam/g2",
            graphPath: "shivam/g3",
            newGraphPath: "shivam/g5",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "shivam/g5") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_with_new_graph_name_succeeds_with_new_node_from_parent_graph_added_to_new_graph():
    work_dir = tempfile.mkdtemp()
    g = Graph()

    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_edge(4, "ben", "shivam", {"prop1": 4})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})
    g.add_node(7, "shivam", {"dept": "engineering"})
    g.save_to_file(os.path.join(work_dir, "g1"))

    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "shivam"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g3") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'shivam', 'properties': {'temporal': {'get': {'values': ['engineering']}}}}
        ]
        assert result['graph']['edges']['list'] == []
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_with_new_graph_name_succeeds_with_new_node_removed_from_new_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g3") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['creationTime']['value'] is not None
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


# Update Graph tests (save graph as same graph name)
def test_update_graph_fails_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            updateGraph(
              parentGraphPath: "g0",
              graphPath: "shivam/g2",
              newGraphPath: "g2",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_fails_if_current_graph_not_found():
    g = Graph()
    work_dir = tempfile.mkdtemp()
    g.save_to_file(os.path.join(work_dir, "g1"))
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()
        query = """mutation {
            updateGraph(
              parentGraphPath: "g1",
              graphPath: "shivam/g0",
              newGraphPath: "shivam/g0",
              props: "{{ \\"target\\": 6 : }}",
              isArchive: 0,
              graphNodes: ["ben"]
            )
        }"""

        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_succeeds_if_parent_graph_belongs_to_different_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g2",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g2") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_succeeds_if_parent_graph_belongs_to_same_namespace():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "shivam/g2",
            graphPath: "shivam/g3",
            newGraphPath: "g3",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "hamza"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g3") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'hamza', 'properties': {'temporal': {'get': {'values': ['director']}}}}
        ]
        assert result['graph']['edges']['list'] == [{'properties': {'temporal': {'get': {'values': ['1']}}}}]
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_succeeds_with_new_node_from_parent_graph_added_to_new_graph():
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_edge(4, "ben", "shivam", {"prop1": 4})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})
    g.add_node(7, "shivam", {"dept": "engineering"})
    g.save_to_file(os.path.join(work_dir, "g1"))

    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g2",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben", "shivam"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g2") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
            {'name': 'shivam', 'properties': {'temporal': {'get': {'values': ['engineering']}}}}
        ]
        assert result['graph']['edges']['list'] == []
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_succeeds_with_new_node_removed_from_new_graph():
    g = Graph()
    g.add_edge(1, "ben", "hamza", {"prop1": 1})
    g.add_edge(2, "haaroon", "hamza", {"prop1": 2})
    g.add_edge(3, "ben", "haaroon", {"prop1": 3})
    g.add_node(4, "ben", {"dept": "engineering"})
    g.add_node(5, "hamza", {"dept": "director"})
    g.add_node(6, "haaroon", {"dept": "operations"})

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g.save_to_file(os.path.join(work_dir, "g1"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation {
          updateGraph(
            parentGraphPath: "g1",
            graphPath: "shivam/g2",
            newGraphPath: "g2",
            props: "{ \\"target\\": 6 : }",
            isArchive: 1,
            graphNodes: ["ben"]
          )
        }"""
        client.query(query)

        query = """{
            graph(path: "g2") { 
                nodes {list { 
                    name
                    properties { temporal { get(key: "dept") { values } } } 
                }}
                edges { list {
                    properties { temporal { get(key: "prop1") { values } } }
                }}
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
        assert result['graph']['nodes']['list'] == [
            {'name': 'ben', 'properties': {'temporal': {'get': {'values': ['engineering']}}}},
        ]
        assert result['graph']['edges']['list'] == []
        assert result['graph']['properties']['constant']['lastOpened']['value'] is not None
        assert result['graph']['properties']['constant']['lastUpdated']['value'] is not None
        assert result['graph']['properties']['constant']['uiProps']['value'] == '{ "target": 6 : }'
        assert result['graph']['properties']['constant']['isArchive']['value'] == 1


def test_update_graph_last_opened_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation { updateGraphLastOpened(path: "g1") }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_last_opened_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation { updateGraphLastOpened(path: "shivam/g1") }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_update_graph_last_opened_succeeds():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        g.save_to_file(os.path.join(work_dir, "g1"))

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query_last_opened = """{ graph(path: "g1") { properties { constant { get(key: "lastOpened") { value } } } } }"""
        mutate_last_opened = """mutation { updateGraphLastOpened(path: "g1") }"""
        assert client.query(query_last_opened) == {'graph': {'properties': {'constant': {'get': None}}}}
        assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
        updated_last_opened1 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
        time.sleep(1)
        assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
        updated_last_opened2 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
        assert updated_last_opened2 > updated_last_opened1


def test_update_graph_last_opened_succeeds_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "g1"))
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query_last_opened = """{ graph(path: "shivam/g2") { properties { constant { get(key: "lastOpened") { value } } } } }"""
        mutate_last_opened = """mutation { updateGraphLastOpened(path: "shivam/g2") }"""
        assert client.query(query_last_opened) == {'graph': {'properties': {'constant': {'get': None}}}}
        assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
        updated_last_opened1 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
        time.sleep(1)
        assert client.query(mutate_last_opened) == {'updateGraphLastOpened': True}
        updated_last_opened2 = client.query(query_last_opened)['graph']['properties']['constant']['get']['value']
        assert updated_last_opened2 > updated_last_opened1


def test_archive_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation { archiveGraph(path: "g1", isArchive: 0) }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_archive_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        query = """mutation { archiveGraph(path: "shivam/g1", isArchive: 0) }"""
        try:
            client.query(query)
        except Exception as e:
            assert "Graph not found" in str(e), f"Unexpected exception message: {e}"


def test_archive_graph_succeeds():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "g1"))
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query_is_archive = """{ graph(path: "g1") { properties { constant { get(key: "isArchive") { value } } } } }"""
        assert client.query(query_is_archive) == {'graph': {'properties': {'constant': {'get': None}}}}
        update_archive_graph = """mutation { archiveGraph(path: "g1", isArchive: 0) }"""
        assert client.query(update_archive_graph) == {"archiveGraph": True}
        assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 0
        update_archive_graph = """mutation { archiveGraph(path: "g1", isArchive: 1) }"""
        assert client.query(update_archive_graph) == {"archiveGraph": True}
        assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 1


def test_archive_graph_succeeds_at_namespace():
    work_dir = tempfile.mkdtemp()
    with RaphtoryServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "g1"))
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query_is_archive = """{ graph(path: "shivam/g2") { properties { constant { get(key: "isArchive") { value } } } } }"""
        assert client.query(query_is_archive) == {'graph': {'properties': {'constant': {'get': None}}}}
        update_archive_graph = """mutation { archiveGraph(path: "shivam/g2", isArchive: 0) }"""
        assert client.query(update_archive_graph) == {"archiveGraph": True}
        assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 0
        update_archive_graph = """mutation { archiveGraph(path: "shivam/g2", isArchive: 1) }"""
        assert client.query(update_archive_graph) == {"archiveGraph": True}
        assert client.query(query_is_archive)['graph']['properties']['constant']['get']['value'] == 1


def test_graph_windows_and_layers_query():
    g1 = graph_loader.lotr_graph()
    g1.add_constant_properties({"name": "lotr"})
    g2 = Graph()
    g2.add_constant_properties({"name": "layers"})
    g2.add_edge(1, 1, 2, layer="layer1")
    g2.add_edge(1, 2, 3, layer="layer2")

    tmp_work_dir = tempfile.mkdtemp()
    with RaphtoryServer(tmp_work_dir).start() as server:
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
    with RaphtoryServer(tmp_work_dir).start() as server:
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


# def test_load_graph_from_path():
#     tmp_graph_dir = tempfile.mkdtemp()
#
#     g = Graph()
#     g.add_edge(1, "ben", "hamza")
#     g.add_edge(2, "haaroon", "hamza")
#     g.add_edge(3, "ben", "haaroon")
#     g_file_path = os.path.join(tmp_graph_dir, "g")
#     g.save_to_file(g_file_path)
#
#     tmp_work_dir = tempfile.mkdtemp()
#     with RaphtoryServer(tmp_work_dir).start() as server:
#         client = server.get_client()
#         normalized_path = normalize_path(g_file_path)
#         query = f"""mutation {{
#           loadGraphFromPath(
#             pathOnServer: "{normalized_path}",
#             overwrite: false
#           )
#         }}"""
#         res = client.query(query)
#         print(res)
#
#         query = """{graph(path: "g") {nodes {list {name}}}}"""
#         assert client.query(query) == {
#             "graph": {
#                 "nodes": {"list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]}
#             }
#         }
#
