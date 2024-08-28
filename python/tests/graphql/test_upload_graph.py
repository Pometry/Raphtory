import os
import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient
import pytest

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
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.upload_graph(path="g", file_path=g_file_path, overwrite=False)

        query = """{graph(path: "g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
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
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.upload_graph(path="g", file_path=g_file_path)
        assert "Graph already exists by name" in str(excinfo.value)


def test_upload_graph_succeeds_if_graph_already_exists_with_overwrite_enabled():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    tmp_dir = tempfile.mkdtemp()
    g_file_path = tmp_dir + "/g"
    g.save_to_file(g_file_path)

    tmp_work_dir = tempfile.mkdtemp()
    with GraphServer(tmp_work_dir).start():
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
                "nodes": {
                    "list": [
                        {"name": "ben"},
                        {"name": "hamza"},
                        {"name": "haaroon"},
                        {"name": "shivam"},
                    ]
                }
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
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        client.upload_graph(path="shivam/g", file_path=g_file_path, overwrite=False)

        query = """{graph(path: "shivam/g") {nodes {list {name}}}}"""
        assert client.query(query) == {
            "graph": {
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                }
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
    with GraphServer(tmp_work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.upload_graph(path="shivam/g", file_path=g_file_path, overwrite=False)
        assert "Graph already exists by name" in str(excinfo.value)


def test_upload_graph_succeeds_if_graph_already_exists_at_namespace_with_overwrite_enabled():
    tmp_work_dir = tempfile.mkdtemp()
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")
    os.makedirs(os.path.join(tmp_work_dir, "shivam"), exist_ok=True)
    g.save_to_file(os.path.join(tmp_work_dir, "shivam", "g"))

    with GraphServer(tmp_work_dir).start():
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
                "nodes": {
                    "list": [
                        {"name": "ben"},
                        {"name": "hamza"},
                        {"name": "haaroon"},
                        {"name": "shivam"},
                    ]
                }
            }
        }
