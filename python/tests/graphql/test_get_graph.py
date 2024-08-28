import os
import tempfile

import pytest

from test_graphql import normalize_path
from raphtory import Graph
from raphtory.graphql import GraphServer


def test_get_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = """{ graph(path: "g1") { name, path, nodes { list { name } } } }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_get_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = (
            """{ graph(path: "shivam/g1") { name, path, nodes { list { name } } } }"""
        )
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_get_graph_succeeds_if_graph_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "g1"))

        query = """{ graph(path: "g1") { name, path, nodes { list { name } } } }"""
        assert client.query(query) == {
            "graph": {
                "name": "g1",
                "nodes": {
                    "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
                },
                "path": "g1",
            }
        }


def test_get_graph_succeeds_if_graph_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        g = Graph()
        g.add_edge(1, "ben", "hamza")
        g.add_edge(2, "haaroon", "hamza")
        g.add_edge(3, "ben", "haaroon")

        os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
        g.save_to_file(os.path.join(work_dir, "shivam", "g2"))

        query = (
            """{ graph(path: "shivam/g2") { name, path, nodes { list { name } } } }"""
        )
        response = client.query(query)
        assert response["graph"]["name"] == "g2"
        assert response["graph"]["nodes"] == {
            "list": [{"name": "ben"}, {"name": "hamza"}, {"name": "haaroon"}]
        }
        assert normalize_path(response["graph"]["path"]) == "shivam/g2"


def test_get_graphs_returns_emtpy_list_if_no_graphs_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        # Assert if no graphs are discoverable
        query = """{ graphs { name, path } }"""
        assert client.query(query) == {"graphs": {"name": [], "path": []}}


def test_get_graphs_returns_graph_list_if_graphs_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
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
            "graphs": {
                "name": sorted(response["graphs"]["name"]),
                "path": sorted(normalize_path(p) for p in response["graphs"]["path"]),
            }
        }
        assert sorted_response == {
            "graphs": {
                "name": ["g1", "g2", "g3"],
                "path": ["g1", "shivam/g2", "shivam/g3"],
            }
        }

