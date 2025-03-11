import os
import tempfile

import pytest

from raphtory import Graph
from raphtory.graphql import GraphServer


def test_archive_graph_fails_if_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = """query {
                  updateGraph(path: "g1") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 0 }}]
                    )
                  }
                }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_archive_graph_fails_if_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
        client = server.get_client()

        query = """query {
                  updateGraph(path: "shivam/g1") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 0 }}]
                    )
                  }
                }"""
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_archive_graph_succeeds():
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

        query_is_archive = """{ graph(path: "g1") { properties { constant { get(key: "isArchive") { value } } } } }"""
        assert client.query(query_is_archive) == {
            "graph": {"properties": {"constant": {"get": None}}}
        }
        update_archive_graph = """query {
                  updateGraph(path: "g1") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 0 }}]
                    )
                  }
                }"""
        assert client.query(update_archive_graph) == {
            "updateGraph": {"updateConstantProperties": True}
        }
        assert (
            client.query(query_is_archive)["graph"]["properties"]["constant"]["get"][
                "value"
            ]
            == 0
        )
        update_archive_graph = """query {
                  updateGraph(path: "g1") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 1 }}]
                    )
                  }
                }"""
        assert client.query(update_archive_graph) == {
            "updateGraph": {"updateConstantProperties": True}
        }
        assert (
            client.query(query_is_archive)["graph"]["properties"]["constant"]["get"][
                "value"
            ]
            == 1
        )


def test_archive_graph_succeeds_at_namespace():
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

        query_is_archive = """{ graph(path: "shivam/g2") { properties { constant { get(key: "isArchive") { value } } } } }"""
        assert client.query(query_is_archive) == {
            "graph": {"properties": {"constant": {"get": None}}}
        }
        update_archive_graph = """query {
                  updateGraph(path: "shivam/g2") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 0 }}]
                    )
                  }
                }"""
        assert client.query(update_archive_graph) == {
            "updateGraph": {"updateConstantProperties": True}
        }
        assert (
            client.query(query_is_archive)["graph"]["properties"]["constant"]["get"][
                "value"
            ]
            == 0
        )
        update_archive_graph = """query {
                  updateGraph(path: "shivam/g2") {
                    updateConstantProperties(
                      properties: [{key: "isArchive", value: { u64: 1 }}]
                    )
                  }
                }"""
        assert client.query(update_archive_graph) == {
            "updateGraph": {"updateConstantProperties": True}
        }
        assert (
            client.query(query_is_archive)["graph"]["properties"]["constant"]["get"][
                "value"
            ]
            == 1
        )
