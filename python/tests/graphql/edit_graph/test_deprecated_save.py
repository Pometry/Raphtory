import os
import tempfile
import time

import pytest

from raphtory import Graph
from raphtory.graphql import GraphServer


def test_create_graph_fail_if_parent_graph_not_found():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
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
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_create_graph_fail_if_parent_graph_not_found_at_namespace():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start() as server:
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
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph not found" in str(excinfo.value)


def test_create_graph_fail_if_graph_already_exists():
    work_dir = tempfile.mkdtemp()

    g = Graph()
    g.save_to_file(os.path.join(work_dir, "g0"))
    g.save_to_file(os.path.join(work_dir, "g3"))

    with GraphServer(work_dir).start() as server:
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
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


def test_create_graph_fail_if_graph_already_exists_at_namespace():
    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)

    g = Graph()
    g.save_to_file(os.path.join(work_dir, "g0"))
    g.save_to_file(os.path.join(work_dir, "shivam", "g3"))

    with GraphServer(work_dir).start() as server:
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
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert "Graph already exists by name" in str(excinfo.value)


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

    with GraphServer(work_dir).start() as server:
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
        assert result["graph"]["nodes"]["list"] == [
            {
                "name": "ben",
                "properties": {"temporal": {"get": {"values": ["engineering"]}}},
            },
            {
                "name": "hamza",
                "properties": {"temporal": {"get": {"values": ["director"]}}},
            },
        ]
        assert result["graph"]["edges"]["list"] == [
            {"properties": {"temporal": {"get": {"values": ["1"]}}}}
        ]
        assert (
            result["graph"]["properties"]["constant"]["creationTime"]["value"]
            is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["lastUpdated"]["value"]
            is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["uiProps"]["value"]
            == '{ "target": 6 : }'
        )
        assert result["graph"]["properties"]["constant"]["isArchive"]["value"] == 1


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

    with GraphServer(work_dir).start() as server:
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
        assert result["graph"]["nodes"]["list"] == [
            {
                "name": "ben",
                "properties": {"temporal": {"get": {"values": ["engineering"]}}},
            },
            {
                "name": "hamza",
                "properties": {"temporal": {"get": {"values": ["director"]}}},
            },
        ]
        assert result["graph"]["edges"]["list"] == [
            {"properties": {"temporal": {"get": {"values": ["1"]}}}}
        ]
        assert (
            result["graph"]["properties"]["constant"]["creationTime"]["value"]
            is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["lastOpened"]["value"] is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["lastUpdated"]["value"]
            is not None
        )
        assert (
            result["graph"]["properties"]["constant"]["uiProps"]["value"]
            == '{ "target": 6 : }'
        )
        assert result["graph"]["properties"]["constant"]["isArchive"]["value"] == 1



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
                      properties: [{key: "isArchive", value: 0}]
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
                      properties: [{key: "isArchive", value: 1}]
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
