from time import time
import pytest
import jwt
import base64
import json
import requests
import tempfile
from datetime import datetime, timezone
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

# openssl genpkey -algorithm ed25519 -out raphtory-key.pem
# openssl pkey -in raphtory-key.pem -pubout -outform DER | base64
PUB_KEY = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
# cat raphtory-key.pem
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----"""

RAPHTORY = "http://localhost:1736"

READ_JWT = jwt.encode({"a": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
READ_HEADERS = {
    "Authorization": f"Bearer {READ_JWT}",
}

WRITE_JWT = jwt.encode({"a": "rw"}, PRIVATE_KEY, algorithm="EdDSA")
WRITE_HEADERS = {
    "Authorization": f"Bearer {WRITE_JWT}",
}

NEW_TEST_GRAPH = """mutation { newGraph(path:"test", graphType:EVENT) }"""

QUERY_NAMESPACES = """query { namespaces { path } }"""
QUERY_ROOT = """query { root { graphs { path } } }"""
QUERY_GRAPH = """query { graph(path: "test") { path } }"""
TEST_QUERIES = [QUERY_NAMESPACES, QUERY_GRAPH, QUERY_ROOT]


def assert_successful_response(response: requests.Response):
    assert "errors" not in response.json()
    assert type(response.json()["data"]) == dict
    assert len(response.json()["data"]) == 1


# TODO: implement this so we can use the with sintax
def add_test_graph():
    requests.post(
        RAPHTORY, headers=WRITE_HEADERS, data=json.dumps({"query": NEW_TEST_GRAPH})
    )


def test_expired_token():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        exp = time() - 100
        token = jwt.encode({"a": "ro", "exp": exp}, PRIVATE_KEY, algorithm="EdDSA")
        headers = {
            "Authorization": f"Bearer {token}",
        }
        response = requests.post(
            RAPHTORY, headers=headers, data=json.dumps({"query": QUERY_ROOT})
        )
        assert response.status_code == 401

        token = jwt.encode({"a": "rw", "exp": exp}, PRIVATE_KEY, algorithm="EdDSA")
        headers = {
            "Authorization": f"Bearer {token}",
        }
        response = requests.post(
            RAPHTORY, headers=headers, data=json.dumps({"query": QUERY_ROOT})
        )
        assert response.status_code == 401


@pytest.mark.parametrize("query", TEST_QUERIES)
def test_default_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert response.status_code == 401

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


@pytest.mark.parametrize("query", TEST_QUERIES)
def test_disabled_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir, auth_public_key=PUB_KEY, auth_enabled_for_reads=False
    ).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


ADD_NODE = """
query {
  updateGraph(path: "test") {
    addNode(time: 0, name: "node") {
      success
    }
  }
}
"""
ADD_EDGE = """
query {
  updateGraph(path: "test") {
    addNode(time: 0, name: "node") {
      success
    }
  }
}
"""
ADD_TEMP_PROP = """
query {
  updateGraph(path: "test") {
    addProperties(t: 0, properties: [{key: "value", value: {str: "value"}}])
  }
}
"""
ADD_CONST_PROP = """
query {
  updateGraph(path: "test") {
    addConstantProperties(properties: [{key: "value", value: {str: "value"}}])
  }
}
"""


@pytest.mark.parametrize("query", [ADD_NODE, ADD_EDGE, ADD_TEMP_PROP, ADD_CONST_PROP])
def test_update_graph(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert response.status_code == 401

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert response.json()["data"] is None
        assert (
            response.json()["errors"][0]["message"]
            == "The requested endpoint requires write access"
        )

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


NEW_GRAPH = """mutation { newGraph(path:"new", graphType:EVENT) }"""
MOVE_GRAPH = """mutation { moveGraph(path:"test", newPath:"moved") }"""
COPY_GRAPH = """mutation { copyGraph(path:"test", newPath:"copied") }"""
DELETE_GRAPH = """mutation { deleteGraph(path:"test") }"""
CREATE_SUBGRAPH = """mutation { createSubgraph(parentPath:"test", newPath: "subgraph", nodes: [], overwrite: false) }"""


@pytest.mark.parametrize(
    "query", [NEW_GRAPH, MOVE_GRAPH, COPY_GRAPH, DELETE_GRAPH, CREATE_SUBGRAPH]
)
def test_mutations(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert response.status_code == 401

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert response.json()["data"] is None
        assert (
            response.json()["errors"][0]["message"]
            == "The requested endpoint requires write access"
        )

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)


def test_raphtory_client():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        client = RaphtoryClient(url=RAPHTORY, token=WRITE_JWT)
        client.new_graph("test", "EVENT")
        g = client.remote_graph("test")
        g.add_node(0, "test")
        node = g.node("test")
        g = client.receive_graph("test")
        assert g.node("test") is not None


def test_upload_graph():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        client = RaphtoryClient(url=RAPHTORY, token=WRITE_JWT)
        g = Graph()
        g.add_node(0, "uploaded-node")
        tmp_dir = tempfile.mkdtemp()
        path = tmp_dir + "/graph"
        g.save_to_zip(path)
        client.upload_graph(path="uploaded", file_path=path)
        g = client.receive_graph("uploaded")
        assert g.node("uploaded-node") is not None
