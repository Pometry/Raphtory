from time import time
import pytest
import jwt
import json
import requests
import tempfile
from datetime import datetime, timezone
from raphtory.graphql import GraphServer, RaphtoryClient

SECRET = "SpoDpkfhHNlcx0V5wG9vD5njzj0DAHNC17mWTa3B/h8="

RAPHTORY = "http://localhost:1736"

READ_JWT = jwt.encode({"a": "ro"}, SECRET, algorithm="HS256")
READ_HEADERS = {
    "Authorization": f"Bearer {READ_JWT}",
}

WRITE_JWT = jwt.encode({"a": "rw"}, SECRET, algorithm="HS256")
WRITE_HEADERS = {
    "Authorization": f"Bearer {WRITE_JWT}",
}

NEW_TEST_GRAPH = """mutation { newGraph(path:"test", graphType:EVENT) }"""

QUERY_GRAPHS = """query { graphs { name } }"""
QUERY_NAMEPSACES = """query { namespaces { path } }"""
QUERY_ROOT = """query { root { graphs { path } } }"""
QUERY_GRAPH = """query { graph(path: "test") { path } }"""
TEST_QUERIES = [QUERY_GRAPHS, QUERY_NAMEPSACES, QUERY_ROOT, QUERY_GRAPH]

def assert_successful_response(response: requests.Response):
    assert("errors" not in response.json())
    assert(type(response.json()["data"]) == dict)
    assert(len(response.json()["data"]) == 1)

# TODO: implement this so we can use the with sintax
def add_test_graph():
    requests.post(RAPHTORY, headers=WRITE_HEADERS, data=json.dumps({"query": NEW_TEST_GRAPH}))

def test_expired_token():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET, read_requires_auth=True).start():
        exp = time() - 100
        headers = {
            "Authorization": f"Bearer {jwt.encode({"a": "ro", "exp": exp}, SECRET, algorithm="HS256")}",
        }
        response = requests.post(RAPHTORY, headers=headers, data=json.dumps({"query": QUERY_GRAPHS}))
        assert(response.status_code == 401)

        headers = {
            "Authorization": f"Bearer {jwt.encode({"a": "rw", "exp": exp}, SECRET, algorithm="HS256")}",
        }
        response = requests.post(RAPHTORY, headers=headers,  data=json.dumps({"query": QUERY_GRAPHS}))
        assert(response.status_code == 401)

@pytest.mark.parametrize("query", TEST_QUERIES)
def test_default_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert(response.status_code == 401)

        response = requests.post(RAPHTORY, headers=READ_HEADERS,  data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS,  data=data)
        assert_successful_response(response)

@pytest.mark.parametrize("query", TEST_QUERIES)
def test_disabled_read_access(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET, read_requires_auth=False).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=READ_HEADERS,  data=data)
        assert_successful_response(response)

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS,  data=data)
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
    with GraphServer(work_dir, auth_secret=SECRET).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert(response.status_code == 401)

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write access")

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)

NEW_GRAPH = """mutation { newGraph(path:"new", graphType:EVENT) }"""
MOVE_GRAPH = """mutation { moveGraph(path:"test", newPath:"moved") }"""
COPY_GRAPH = """mutation { copyGraph(path:"test", newPath:"copied") }"""
DELETE_GRAPH = """mutation { deleteGraph(path:"test") }"""
CREATE_SUBGRAPH = """mutation { createSubgraph(parentPath:"test", newPath: "subgraph", nodes: [], overwrite: false) }"""

@pytest.mark.parametrize("query", [NEW_GRAPH, MOVE_GRAPH, COPY_GRAPH, DELETE_GRAPH, CREATE_SUBGRAPH])
def test_mutations(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET).start():
        add_test_graph()
        data = json.dumps({"query": query})

        response = requests.post(RAPHTORY, data=data)
        assert(response.status_code == 401)

        response = requests.post(RAPHTORY, headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write access")

        response = requests.post(RAPHTORY, headers=WRITE_HEADERS, data=data)
        assert_successful_response(response)
