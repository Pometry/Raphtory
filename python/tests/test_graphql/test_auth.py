from time import time
import pytest
import jwt
import json
import requests
import tempfile
from datetime import datetime, timezone
from raphtory.graphql import GraphServer, RaphtoryClient

SECRET = "SpoDpkfhHNlcx0V5wG9vD5njzj0DAHNC17mWTa3B/h8="

READ_HEADERS = {
    "Authorization": f"Bearer {jwt.encode({"role": "read"}, SECRET, algorithm="HS256")}",
}

WRITE_HEADERS = {
    "Authorization": f"Bearer {jwt.encode({"role": "write"}, SECRET, algorithm="HS256")}",
}

NEW_TEST_GRAPH = """mutation { newGraph(path:"test", graphType:EVENT) }"""

QUERY_GRAPHS = """
query {
  graphs {
    name
  }
}
"""

def test_expired_token():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET, read_requires_auth=True).start():
        exp = time() - 100
        headers = {
            "Authorization": f"Bearer {jwt.encode({"role": "read", "exp": exp}, SECRET, algorithm="HS256")}",
        }
        response = requests.post("http://localhost:1736", headers=headers, data=json.dumps({"query": QUERY_GRAPHS}))
        assert(response.status_code == 401)

        headers = {
            "Authorization": f"Bearer {jwt.encode({"role": "write", "exp": exp}, SECRET, algorithm="HS256")}",
        }
        response = requests.post("http://localhost:1736", headers=headers,  data=json.dumps({"query": QUERY_GRAPHS}))
        assert(response.status_code == 401)

def test_default_read_access():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET).start():
        data = json.dumps({"query": QUERY_GRAPHS})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.status_code == 401)

        response = requests.post("http://localhost:1736", headers=READ_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

def test_disabled_read_access():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET, read_requires_auth=False).start():
        data = json.dumps({"query": QUERY_GRAPHS})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

        response = requests.post("http://localhost:1736", headers=READ_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

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
        requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=json.dumps({"query": NEW_TEST_GRAPH}))

        data = json.dumps({"query": query})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.status_code == 401)

        response = requests.post("http://localhost:1736", headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=data)
        assert("errors" not in response.json())
        assert(len(response.json()["data"]) == 1)

NEW_GRAPH = """mutation { newGraph(path:"new", graphType:EVENT) }"""
MOVE_GRAPH = """mutation { moveGraph(path:"test", newPath:"moved") }"""
COPY_GRAPH = """mutation { copyGraph(path:"test", newPath:"copied") }"""
DELETE_GRAPH = """mutation { deleteGraph(path:"test") }"""
CREATE_SUBGRAPH = """mutation { createSubgraph(parentPath:"test", newPath: "subgraph", nodes: [], overwrite: false) }"""

@pytest.mark.parametrize("query", [NEW_GRAPH, MOVE_GRAPH, COPY_GRAPH, DELETE_GRAPH, CREATE_SUBGRAPH])
def test_mutations(query):
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET).start():
        requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=json.dumps({"query": NEW_TEST_GRAPH}))

        data = json.dumps({"query": query})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.status_code == 401)

        response = requests.post("http://localhost:1736", headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=data)
        assert("errors" not in response.json())
        assert(len(response.json()["data"]) == 1)
