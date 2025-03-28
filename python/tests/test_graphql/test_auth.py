import pytest
import jwt
import json
import requests
import tempfile
from raphtory.graphql import GraphServer, RaphtoryClient

SECRET = "SpoDpkfhHNlcx0V5wG9vD5njzj0DAHNC17mWTa3B/h8="

READ_HEADERS = {
    "Authorization": f"Bearer {jwt.encode({"role": "read"}, SECRET, algorithm="HS256")}",
    # "Content-Type": "application/json"
}

WRITE_HEADERS = {
    "Authorization": f"Bearer {jwt.encode({"role": "write"}, SECRET, algorithm="HS256")}",
    # "Content-Type": "application/json"
}

QUERY_GRAPHS = """
query {
  graphs {
    name
  }
}
"""

UPDATE_GRAPH = """
query {
  updateGraph(path: "test") {
    addNode(time: 0, name: "node") {
      success
    }
  }
}
"""

NEW_TEST_GRAPH = """mutation { newGraph(path:"test", graphType:EVENT) }"""

def test_read_access():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET, read_requires_auth=True).start():
        data = json.dumps({"query": QUERY_GRAPHS})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.status_code == 401)

        response = requests.post("http://localhost:1736", headers=READ_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS,  data=data)
        assert(response.json() == {"data": {'graphs': {'name': []}}})

def test_update_graph():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, auth_secret=SECRET).start():
        requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=json.dumps({"query": NEW_TEST_GRAPH}))

        data = json.dumps({"query": UPDATE_GRAPH})

        response = requests.post("http://localhost:1736", data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=data)
        assert(response.json() == {"data": {"updateGraph": {"addNode": {"success": True}}}})

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
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=READ_HEADERS, data=data)
        assert(response.json()["data"] is None)
        assert(response.json()["errors"][0]["message"] == "The requested endpoint requires write privileges")

        response = requests.post("http://localhost:1736", headers=WRITE_HEADERS, data=data)
        assert("errors" not in response.json())
        assert(len(response.json()["data"]) == 1)
