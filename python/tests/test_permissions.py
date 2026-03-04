import json
import tempfile
import os
import time
import requests
import jwt
import pytest
from raphtory.graphql import GraphServer

# Reuse the same key pair as test_auth.py
PUB_KEY = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----"""

RAPHTORY = "http://localhost:1736"

# JWTs with roles
ANALYST_JWT = jwt.encode({"a": "ro", "role": "analyst"}, PRIVATE_KEY, algorithm="EdDSA")
ANALYST_HEADERS = {"Authorization": f"Bearer {ANALYST_JWT}"}

ADMIN_JWT = jwt.encode({"a": "rw", "role": "admin"}, PRIVATE_KEY, algorithm="EdDSA")
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_JWT}"}

# JWT with no role — valid token but no role field
NO_ROLE_JWT = jwt.encode({"a": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
NO_ROLE_HEADERS = {"Authorization": f"Bearer {NO_ROLE_JWT}"}

QUERY_JIRA = """query { graph(path: "jira") { path } }"""
QUERY_ADMIN = """query { graph(path: "admin") { path } }"""
CREATE_JIRA = """mutation { newGraph(path:"jira", graphType:EVENT) }"""
CREATE_ADMIN = """mutation { newGraph(path:"admin", graphType:EVENT) }"""


def make_permissions_store(path: str) -> str:
    """Write a permissions store JSON file and return its path."""
    store = {
        "roles": {
            "analyst": {"graphs": [{"name": "jira"}]},
            "admin": {"graphs": [{"name": "*", "nodes": "ro", "edges": "ro"}]},
        }
    }
    store_path = os.path.join(path, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)
    return store_path


def make_permissions_store_with_node_access(path: str) -> str:
    """Permissions store where analyst has node access to jira but not edges."""
    store = {
        "roles": {
            "analyst": {"graphs": [{"name": "jira", "nodes": "ro"}]},
            "admin": {"graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}]},
        }
    }
    store_path = os.path.join(path, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)
    return store_path


def test_analyst_can_access_permitted_graph():
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        # Create the graphs using admin role
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_ADMIN})
        )

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


def test_analyst_cannot_access_denied_graph():
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_ADMIN})
        )

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_ADMIN})
        )
        assert response.json()["data"] is None
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_admin_can_access_all_graphs():
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_ADMIN})
        )

        for query in [QUERY_JIRA, QUERY_ADMIN]:
            response = requests.post(
                RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": query})
            )
            assert "errors" not in response.json(), response.json()


def test_no_role_is_denied_when_store_is_active():
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        response = requests.post(
            RAPHTORY, headers=NO_ROLE_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert response.json()["data"] is None
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_no_permissions_store_gives_full_access():
    """Without a permissions store configured, all authenticated users see everything."""
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir, auth_public_key=PUB_KEY).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        # analyst role but no store → full access
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()


QUERY_JIRA_NODES = """query { graph(path: "jira") { nodes { list { name } } } }"""
QUERY_JIRA_EDGES = (
    """query { graph(path: "jira") { edges { list { src { name } } } } }"""
)
ADD_NODE_JIRA = """mutation { newGraph(path:"jira", graphType:EVENT) }"""


def test_graph_metadata_accessible_without_node_grant():
    """path/name are shallow — accessible even without nodes/edges grant."""
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        # analyst has graph access but no nodes/edges grant
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


def test_nodes_denied_without_grant():
    """Querying nodes without a nodes grant returns an error."""
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_JIRA_NODES}),
        )
        assert (
            response.json()["data"] is None or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_edges_denied_without_grant():
    """Querying edges without an edges grant returns an error."""
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_JIRA_EDGES}),
        )
        assert (
            response.json()["data"] is None or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_nodes_accessible_with_grant():
    """Querying nodes succeeds when nodes grant is present."""
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store_with_node_access(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_JIRA_NODES}),
        )
        assert "errors" not in response.json(), response.json()
        assert isinstance(response.json()["data"]["graph"]["nodes"]["list"], list)


def test_edges_still_denied_when_only_nodes_granted():
    """Having nodes access does not grant edges access."""
    work_dir = tempfile.mkdtemp()
    store_path = make_permissions_store_with_node_access(work_dir)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_JIRA_EDGES}),
        )
        assert (
            response.json()["data"] is None or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_permissions_hot_reload():
    """Updating the permissions file on disk is picked up without restarting the server."""
    work_dir = tempfile.mkdtemp()

    # Start with analyst denied access to jira
    store = {
        "roles": {
            "analyst": {"graphs": []},  # no graphs granted
            "admin": {"graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}]},
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )

        # Confirm analyst is denied before reload
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "Access denied" in response.json()["errors"][0]["message"]

        # Update permissions file on disk — grant analyst access to jira
        store["roles"]["analyst"]["graphs"] = [{"name": "jira"}]
        with open(store_path, "w") as f:
            json.dump(store, f)

        # Wait for the polling task to pick up the change (polls every 5s)
        time.sleep(7)

        # Analyst should now be able to access jira without a server restart
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


######## Introspection tests ##################################################

QUERY_SCHEMA = """query { __schema { queryType { name } } }"""
QUERY_COUNT_NODES = """query { graph(path: "jira") { countNodes } }"""
QUERY_UNIQUE_LAYERS = """query { graph(path: "jira") { uniqueLayers } }"""
QUERY_COUNT_EDGES = """query { graph(path: "jira") { countEdges } }"""


def test_schema_introspection_denied_when_role_flag_false():
    """Role with introspection:false cannot query __schema."""
    work_dir = tempfile.mkdtemp()
    store = {
        "roles": {
            "analyst": {
                "introspection": False,
                "graphs": [{"name": "jira", "nodes": "ro"}],
            },
            "admin": {
                "introspection": True,
                "graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}],
            },
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_SCHEMA})
        )
        # async-graphql returns {"data": null} with no errors array when introspection
        # is disabled via disable_introspection() — the field is silently nullified.
        assert response.json()["data"] is None
        assert "errors" not in response.json() or response.json()["errors"] == []


def test_schema_introspection_allowed_when_role_flag_true():
    """Role with introspection:true can query __schema."""
    work_dir = tempfile.mkdtemp()
    store = {
        "roles": {
            "admin": {
                "introspection": True,
                "graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}],
            }
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        response = requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": QUERY_SCHEMA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["__schema"]["queryType"]["name"] == "QueryRoot"


def test_count_nodes_denied_when_graph_introspection_false():
    """countNodes is blocked when graph-level introspection is false."""
    work_dir = tempfile.mkdtemp()
    store = {
        "roles": {
            "analyst": {
                "introspection": True,  # role-level allows schema
                "graphs": [{"name": "jira", "nodes": "ro", "introspection": False}],
            },
            "admin": {"graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}]},
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_COUNT_NODES}),
        )
        assert (
            response.json()["data"] is None or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_count_nodes_allowed_when_graph_introspection_true():
    """countNodes succeeds when graph-level introspection is true."""
    work_dir = tempfile.mkdtemp()
    store = {
        "roles": {
            "analyst": {
                "introspection": False,  # role-level denies schema
                "graphs": [{"name": "jira", "nodes": "ro", "introspection": True}],
            },
            "admin": {"graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}]},
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_COUNT_NODES}),
        )
        assert "errors" not in response.json(), response.json()
        assert isinstance(response.json()["data"]["graph"]["countNodes"], int)


def test_per_graph_introspection_overrides_role_level():
    """Per-graph introspection:false blocks counts even when role-level is true."""
    work_dir = tempfile.mkdtemp()
    store = {
        "roles": {
            "analyst": {
                "introspection": True,
                "graphs": [
                    {"name": "jira", "nodes": "ro", "introspection": False},
                    {"name": "admin", "nodes": "ro", "introspection": True},
                ],
            },
            "admin": {"graphs": [{"name": "*", "nodes": "rw", "edges": "rw"}]},
        }
    }
    store_path = os.path.join(work_dir, "permissions.json")
    with open(store_path, "w") as f:
        json.dump(store, f)

    with GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=store_path,
    ).start():
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_JIRA})
        )
        requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": CREATE_ADMIN})
        )

        # jira: per-graph false overrides role-level true → denied
        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_COUNT_NODES}),
        )
        assert (
            response.json()["data"] is None or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]

        # admin graph: per-graph true → allowed
        query_admin_count = """query { graph(path: "admin") { countNodes } }"""
        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": query_admin_count}),
        )
        assert "errors" not in response.json(), response.json()
        assert isinstance(response.json()["data"]["graph"]["countNodes"], int)
