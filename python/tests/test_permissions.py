import json
import os
import tempfile
import requests
import jwt
import pytest
from raphtory.graphql import GraphServer, has_permissions_extension

pytestmark = pytest.mark.skipif(
    not has_permissions_extension(),
    reason="raphtory-auth not compiled in (open-source build)",
)

# Reuse the same key pair as test_auth.py
PUB_KEY = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----"""

RAPHTORY = "http://localhost:1736"

ANALYST_JWT = jwt.encode({"a": "ro", "role": "analyst"}, PRIVATE_KEY, algorithm="EdDSA")
ANALYST_HEADERS = {"Authorization": f"Bearer {ANALYST_JWT}"}

ADMIN_JWT = jwt.encode({"a": "rw", "role": "admin"}, PRIVATE_KEY, algorithm="EdDSA")
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_JWT}"}

NO_ROLE_JWT = jwt.encode({"a": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
NO_ROLE_HEADERS = {"Authorization": f"Bearer {NO_ROLE_JWT}"}

QUERY_JIRA = """query { graph(path: "jira") { path } }"""
QUERY_ADMIN = """query { graph(path: "admin") { path } }"""
QUERY_COUNT_NODES = """query { graph(path: "jira") { countNodes } }"""
CREATE_JIRA = """mutation { newGraph(path:"jira", graphType:EVENT) }"""
CREATE_ADMIN = """mutation { newGraph(path:"admin", graphType:EVENT) }"""


def gql(query: str, headers=None) -> dict:
    h = headers if headers is not None else ADMIN_HEADERS
    return requests.post(RAPHTORY, headers=h, data=json.dumps({"query": query})).json()


def create_role(role: str) -> None:
    gql(f'mutation {{ permissions {{ createRole(name: "{role}") {{ success }} }} }}')


def grant_graph(role: str, path: str, permissions: list) -> None:
    perms = "[" + ", ".join(permissions) + "]"
    gql(
        f'mutation {{ permissions {{ grantGraph(role: "{role}", path: "{path}", permissions: {perms}) {{ success }} }} }}'
    )


def grant_namespace(role: str, path: str, permissions: list) -> None:
    perms = "[" + ", ".join(permissions) + "]"
    gql(
        f'mutation {{ permissions {{ grantNamespace(role: "{role}", path: "{path}", permissions: {perms}) {{ success }} }} }}'
    )


def make_server(work_dir: str):
    """Create a GraphServer wired with a permissions store at {work_dir}/permissions.json."""
    return GraphServer(
        work_dir,
        auth_public_key=PUB_KEY,
        permissions_store_path=os.path.join(work_dir, "permissions.json"),
    )


def test_analyst_can_access_permitted_graph():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)
        create_role("analyst")
        create_role("admin")
        grant_graph("analyst", "jira", ["READ"])
        grant_namespace("admin", "*", ["READ"])

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


def test_analyst_cannot_access_denied_graph():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_ADMIN)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])  # only jira, not admin

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_ADMIN})
        )
        assert response.json()["data"] is None
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_admin_can_access_all_graphs():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)
        create_role("admin")
        grant_namespace("admin", "*", ["READ"])

        for query in [QUERY_JIRA, QUERY_ADMIN]:
            response = requests.post(
                RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": query})
            )
            assert "errors" not in response.json(), response.json()


def test_no_role_is_denied_when_policy_is_active():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])

        response = requests.post(
            RAPHTORY, headers=NO_ROLE_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert response.json()["data"] is None
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_empty_store_gives_full_access():
    """With an empty permissions store (no roles configured), authenticated users see everything."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()


def test_introspection_allowed_with_introspect_permission():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ", "INTROSPECT"])

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_COUNT_NODES}),
        )
        assert "errors" not in response.json(), response.json()
        assert isinstance(response.json()["data"]["graph"]["countNodes"], int)


def test_introspection_denied_without_introspect_permission():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])  # READ only, no INTROSPECT

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": QUERY_COUNT_NODES}),
        )
        assert (
            response.json()["data"] is None
            or response.json()["data"]["graph"] is None
        )
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_permissions_update_via_mutation():
    """Granting access via mutation takes effect immediately."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")

        # No grants yet — denied
        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "Access denied" in response.json()["errors"][0]["message"]

        # Grant via mutation
        grant_graph("analyst", "jira", ["READ"])

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


def test_namespace_wildcard_grants_access_to_all_graphs():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)
        create_role("analyst")
        grant_namespace("analyst", "*", ["READ"])

        for query in [QUERY_JIRA, QUERY_ADMIN]:
            response = requests.post(
                RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": query})
            )
            assert "errors" not in response.json(), response.json()


# --- WRITE permission enforcement ---

UPDATE_JIRA = """mutation { updateGraph(path: "jira") { addNode(time: 1, name: "test_node") } }"""
CREATE_JIRA_NS = """mutation { newGraph(path:"team/jira", graphType:EVENT) }"""


def test_admin_bypasses_policy_for_reads():
    """'a':'rw' admin can read any graph even without a role entry in the store."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Policy is active (analyst role exists) but admin has no role entry
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])

        response = requests.post(
            RAPHTORY, headers=ADMIN_HEADERS, data=json.dumps({"query": QUERY_JIRA})
        )
        assert "errors" not in response.json(), response.json()
        assert response.json()["data"]["graph"]["path"] == "jira"


def test_analyst_can_write_with_write_grant():
    """'a':'ro' user with WRITE grant on a specific graph can call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ", "WRITE"])

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": UPDATE_JIRA})
        )
        assert "errors" not in response.json(), response.json()


def test_analyst_cannot_write_without_write_grant():
    """'a':'ro' user with READ-only grant cannot call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])  # READ only, no WRITE

        response = requests.post(
            RAPHTORY, headers=ANALYST_HEADERS, data=json.dumps({"query": UPDATE_JIRA})
        )
        assert (
            response.json()["data"] is None
            or response.json()["data"].get("updateGraph") is None
        )
        assert "errors" in response.json()
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_analyst_can_create_graph_in_namespace():
    """'a':'ro' user with namespace WRITE grant can create a new graph in that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", ["READ", "WRITE"])

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": CREATE_JIRA_NS}),
        )
        assert "errors" not in response.json(), response.json()


def test_analyst_cannot_create_graph_outside_namespace():
    """'a':'ro' user with namespace WRITE grant cannot create a graph outside that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", ["READ", "WRITE"])

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": CREATE_JIRA}),  # "jira" not under "team/"
        )
        assert "errors" in response.json()
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_analyst_cannot_call_permissions_mutations():
    """'a':'ro' user with WRITE grant on a graph cannot manage roles/permissions."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "*", ["READ", "WRITE"])

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps(
                {"query": 'mutation { permissions { createRole(name: "hacker") { success } } }'}
            ),
        )
        assert "errors" in response.json()
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_admin_can_list_roles():
    """'a':'rw' admin can query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = requests.post(
            RAPHTORY,
            headers=ADMIN_HEADERS,
            data=json.dumps({"query": "query { permissions { listRoles } }"}),
        )
        assert "errors" not in response.json(), response.json()
        assert "analyst" in response.json()["data"]["permissions"]["listRoles"]


def test_analyst_cannot_list_roles():
    """'a':'ro' user cannot query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps({"query": "query { permissions { listRoles } }"}),
        )
        assert "errors" in response.json()
        assert "Access denied" in response.json()["errors"][0]["message"]


def test_admin_can_get_role():
    """'a':'rw' admin can query permissions { getRole(...) }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_graph("analyst", "jira", ["READ"])

        response = requests.post(
            RAPHTORY,
            headers=ADMIN_HEADERS,
            data=json.dumps(
                {
                    "query": 'query { permissions { getRole(name: "analyst") { name graphs { path permissions } } } }'
                }
            ),
        )
        assert "errors" not in response.json(), response.json()
        role_data = response.json()["data"]["permissions"]["getRole"]
        assert role_data["name"] == "analyst"
        assert role_data["graphs"][0]["path"] == "jira"


def test_analyst_cannot_get_role():
    """'a':'ro' user cannot query permissions { getRole(...) }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = requests.post(
            RAPHTORY,
            headers=ANALYST_HEADERS,
            data=json.dumps(
                {"query": 'query { permissions { getRole(name: "analyst") { name } } }'}
            ),
        )
        assert "errors" in response.json()
        assert "Access denied" in response.json()["errors"][0]["message"]
