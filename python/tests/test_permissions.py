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


def grant_graph(role: str, path: str, permission: str) -> None:
    gql(
        f'mutation {{ permissions {{ grantGraph(role: "{role}", path: "{path}", permission: {permission}) {{ success }} }} }}'
    )


def grant_namespace(role: str, path: str, permission: str) -> None:
    gql(
        f'mutation {{ permissions {{ grantNamespace(role: "{role}", path: "{path}", permission: {permission}) {{ success }} }} }}'
    )


def grant_graph_filtered_read_only(role: str, path: str, filter_gql: str) -> None:
    """Call grantGraphFilteredReadOnly with a raw GQL filter fragment."""
    resp = gql(
        f'mutation {{ permissions {{ grantGraphFilteredReadOnly(role: "{role}", path: "{path}", filter: {filter_gql}) {{ success }} }} }}'
    )
    assert "errors" not in resp, f"grantGraphFilteredReadOnly failed: {resp}"


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
        grant_graph("analyst", "jira", "READ")
        grant_namespace("admin", "*", "READ")

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_analyst_cannot_access_denied_graph():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_ADMIN)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")  # only jira, not admin

        response = gql(QUERY_ADMIN, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


def test_admin_can_access_all_graphs():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)
        create_role("admin")
        grant_namespace("admin", "*", "READ")

        for query in [QUERY_JIRA, QUERY_ADMIN]:
            response = gql(query, headers=ADMIN_HEADERS)
            assert "errors" not in response, response


def test_no_role_is_denied_when_policy_is_active():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_JIRA, headers=NO_ROLE_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


def test_empty_store_gives_full_access():
    """With an empty permissions store (no roles configured), authenticated users see everything."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response


def test_introspection_allowed_with_introspect_permission():
    """INTROSPECT-only role can call countNodes."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "INTROSPECT")

        response = gql(QUERY_COUNT_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert isinstance(response["data"]["graph"]["countNodes"], int)


def test_read_implies_introspect():
    """READ implies INTROSPECT: a role with READ can call countNodes without an explicit INTROSPECT grant."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")  # READ implies INTROSPECT

        response = gql(QUERY_COUNT_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert isinstance(response["data"]["graph"]["countNodes"], int)


def test_permissions_update_via_mutation():
    """Granting access via mutation takes effect immediately."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")

        # No grants yet — denied
        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "Access denied" in response["errors"][0]["message"]

        # Grant via mutation
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_namespace_wildcard_grants_access_to_all_graphs():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)
        create_role("analyst")
        grant_namespace("analyst", "*", "READ")

        for query in [QUERY_JIRA, QUERY_ADMIN]:
            response = gql(query, headers=ANALYST_HEADERS)
            assert "errors" not in response, response


# --- WRITE permission enforcement ---

UPDATE_JIRA = """query { updateGraph(path: "jira") { addNode(time: 1, name: "test_node") { success } } }"""
CREATE_JIRA_NS = """mutation { newGraph(path:"team/jira", graphType:EVENT) }"""


def test_admin_bypasses_policy_for_reads():
    """'a':'rw' admin can read any graph even without a role entry in the store."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Policy is active (analyst role exists) but admin has no role entry
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_JIRA, headers=ADMIN_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_analyst_can_write_with_write_grant():
    """'a':'ro' user with WRITE grant on a specific graph can call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")

        response = gql(UPDATE_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response


def test_analyst_cannot_write_without_write_grant():
    """'a':'ro' user with READ-only grant cannot call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")  # READ only, no WRITE

        response = gql(UPDATE_JIRA, headers=ANALYST_HEADERS)
        assert (
            response["data"] is None
            or response["data"].get("updateGraph") is None
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_can_create_graph_in_namespace():
    """'a':'ro' user with namespace WRITE grant can create a new graph in that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", "WRITE")

        response = gql(CREATE_JIRA_NS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response


def test_analyst_cannot_create_graph_outside_namespace():
    """'a':'ro' user with namespace WRITE grant cannot create a graph outside that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", "WRITE")

        response = gql(CREATE_JIRA, headers=ANALYST_HEADERS)  # "jira" not under "team/"
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_cannot_call_permissions_mutations():
    """'a':'ro' user with WRITE grant on a graph cannot manage roles/permissions."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "*", "WRITE")

        response = gql(
            'mutation { permissions { createRole(name: "hacker") { success } } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_admin_can_list_roles():
    """'a':'rw' admin can query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql("query { permissions { listRoles } }", headers=ADMIN_HEADERS)
        assert "errors" not in response, response
        assert "analyst" in response["data"]["permissions"]["listRoles"]


def test_analyst_cannot_list_roles():
    """'a':'ro' user cannot query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql("query { permissions { listRoles } }", headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_admin_can_get_role():
    """'a':'rw' admin can query permissions { getRole(...) }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(
            'query { permissions { getRole(name: "analyst") { name graphs { path permission } } } }',
            headers=ADMIN_HEADERS,
        )
        assert "errors" not in response, response
        role_data = response["data"]["permissions"]["getRole"]
        assert role_data["name"] == "analyst"
        assert role_data["graphs"][0]["path"] == "jira"
        assert role_data["graphs"][0]["permission"] == "READ"


def test_analyst_cannot_get_role():
    """'a':'ro' user cannot query permissions { getRole(...) }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql(
            'query { permissions { getRole(name: "analyst") { name } } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_introspect_only_can_call_introspection_fields():
    """A role with only INTROSPECT (no READ) can call countNodes/countEdges/schema/uniqueLayers."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "INTROSPECT")  # no READ

        for query in [
            'query { graph(path: "jira") { countNodes } }',
            'query { graph(path: "jira") { countEdges } }',
            'query { graph(path: "jira") { uniqueLayers } }',
        ]:
            response = gql(query, headers=ANALYST_HEADERS)
            assert "errors" not in response, f"query={query} response={response}"


def test_introspect_only_cannot_read_nodes_or_edges():
    """A role with only INTROSPECT (no READ) is denied access to node/edge data."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Add a node and edge so the graph has data to query
        gql(
            'query { updateGraph(path: "jira") { addNode(time: 1, name: "a") { success } } }'
        )
        gql(
            'query { updateGraph(path: "jira") { addEdge(time: 1, src: "a", dst: "b") { success } } }'
        )
        create_role("analyst")
        grant_graph("analyst", "jira", "INTROSPECT")  # no READ

        for query, expected_field in [
            ('query { graph(path: "jira") { nodes { list { name } } } }', "nodes"),
            (
                'query { graph(path: "jira") { edges { list { src { name } } } } }',
                "edges",
            ),
            ('query { graph(path: "jira") { node(name: "a") { name } } }', "node"),
            (
                'query { graph(path: "jira") { properties { values { key } } } }',
                "properties",
            ),
            (
                'query { graph(path: "jira") { earliestTime { timestamp } } }',
                "earliestTime",
            ),
        ]:
            response = gql(query, headers=ANALYST_HEADERS)
            errors = response.get("errors", [])
            assert errors, f"expected denial for query={query!r}, got: {response}"
            msg = errors[0]["message"]
            assert (
                "Access denied" in msg and expected_field in msg and "read" in msg
            ), f"unexpected error for {expected_field!r}: {msg!r}"


def test_introspect_only_is_denied_without_introspect_or_read():
    """A role with no permissions on a graph is denied even when trying introspection fields."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # analyst has no grant on jira at all

        response = gql(
            'query { graph(path: "jira") { countNodes } }',
            headers=ANALYST_HEADERS,
        )
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_sees_only_filtered_nodes():
    """grantGraphFilteredReadOnly applies a node filter transparently for the role.

    Admin sees all nodes; analyst only sees nodes matching the stored filter.
    Calling grantGraph(READ) clears the filter and restores full access.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        # Create graph and add nodes with a "region" property
        gql(CREATE_JIRA)
        for name, region in [
            ("alice", "us-west"),
            ("bob", "us-east"),
            ("carol", "us-west"),
        ]:
            resp = gql(
                f"""query {{
                    updateGraph(path: "jira") {{
                        addNode(
                            time: 1,
                            name: "{name}",
                            properties: [{{ key: "region", value: {{ str: "{region}" }} }}]
                        ) {{
                            success
                            node {{
                                name
                            }}
                        }}
                    }}
                }}"""
            )
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # Grant filtered read-only: analyst only sees nodes where region = "us-west"
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { property: { name: "region", where: { eq: { str: "us-west" } } } } }',
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'

        # Analyst should only see alice and carol (region=us-west)
        analyst_response = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_names = {
            n["name"]
            for n in analyst_response["data"]["graph"]["nodes"]["list"]
        }
        assert analyst_names == {"alice", "carol"}, f"expected {{alice, carol}}, got {analyst_names}"

        # Admin should see all three nodes (filter is bypassed for "a":"rw")
        admin_response = gql(QUERY_NODES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_names = {
            n["name"]
            for n in admin_response["data"]["graph"]["nodes"]["list"]
        }
        assert admin_names == {"alice", "bob", "carol"}, f"expected all 3 nodes, got {admin_names}"

        # Clear the filter by calling grantGraph(READ) — analyst should now see all nodes
        grant_graph("analyst", "jira", "READ")
        analyst_response_after = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response_after, analyst_response_after
        names_after = {
            n["name"]
            for n in analyst_response_after["data"]["graph"]["nodes"]["list"]
        }
        assert names_after == {"alice", "bob", "carol"}, (
            f"after plain grant, expected all 3 nodes, got {names_after}"
        )


def test_analyst_sees_only_filtered_edges():
    """grantGraphFilteredReadOnly with an edge filter hides edges that don't match.

    Edges with weight >= 5 are visible; edges with weight < 5 are hidden.
    Admin bypasses the filter and sees all edges.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Add three edges: (a->b weight=3), (b->c weight=7), (a->c weight=9)
        for src, dst, weight in [("a", "b", 3), ("b", "c", 7), ("a", "c", 9)]:
            resp = gql(
                f"""query {{
                    updateGraph(path: "jira") {{
                        addEdge(
                            time: 1,
                            src: "{src}",
                            dst: "{dst}",
                            properties: [{{ key: "weight", value: {{ i64: {weight} }} }}]
                        ) {{
                            success
                            edge {{
                                src {{ name }}
                                dst {{ name }}
                            }}
                        }}
                    }}
                }}"""
            )
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # Only show edges where weight >= 5
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ edge: { property: { name: "weight", where: { ge: { i64: 5 } } } } }',
        )

        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'

        analyst_response = gql(QUERY_EDGES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in analyst_response["data"]["graph"]["edges"]["list"]
        }
        assert analyst_edges == {("b", "c"), ("a", "c")}, (
            f"expected only heavy edges, got {analyst_edges}"
        )

        # Admin sees all three edges
        admin_response = gql(QUERY_EDGES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in admin_response["data"]["graph"]["edges"]["list"]
        }
        assert admin_edges == {("a", "b"), ("b", "c"), ("a", "c")}, (
            f"expected all edges for admin, got {admin_edges}"
        )


def test_analyst_sees_only_graph_filter_window():
    """grantGraphFilteredReadOnly with a graph-level window filter restricts the temporal view.

    Nodes added inside the window [5, 15) are visible; those outside are not.
    Admin bypasses the filter and sees all nodes.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Add nodes at different timestamps: t=1 (outside), t=10 (inside), t=20 (outside)
        for name, t in [("early", 1), ("middle", 10), ("late", 20)]:
            resp = gql(
                f"""query {{
                    updateGraph(path: "jira") {{
                        addNode(time: {t}, name: "{name}") {{
                            success
                            node {{
                                name
                            }}
                        }}
                    }}
                }}"""
            )
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # Window [5, 15) — only "middle" (t=10) falls inside
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ graph: { window: { start: 5, end: 15 } } }",
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'

        analyst_response = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_names = {
            n["name"]
            for n in analyst_response["data"]["graph"]["nodes"]["list"]
        }
        assert analyst_names == {"middle"}, (
            f"expected only 'middle' in window, got {analyst_names}"
        )

        # Admin sees all three nodes
        admin_response = gql(QUERY_NODES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_names = {
            n["name"]
            for n in admin_response["data"]["graph"]["nodes"]["list"]
        }
        assert admin_names == {"early", "middle", "late"}, (
            f"expected all nodes for admin, got {admin_names}"
        )
