import json
import os
import tempfile
import requests
import jwt
import pytest
from raphtory.graphql import GraphServer, RaphtoryClient, has_permissions_extension

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

ANALYST_JWT = jwt.encode(
    {"access": "ro", "role": "analyst"}, PRIVATE_KEY, algorithm="EdDSA"
)
ANALYST_HEADERS = {"Authorization": f"Bearer {ANALYST_JWT}"}

ADMIN_JWT = jwt.encode(
    {"access": "rw", "role": "admin"}, PRIVATE_KEY, algorithm="EdDSA"
)
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_JWT}"}

NO_ROLE_JWT = jwt.encode({"access": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
NO_ROLE_HEADERS = {"Authorization": f"Bearer {NO_ROLE_JWT}"}

QUERY_JIRA = """query { graph(path: "jira") { path } }"""
QUERY_ADMIN = """query { graph(path: "admin") { path } }"""
QUERY_NS_GRAPHS = """query { root { graphs { list { path } } } }"""
QUERY_NS_CHILDREN = """query { root { children { list { path } } } }"""
QUERY_META_JIRA = """query { graphMetadata(path: "jira") { path nodeCount } }"""
CREATE_JIRA = """mutation { newGraph(path:"jira", graphType:EVENT) }"""
CREATE_ADMIN = """mutation { newGraph(path:"admin", graphType:EVENT) }"""
CREATE_TEAM_JIRA = """mutation { newGraph(path:"team/jira", graphType:EVENT) }"""
CREATE_TEAM_CONFLUENCE = (
    """mutation { newGraph(path:"team/confluence", graphType:EVENT) }"""
)
CREATE_DEEP = """mutation { newGraph(path:"a/b/c", graphType:EVENT) }"""
QUERY_TEAM_JIRA = """query { graph(path: "team/jira") { path } }"""
QUERY_TEAM_GRAPHS = """query { namespace(path: "team") { graphs { list { path } } } }"""
QUERY_A_CHILDREN = """query { namespace(path: "a") { children { list { path } } } }"""


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


def revoke_graph(role: str, path: str) -> None:
    gql(
        f'mutation {{ permissions {{ revokeGraph(role: "{role}", path: "{path}") {{ success }} }} }}'
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
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_analyst_cannot_access_denied_graph():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_ADMIN)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")  # only jira, not admin

        # "admin" graph is silently null — analyst has no namespace INTROSPECT, so
        # existence of "admin" is not revealed.
        response = gql(QUERY_ADMIN, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_admin_can_access_all_graphs():
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_ADMIN)

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
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_unknown_role_is_denied_when_policy_is_active():
    """JWT has a role claim but that role does not exist in the store → Denied.

    Distinct from test_no_role_is_denied_when_policy_is_active: here the JWT
    does carry a role claim ('analyst'), but 'analyst' was never created in the
    store. Both paths deny, but via different branches of the policy flowchart.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Make the store non-empty with a different role — but never create "analyst"
        create_role("other_team")

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)  # JWT says role="analyst"
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_empty_store_denies_non_admin():
    """With an empty permissions store (no roles configured), non-admin users are denied."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_empty_store_allows_admin():
    """With an empty permissions store, admin (rw JWT) still gets full access."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)

        response = gql(QUERY_JIRA, headers=ADMIN_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_introspection_allowed_with_introspect_permission():
    """Namespace INTROSPECT makes graphs visible in listings but graph() is denied."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "INTROSPECT")

        # Namespace listing shows the graph as MetaGraph
        response = gql(QUERY_TEAM_GRAPHS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [g["path"] for g in response["data"]["namespace"]["graphs"]["list"]]
        assert "team/jira" in paths

        # graph() resolver returns null — INTROSPECT does not grant data access
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_read_implies_introspect():
    """READ also shows the graph in namespace listings (implies INTROSPECT)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_NS_GRAPHS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [g["path"] for g in response["data"]["root"]["graphs"]["list"]]
        assert "jira" in paths


def test_permissions_update_via_mutation():
    """Granting access via mutation takes effect immediately."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")

        # No grants yet — graph returns null (indistinguishable from "graph not found")
        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None

        # Grant via mutation
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "jira"


def test_namespace_grant_does_not_cover_root_level_graphs():
    """Namespace grants only apply to graphs within that namespace; root-level graphs require explicit graph grants."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace(
            "analyst", "team", "READ"
        )  # covers team/jira but not root-level jira

        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response

        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert (
            response["data"]["graph"] is None
        )  # root-level graph not covered by namespace grant


# --- WRITE permission enforcement ---

UPDATE_JIRA = """query { updateGraph(path: "jira") { addNode(time: 1, name: "test_node") { success } } }"""
CREATE_JIRA_NS = """mutation { newGraph(path:"team/jira", graphType:EVENT) }"""


def test_admin_bypasses_policy_for_reads():
    """'access':'rw' admin can read any graph even without a role entry in the store."""
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
    """'access':'ro' user with WRITE grant on a specific graph can call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")

        response = gql(UPDATE_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response


def test_analyst_cannot_write_without_write_grant():
    """'access':'ro' user with READ-only grant cannot call updateGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")  # READ only, no WRITE

        response = gql(UPDATE_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None or response["data"].get("updateGraph") is None
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_can_create_graph_in_namespace():
    """'access':'ro' user with namespace WRITE grant can create a new graph in that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", "WRITE")

        response = gql(CREATE_JIRA_NS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["newGraph"] is True


def test_analyst_cannot_create_graph_outside_namespace():
    """'access':'ro' user with namespace WRITE grant cannot create a graph outside that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team/", "WRITE")

        response = gql(CREATE_JIRA, headers=ANALYST_HEADERS)  # "jira" not under "team/"
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "jira" was not created as a side effect
        ns_graphs = gql(QUERY_NS_GRAPHS)["data"]["root"]["graphs"]["list"]
        assert "jira" not in [g["path"] for g in ns_graphs]


def test_analyst_cannot_call_permissions_mutations():
    """'access':'ro' user with WRITE grant on a graph cannot manage roles/permissions."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(
            'mutation { permissions { createRole(name: "hacker") { success } } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "hacker" role was not created as a side effect
        roles = gql("query { permissions { listRoles } }")["data"]["permissions"][
            "listRoles"
        ]
        assert "hacker" not in roles


def test_admin_can_list_roles():
    """'access':'rw' admin can query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql("query { permissions { listRoles } }", headers=ADMIN_HEADERS)
        assert "errors" not in response, response
        assert "analyst" in response["data"]["permissions"]["listRoles"]


def test_analyst_cannot_list_roles():
    """'access':'ro' user cannot query permissions { listRoles }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql("query { permissions { listRoles } }", headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_admin_can_get_role():
    """'access':'rw' admin can query permissions { getRole(...) }."""
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
    """'access':'ro' user cannot query permissions { getRole(...) }."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")

        response = gql(
            'query { permissions { getRole(name: "analyst") { name } } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_introspect_only_cannot_access_graph_data():
    """Namespace INTROSPECT is denied by graph() — READ is required to access graph data."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "INTROSPECT")  # no READ

        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_no_grant_hidden_from_namespace_and_graph():
    """A role with no namespace INTROSPECT sees graph() as null, not an 'Access denied' error.

    Returning an error would leak that the graph exists. Null is indistinguishable from
    'graph not found'. An error is only appropriate when the role already has INTROSPECT
    on the namespace (and therefore can list the graph name anyway).
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # analyst has no grant at all

        # graph() returns null silently — does not reveal the graph exists
        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None

        # namespace listing hides it
        response = gql(QUERY_NS_GRAPHS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [g["path"] for g in response["data"]["root"]["graphs"]["list"]]
        assert "jira" not in paths


def test_grantgraph_introspect_rejected():
    """grantGraph with INTROSPECT permission is rejected — INTROSPECT is namespace-only."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")

        response = gql(
            'mutation { permissions { grantGraph(role: "analyst", path: "jira", permission: INTROSPECT) { success } } }'
        )
        assert "errors" in response
        assert (
            "INTROSPECT cannot be granted on a graph"
            in response["errors"][0]["message"]
        )


def test_graph_metadata_allowed_with_introspect():
    """graphMetadata is accessible with INTROSPECT permission (namespace grant)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "INTROSPECT")

        response = gql(
            'query { graphMetadata(path: "team/jira") { path nodeCount } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" not in response, response
        assert response["data"]["graphMetadata"]["path"] == "team/jira"

        # graph() returns null — INTROSPECT does not grant data access
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_graph_metadata_allowed_with_read():
    """graphMetadata is also accessible with READ."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(QUERY_META_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graphMetadata"]["path"] == "jira"


def test_graph_metadata_denied_without_grant():
    """graphMetadata is denied when the role has no grant on the graph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # no grant on jira

        response = gql(QUERY_META_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graphMetadata"] is None


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
            resp = gql(f"""query {{
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
                }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # Grant filtered read-only: analyst only sees nodes where region = "us-west"
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { filter: { property: { name: "region", where: { eq: { str: "us-west" } } } } } }',
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'

        # Analyst should only see alice and carol (region=us-west)
        analyst_response = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_names = {
            n["name"] for n in analyst_response["data"]["graph"]["nodes"]["list"]
        }
        assert analyst_names == {
            "alice",
            "carol",
        }, f"expected {{alice, carol}}, got {analyst_names}"

        # Admin should see all three nodes (filter is bypassed for "access":"rw")
        admin_response = gql(QUERY_NODES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_names = {
            n["name"] for n in admin_response["data"]["graph"]["nodes"]["list"]
        }
        assert admin_names == {
            "alice",
            "bob",
            "carol",
        }, f"expected all 3 nodes, got {admin_names}"

        # Clear the filter by calling grantGraph(READ) — analyst should now see all nodes
        grant_graph("analyst", "jira", "READ")
        analyst_response_after = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response_after, analyst_response_after
        names_after = {
            n["name"] for n in analyst_response_after["data"]["graph"]["nodes"]["list"]
        }
        assert names_after == {
            "alice",
            "bob",
            "carol",
        }, f"after plain grant, expected all 3 nodes, got {names_after}"


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
            resp = gql(f"""query {{
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
                }}""")
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # Only show edges where weight >= 5
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ edge: { filter: { property: { name: "weight", where: { ge: { i64: 5 } } } } } }',
        )

        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'

        analyst_response = gql(QUERY_EDGES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in analyst_response["data"]["graph"]["edges"]["list"]
        }
        assert analyst_edges == {
            ("b", "c"),
            ("a", "c"),
        }, f"expected only heavy edges, got {analyst_edges}"

        # Admin sees all three edges
        admin_response = gql(QUERY_EDGES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in admin_response["data"]["graph"]["edges"]["list"]
        }
        assert admin_edges == {
            ("a", "b"),
            ("b", "c"),
            ("a", "c"),
        }, f"expected all edges for admin, got {admin_edges}"


def test_raphtory_client_analyst_can_query_permitted_graph():
    """RaphtoryClient with analyst role can query a graph it has READ access to."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        result = client.query(QUERY_JIRA)
        assert result["graph"]["path"] == "jira"


def test_raphtory_client_analyst_denied_unpermitted_graph():
    """RaphtoryClient with analyst role gets null for a graph it has no grant for."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # No grant on jira — graph returns null (indistinguishable from "graph not found")

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        response = client.query(QUERY_JIRA)
        assert response["graph"] is None


def test_raphtory_client_analyst_write_with_write_grant():
    """RaphtoryClient with analyst role and WRITE grant can add nodes via remote_graph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        client.remote_graph("jira").add_node(1, "client_node")

        client2 = RaphtoryClient(url=RAPHTORY, token=ADMIN_JWT)
        received = client2.receive_graph("jira")
        assert received.node("client_node") is not None


def test_raphtory_client_analyst_write_denied_without_write_grant():
    """RaphtoryClient with analyst role and READ-only grant cannot add nodes via remote_graph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        with pytest.raises(Exception, match="Access denied"):
            client.remote_graph("jira").add_node(1, "client_node")


def test_receive_graph_requires_read():
    """receive_graph (graph download) requires at least READ; namespace INTROSPECT is not enough."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")

        # No grant — looks like the graph doesn't exist (no information leakage)
        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        with pytest.raises(Exception, match="does not exist"):
            client.receive_graph("team/jira")

        # Namespace INTROSPECT only — also denied for receive_graph, but now reveals access denied
        grant_namespace("analyst", "team", "INTROSPECT")
        with pytest.raises(Exception, match="Access denied"):
            client.receive_graph("team/jira")

        # READ — allowed
        grant_namespace("analyst", "team", "READ")
        g = client.receive_graph("team/jira")
        assert g is not None


def test_receive_graph_without_introspect_hides_existence():
    """Without namespace INTROSPECT, receive_graph acts as if the graph does not exist.

    This prevents information leakage: a role without any grants cannot distinguish
    between 'graph does not exist' and 'graph exists but you are denied'.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)

        # No grants at all — error must be indistinguishable from a missing graph
        with pytest.raises(Exception, match="does not exist") as exc_no_grant:
            client.receive_graph("team/jira")

        # Compare with a truly non-existent graph — error should look the same
        with pytest.raises(Exception, match="does not exist") as exc_missing:
            client.receive_graph("team/nonexistent")

        assert "Access denied" not in str(exc_no_grant.value)
        assert "Access denied" not in str(exc_missing.value)


def test_receive_graph_with_filtered_access():
    """receive_graph with grantGraphFilteredReadOnly returns a materialized view of the filtered graph.

    The downloaded graph should only contain nodes/edges that pass the stored filter,
    not the full unfiltered graph.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region in [
            ("alice", "us-west"),
            ("bob", "us-east"),
            ("carol", "us-west"),
        ]:
            resp = gql(f"""query {{
                    updateGraph(path: "jira") {{
                        addNode(
                            time: 1,
                            name: "{name}",
                            properties: [{{ key: "region", value: {{ str: "{region}" }} }}]
                        ) {{ success }}
                    }}
                }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { filter: { property: { name: "region", where: { eq: { str: "us-west" } } } } } }',
        )

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        received = client.receive_graph("jira")

        names = {n.name for n in received.nodes}
        assert names == {"alice", "carol"}, f"Expected only us-west nodes, got: {names}"
        assert "bob" not in names


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
            resp = gql(f"""query {{
                    updateGraph(path: "jira") {{
                        addNode(time: {t}, name: "{name}") {{
                            success
                            node {{
                                name
                            }}
                        }}
                    }}
                }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # Window [5, 15) — only "middle" (t=10) falls inside
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ graph: { filter: { window: { start: 5, end: 15 } } } }",
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'

        analyst_response = gql(QUERY_NODES, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_response, analyst_response
        analyst_names = {
            n["name"] for n in analyst_response["data"]["graph"]["nodes"]["list"]
        }
        assert analyst_names == {
            "middle"
        }, f"expected only 'middle' in window, got {analyst_names}"

        # Admin sees all three nodes
        admin_response = gql(QUERY_NODES, headers=ADMIN_HEADERS)
        assert "errors" not in admin_response, admin_response
        admin_names = {
            n["name"] for n in admin_response["data"]["graph"]["nodes"]["list"]
        }
        assert admin_names == {
            "early",
            "middle",
            "late",
        }, f"expected all nodes for admin, got {admin_names}"


# --- Filter composition (And / Or) tests ---


def test_filter_and_node_node():
    """And([node, node]): both node predicates must match (intersection)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region, role in [
            ("alice", "us-west", "admin"),
            ("bob", "us-east", "admin"),
            ("carol", "us-west", "user"),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(
                        time: 1, name: "{name}",
                        properties: [
                            {{ key: "region", value: {{ str: "{region}" }} }},
                            {{ key: "role", value: {{ str: "{role}" }} }}
                        ]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # region=us-west AND role=admin → only alice
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ node: { filter: { and: ["
            '{ property: { name: "region", where: { eq: { str: "us-west" } } } },'
            '{ property: { name: "role", where: { eq: { str: "admin" } } } }'
            "] } } }",
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'
        analyst_names = {
            n["name"]
            for n in gql(QUERY_NODES, headers=ANALYST_HEADERS)["data"]["graph"][
                "nodes"
            ]["list"]
        }
        assert analyst_names == {"alice"}, f"expected only alice, got {analyst_names}"


def test_filter_and_edge_edge():
    """And([edge, edge]): both edge predicates must match (intersection)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for src, dst, weight, kind in [
            ("a", "b", 3, "follows"),
            ("b", "c", 7, "mentions"),
            ("a", "c", 9, "follows"),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addEdge(
                        time: 1, src: "{src}", dst: "{dst}",
                        properties: [
                            {{ key: "weight", value: {{ i64: {weight} }} }},
                            {{ key: "kind", value: {{ str: "{kind}" }} }}
                        ]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # weight >= 5 AND kind=follows → only (a,c) weight=9 follows
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ edge: { filter: { and: ["
            '{ property: { name: "weight", where: { ge: { i64: 5 } } } },'
            '{ property: { name: "kind", where: { eq: { str: "follows" } } } }'
            "] } } }",
        )

        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'
        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in gql(QUERY_EDGES, headers=ANALYST_HEADERS)["data"]["graph"][
                "edges"
            ]["list"]
        }
        assert analyst_edges == {
            ("a", "c")
        }, f"expected only (a,c), got {analyst_edges}"


def test_filter_and_graph_graph():
    """graph window filter: only nodes within the window [5,15) are visible."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, t in [("early", 1), ("middle", 10), ("late", 20)]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(time: {t}, name: "{name}") {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # window [5,15) → only middle (t=10)
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ graph: { filter: { window: { start: 5, end: 15 } } } }",
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'
        analyst_names = {
            n["name"]
            for n in gql(QUERY_NODES, headers=ANALYST_HEADERS)["data"]["graph"][
                "nodes"
            ]["list"]
        }
        assert analyst_names == {"middle"}, f"expected only middle, got {analyst_names}"


def test_filter_and_node_edge():
    """And([node, edge]): node filter applied first restricts nodes (and their edges), then edge filter further restricts."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region in [
            ("alice", "us-west"),
            ("bob", "us-east"),
            ("carol", "us-west"),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(
                        time: 1, name: "{name}",
                        properties: [{{ key: "region", value: {{ str: "{region}" }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        for src, dst, weight in [
            ("alice", "bob", 3),
            ("alice", "carol", 7),
            ("bob", "carol", 9),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addEdge(
                        time: 1, src: "{src}", dst: "{dst}",
                        properties: [{{ key: "weight", value: {{ i64: {weight} }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # Node(us-west) applied first: bob hidden, bob's edges hidden.
        # Then Edge(weight≥5): of remaining edges (alice→carol weight=7), only alice→carol passes.
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { filter: { property: { name: "region", where: { eq: { str: "us-west" } } } } },'
            ' edge: { filter: { property: { name: "weight", where: { ge: { i64: 5 } } } } } }',
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'
        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'

        analyst_names = {
            n["name"]
            for n in gql(QUERY_NODES, headers=ANALYST_HEADERS)["data"]["graph"][
                "nodes"
            ]["list"]
        }
        assert analyst_names == {
            "alice",
            "carol",
        }, f"expected us-west nodes, got {analyst_names}"

        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in gql(QUERY_EDGES, headers=ANALYST_HEADERS)["data"]["graph"][
                "edges"
            ]["list"]
        }
        # Sequential And: Node(us-west) hides bob and bob's edges, then Edge(weight≥5) keeps alice→carol (7).
        assert analyst_edges == {
            ("alice", "carol"),
        }, f"expected only (alice,carol), got {analyst_edges}"


def test_filter_and_node_graph():
    """And([node, graph]): node property filter combined with a graph window."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region, t in [
            ("alice", "us-west", 1),
            ("bob", "us-west", 10),
            ("carol", "us-east", 10),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(
                        time: {t}, name: "{name}",
                        properties: [{{ key: "region", value: {{ str: "{region}" }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # window [5,15): bob(t=10) + carol(t=10); then node us-west → only bob
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ graph: { filter: { window: { start: 5, end: 15 } } },"
            ' node: { filter: { property: { name: "region", where: { eq: { str: "us-west" } } } } } }',
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'
        analyst_names = {
            n["name"]
            for n in gql(QUERY_NODES, headers=ANALYST_HEADERS)["data"]["graph"][
                "nodes"
            ]["list"]
        }
        assert analyst_names == {"bob"}, f"expected only bob, got {analyst_names}"


def test_filter_and_edge_graph():
    """And([edge, graph]): edge property filter combined with a graph window."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for src, dst, weight, t in [
            ("a", "b", 3, 1),
            ("b", "c", 7, 10),
            ("a", "c", 9, 20),
        ]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addEdge(
                        time: {t}, src: "{src}", dst: "{dst}",
                        properties: [{{ key: "weight", value: {{ i64: {weight} }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # window [5,15): b→c(t=10); then edge weight≥5 → b→c(weight=7) passes
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ graph: { filter: { window: { start: 5, end: 15 } } },"
            ' edge: { filter: { property: { name: "weight", where: { ge: { i64: 5 } } } } } }',
        )

        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'
        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in gql(QUERY_EDGES, headers=ANALYST_HEADERS)["data"]["graph"][
                "edges"
            ]["list"]
        }
        assert analyst_edges == {
            ("b", "c")
        }, f"expected only (b,c), got {analyst_edges}"


def test_filter_or_node_node():
    """Or([node, node]): nodes matching either predicate are visible (union)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region in [("alice", "us-west"), ("bob", "us-east"), ("carol", "eu")]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(
                        time: 1, name: "{name}",
                        properties: [{{ key: "region", value: {{ str: "{region}" }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        # us-west OR us-east → alice + bob; carol(eu) filtered out
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ node: { filter: { or: ["
            '{ property: { name: "region", where: { eq: { str: "us-west" } } } },'
            '{ property: { name: "region", where: { eq: { str: "us-east" } } } }'
            "] } } }",
        )

        QUERY_NODES = 'query { graph(path: "jira") { nodes { list { name } } } }'
        analyst_names = {
            n["name"]
            for n in gql(QUERY_NODES, headers=ANALYST_HEADERS)["data"]["graph"][
                "nodes"
            ]["list"]
        }
        assert analyst_names == {
            "alice",
            "bob",
        }, f"expected alice+bob, got {analyst_names}"


def test_filter_or_edge_edge():
    """Or([edge, edge]): edges matching either predicate are visible (union)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for src, dst, weight in [("a", "b", 3), ("b", "c", 7), ("a", "c", 9)]:
            resp = gql(f"""query {{
                updateGraph(path: "jira") {{
                    addEdge(
                        time: 1, src: "{src}", dst: "{dst}",
                        properties: [{{ key: "weight", value: {{ i64: {weight} }} }}]
                    ) {{ success }}
                }}
            }}""")
            assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        # weight=3 OR weight=9 → (a,b) + (a,c); (b,c) weight=7 filtered out
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            "{ edge: { filter: { or: ["
            '{ property: { name: "weight", where: { eq: { i64: 3 } } } },'
            '{ property: { name: "weight", where: { eq: { i64: 9 } } } }'
            "] } } }",
        )

        QUERY_EDGES = 'query { graph(path: "jira") { edges { list { src { name } dst { name } } } } }'
        analyst_edges = {
            (e["src"]["name"], e["dst"]["name"])
            for e in gql(QUERY_EDGES, headers=ANALYST_HEADERS)["data"]["graph"][
                "edges"
            ]["list"]
        }
        assert analyst_edges == {
            ("a", "b"),
            ("a", "c"),
        }, f"expected (a,b)+(a,c), got {analyst_edges}"


# --- Property redaction tests ---


def test_analyst_node_hidden_property_not_visible():
    """hiddenProperties strips a node temporal property from responses for the role.

    Admin still sees all properties; analyst sees the node but not the hidden property.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        resp = gql("""query {
            updateGraph(path: "jira") {
                addNode(time: 1, name: "alice",
                    properties: [
                        { key: "salary", value: { i64: 100000 } },
                        { key: "region", value: { str: "us-west" } }
                    ]
                ) { success }
            }
        }""")
        assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { hiddenProperties: ["salary"] } }',
        )

        QUERY = 'query { graph(path: "jira") { nodes { list { name properties { keys } } } } }'

        # Analyst sees the node but salary is absent from keys
        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        node = analyst_resp["data"]["graph"]["nodes"]["list"][0]
        assert "salary" not in node["properties"]["keys"], (
            f"expected salary hidden, got keys: {node['properties']['keys']}"
        )
        assert "region" in node["properties"]["keys"], "region should still be visible"

        # Admin sees all properties
        admin_resp = gql(QUERY, headers=ADMIN_HEADERS)
        assert "errors" not in admin_resp, admin_resp
        admin_node = admin_resp["data"]["graph"]["nodes"]["list"][0]
        assert "salary" in admin_node["properties"]["keys"], "admin should see salary"


def test_analyst_node_hidden_metadata_not_visible():
    """hiddenMetadata strips a node metadata key from responses for the role."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        resp = gql("""query {
            updateGraph(path: "jira") {
                addNode(time: 1, name: "alice") {
                    success
                    addMetadata(properties: [
                        { key: "ssn", value: { str: "123-45-6789" } },
                        { key: "dept", value: { str: "eng" } }
                    ])
                }
            }
        }""")
        assert resp["data"]["updateGraph"]["addNode"]["success"] is True, resp
        assert resp["data"]["updateGraph"]["addNode"]["addMetadata"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { hiddenMetadata: ["ssn"] } }',
        )

        QUERY = 'query { graph(path: "jira") { nodes { list { name metadata { keys } } } } }'

        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        node = analyst_resp["data"]["graph"]["nodes"]["list"][0]
        assert "ssn" not in node["metadata"]["keys"], (
            f"expected ssn hidden, got: {node['metadata']['keys']}"
        )
        assert "dept" in node["metadata"]["keys"], "dept should still be visible"

        admin_resp = gql(QUERY, headers=ADMIN_HEADERS)
        assert "errors" not in admin_resp, admin_resp
        admin_node = admin_resp["data"]["graph"]["nodes"]["list"][0]
        assert "ssn" in admin_node["metadata"]["keys"], "admin should see ssn"


def test_analyst_edge_hidden_property_not_visible():
    """hiddenProperties strips an edge temporal property from responses for the role."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        resp = gql("""query {
            updateGraph(path: "jira") {
                addEdge(time: 1, src: "a", dst: "b",
                    properties: [
                        { key: "salary_delta", value: { i64: 5000 } },
                        { key: "weight", value: { i64: 3 } }
                    ]
                ) { success }
            }
        }""")
        assert resp["data"]["updateGraph"]["addEdge"]["success"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ edge: { hiddenProperties: ["salary_delta"] } }',
        )

        QUERY = 'query { graph(path: "jira") { edges { list { src { name } dst { name } properties { keys } } } } }'

        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        edge = analyst_resp["data"]["graph"]["edges"]["list"][0]
        assert "salary_delta" not in edge["properties"]["keys"], (
            f"expected salary_delta hidden, got: {edge['properties']['keys']}"
        )
        assert "weight" in edge["properties"]["keys"], "weight should still be visible"

        admin_resp = gql(QUERY, headers=ADMIN_HEADERS)
        assert "errors" not in admin_resp, admin_resp
        admin_edge = admin_resp["data"]["graph"]["edges"]["list"][0]
        assert "salary_delta" in admin_edge["properties"]["keys"], "admin should see salary_delta"


def test_redaction_combined_with_row_filter():
    """Row filter and property redaction compose correctly.

    Analyst sees only us-west nodes AND salary is hidden on those nodes.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        for name, region in [("alice", "us-west"), ("bob", "us-east")]:
            gql(f"""query {{
                updateGraph(path: "jira") {{
                    addNode(time: 1, name: "{name}",
                        properties: [
                            {{ key: "region", value: {{ str: "{region}" }} }},
                            {{ key: "salary", value: {{ i64: 50000 }} }}
                        ]
                    ) {{ success }}
                }}
            }}""")

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ node: { filter: { property: { name: "region", where: { eq: { str: "us-west" } } } }, hiddenProperties: ["salary"] } }',
        )

        QUERY = 'query { graph(path: "jira") { nodes { list { name properties { keys } } } } }'

        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        nodes = analyst_resp["data"]["graph"]["nodes"]["list"]
        names = {n["name"] for n in nodes}
        assert names == {"alice"}, f"expected only alice (us-west), got {names}"
        alice = next(n for n in nodes if n["name"] == "alice")
        assert "salary" not in alice["properties"]["keys"], "salary should be hidden"
        assert "region" in alice["properties"]["keys"], "region should be visible"


def test_analyst_graph_hidden_property_not_visible():
    """hiddenProperties on graph-level strips graph temporal properties for the role."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        resp = gql("""query {
            updateGraph(path: "jira") {
                addProperties(t: 1, properties: [
                    { key: "internal_id", value: { i64: 42 } },
                    { key: "version", value: { i64: 1 } }
                ])
            }
        }""")
        assert resp["data"]["updateGraph"]["addProperties"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ graph: { hiddenProperties: ["internal_id"] } }',
        )

        QUERY = 'query { graph(path: "jira") { properties { keys } } }'

        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        keys = analyst_resp["data"]["graph"]["properties"]["keys"]
        assert "internal_id" not in keys, f"expected internal_id hidden, got: {keys}"
        assert "version" in keys, "version should still be visible"

        admin_resp = gql(QUERY, headers=ADMIN_HEADERS)
        assert "errors" not in admin_resp, admin_resp
        admin_keys = admin_resp["data"]["graph"]["properties"]["keys"]
        assert "internal_id" in admin_keys, "admin should see internal_id"


def test_analyst_graph_hidden_metadata_not_visible():
    """hiddenMetadata on graph-level strips graph metadata for the role."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        resp = gql("""query {
            updateGraph(path: "jira") {
                addMetadata(properties: [
                    { key: "secret_token", value: { str: "abc123" } },
                    { key: "owner", value: { str: "platform-team" } }
                ])
            }
        }""")
        assert resp["data"]["updateGraph"]["addMetadata"] is True, resp

        create_role("analyst")
        grant_graph_filtered_read_only(
            "analyst",
            "jira",
            '{ graph: { hiddenMetadata: ["secret_token"] } }',
        )

        QUERY = 'query { graph(path: "jira") { metadata { keys } } }'

        analyst_resp = gql(QUERY, headers=ANALYST_HEADERS)
        assert "errors" not in analyst_resp, analyst_resp
        keys = analyst_resp["data"]["graph"]["metadata"]["keys"]
        assert "secret_token" not in keys, f"expected secret_token hidden, got: {keys}"
        assert "owner" in keys, "owner should still be visible"

        admin_resp = gql(QUERY, headers=ADMIN_HEADERS)
        assert "errors" not in admin_resp, admin_resp
        admin_keys = admin_resp["data"]["graph"]["metadata"]["keys"]
        assert "secret_token" in admin_keys, "admin should see secret_token"


# --- Namespace permission tests ---


def test_namespace_introspect_shows_graphs_in_listing():
    """grantNamespace INTROSPECT: graphs appear in namespace listing but graph() is denied."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "INTROSPECT")

        # Graphs visible as MetaGraph in namespace listing
        response = gql(QUERY_TEAM_GRAPHS, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [g["path"] for g in response["data"]["namespace"]["graphs"]["list"]]
        assert "team/jira" in paths

        # Direct graph access returns null — INTROSPECT does not grant data access.
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"] is None


def test_namespace_read_exposes_graphs():
    """grantNamespace READ: graphs in the namespace are fully accessible via graph()."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "READ")

        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "team/jira"


def test_child_namespace_restriction_overrides_parent():
    """More-specific child namespace grant overrides a broader parent grant.

    team       → READ  (parent)
    team/restricted → INTROSPECT  (child — more specific, should win)

    Graphs under team/jira are reachable via READ (only parent matches).
    Graphs under team/restricted/ are only introspectable — the child INTROSPECT
    entry overrides the parent READ, so graph() is denied there.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        gql("""mutation { newGraph(path:"team/restricted/secret", graphType:EVENT) }""")
        create_role("analyst")
        grant_namespace("analyst", "team", "READ")
        grant_namespace("analyst", "team/restricted", "INTROSPECT")

        # team/jira: only matched by "team" → READ — direct access allowed
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["graph"]["path"] == "team/jira"

        # team/restricted/secret: "team/restricted" is the most specific match → INTROSPECT only
        response = gql(
            """query { graph(path: "team/restricted/secret") { path } }""",
            headers=ANALYST_HEADERS,
        )
        assert "errors" not in response, response
        assert response["data"]["graph"] is None

        # But team/restricted/secret should still appear in the namespace listing
        response = gql(
            """query { namespace(path: "team/restricted") { graphs { list { path } } } }""",
            headers=ANALYST_HEADERS,
        )
        assert "errors" not in response, response
        paths = [g["path"] for g in response["data"]["namespace"]["graphs"]["list"]]
        assert "team/restricted/secret" in paths


def test_discover_derivation():
    """grantGraph READ on a namespaced graph → ancestor namespace gets DISCOVER (visible in children)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "READ")  # no explicit namespace grant

        # "team" namespace appears in root children due to DISCOVER derivation
        response = gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [n["path"] for n in response["data"]["root"]["children"]["list"]]
        assert "team" in paths


def test_discover_revoked_when_only_child_revoked():
    """Revoking the only child READ grant removes DISCOVER from the parent namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "READ")

        paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "team" in paths  # baseline: DISCOVER present

        revoke_graph("analyst", "team/jira")

        paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "team" not in paths  # DISCOVER gone


def test_discover_stays_when_one_of_two_children_revoked():
    """DISCOVER persists while at least one child grant remains; clears only when all are revoked."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        gql(CREATE_TEAM_CONFLUENCE)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "READ")
        grant_graph("analyst", "team/confluence", "READ")

        revoke_graph("analyst", "team/jira")
        paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "team" in paths  # still visible via team/confluence

        revoke_graph("analyst", "team/confluence")
        paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "team" not in paths  # now gone


def test_discover_stays_when_parent_has_explicit_namespace_read():
    """Revoking a child graph READ does not remove an explicit namespace READ on the parent."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "READ")
        grant_namespace("analyst", "team", "READ")  # explicit, higher than DISCOVER

        revoke_graph("analyst", "team/jira")

        paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "team" in paths  # still visible via explicit namespace READ


def test_discover_revoked_for_nested_namespaces():
    """Revoking the only deep grant removes DISCOVER from all ancestor namespaces."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_DEEP)
        create_role("analyst")
        grant_graph("analyst", "a/b/c", "READ")  # "a" and "a/b" both get DISCOVER

        root_paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "a" in root_paths

        a_paths = [
            n["path"]
            for n in gql(QUERY_A_CHILDREN, headers=ANALYST_HEADERS)["data"][
                "namespace"
            ]["children"]["list"]
        ]
        assert "a/b" in a_paths

        revoke_graph("analyst", "a/b/c")

        root_paths = [
            n["path"]
            for n in gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)["data"]["root"][
                "children"
            ]["list"]
        ]
        assert "a" not in root_paths

        a_paths = [
            n["path"]
            for n in gql(QUERY_A_CHILDREN, headers=ANALYST_HEADERS)["data"][
                "namespace"
            ]["children"]["list"]
        ]
        assert "a/b" not in a_paths


def test_no_namespace_grant_hidden_from_children():
    """No grants at all → namespace is hidden from root children listing."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        # analyst has no grants at all

        response = gql(QUERY_NS_CHILDREN, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        paths = [n["path"] for n in response["data"]["root"]["children"]["list"]]
        assert "team" not in paths


# --- deleteGraph / sendGraph policy delegation ---

DELETE_JIRA = """mutation { deleteGraph(path: "jira") }"""
DELETE_TEAM_JIRA = """mutation { deleteGraph(path: "team/jira") }"""


def test_analyst_can_delete_with_graph_and_namespace_write():
    """deleteGraph requires WRITE on both the graph and its parent namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "WRITE")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(DELETE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["deleteGraph"] is True


def test_analyst_cannot_delete_with_graph_write_only():
    """Graph WRITE alone is insufficient for deleteGraph — namespace WRITE is also required."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")

        response = gql(DELETE_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "jira" was not deleted as a side effect
        check = gql(QUERY_JIRA)
        assert check["data"]["graph"]["path"] == "jira"


def test_analyst_cannot_delete_with_read_grant():
    """'access':'ro' user with READ-only grant is denied by deleteGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(DELETE_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "jira" was not deleted as a side effect
        check = gql(QUERY_JIRA)
        assert check["data"]["graph"]["path"] == "jira"


def test_analyst_can_delete_with_namespace_write():
    """'access':'ro' user with namespace WRITE (cascades to graph WRITE) can delete a graph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(DELETE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["deleteGraph"] is True


def test_analyst_cannot_send_graph_without_namespace_write():
    """'access':'ro' user without namespace WRITE is denied by sendGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "READ")  # READ, not WRITE

        response = gql(
            'mutation { sendGraph(path: "team/new", graph: "dummydata", overwrite: false) }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_send_graph_passes_auth_with_namespace_write():
    """'access':'ro' user with namespace WRITE passes the auth gate in sendGraph.

    The request fails on graph decoding (invalid data), not on access control —
    proving the namespace WRITE check is honoured.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(
            'mutation { sendGraph(path: "team/new", graph: "not_valid_base64", overwrite: false) }',
            headers=ANALYST_HEADERS,
        )
        # Auth passed — error is about graph decoding, not access
        assert "errors" in response
        assert "Access denied" not in response["errors"][0]["message"]


def test_analyst_send_graph_valid_data_with_namespace_write():
    """'access':'ro' user with namespace WRITE can successfully send a valid graph via sendGraph.

    Admin creates a graph and downloads it; analyst with WRITE sends it to a new path.
    The graph appears at the new path and its data matches the original.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        # Add a node so the graph has content to verify after the roundtrip
        gql("""query {
                updateGraph(path: "jira") {
                    addNode(time: 1, name: "alice", properties: []) { success }
                }
            }""")

        # Admin downloads the graph as valid base64
        encoded = gql('query { receiveGraph(path: "jira") }')["data"]["receiveGraph"]

        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        # Analyst sends the encoded graph to a new path
        response = gql(
            f'mutation {{ sendGraph(path: "team/copy", graph: "{encoded}", overwrite: false) }}',
            headers=ANALYST_HEADERS,
        )
        assert "errors" not in response, response
        assert response["data"]["sendGraph"] == "team/copy"

        # Verify the copy exists and contains the expected node
        check = gql('query { graph(path: "team/copy") { nodes { list { name } } } }')
        names = [n["name"] for n in check["data"]["graph"]["nodes"]["list"]]
        assert "alice" in names


# --- moveGraph policy ---

MOVE_TEAM_JIRA = """mutation { moveGraph(path: "team/jira", newPath: "team/jira-moved", overwrite: false) }"""


def test_analyst_can_move_with_graph_write_and_namespace_write():
    """moveGraph requires WRITE on the source graph and its parent namespace, plus WRITE on the destination namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "WRITE")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(MOVE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["moveGraph"] is True


def test_analyst_cannot_move_with_graph_write_only():
    """Graph WRITE alone is insufficient for moveGraph — namespace WRITE on source is also required."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "WRITE")
        # no namespace grant → namespace WRITE check fails

        response = gql(MOVE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "team/jira" still exists and "team/jira-moved" was not created
        team_graphs = gql(QUERY_TEAM_GRAPHS)["data"]["namespace"]["graphs"]["list"]
        paths = [g["path"] for g in team_graphs]
        assert "team/jira" in paths
        assert "team/jira-moved" not in paths


def test_analyst_cannot_move_with_read_grant():
    """READ on source graph is insufficient for moveGraph — WRITE is required."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_graph("analyst", "team/jira", "READ")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(MOVE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "team/jira" still exists and "team/jira-moved" was not created
        team_graphs = gql(QUERY_TEAM_GRAPHS)["data"]["namespace"]["graphs"]["list"]
        paths = [g["path"] for g in team_graphs]
        assert "team/jira" in paths
        assert "team/jira-moved" not in paths


# --- newGraph namespace write enforcement ---


def test_analyst_can_create_namespaced_graph_with_namespace_write():
    """'access':'ro' user with namespace WRITE can create a graph inside that namespace."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(CREATE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["newGraph"] is True


def test_analyst_cannot_create_graph_with_namespace_read_only():
    """'access':'ro' user with namespace READ (not WRITE) is denied by newGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "READ")

        response = gql(CREATE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
        # Verify "team/jira" was not created as a side effect — "team" namespace should be absent
        children = gql(QUERY_NS_CHILDREN)["data"]["root"]["children"]["list"]
        assert "team" not in [c["path"] for c in children]


# --- permissions entry point admin gate ---


def test_analyst_cannot_access_permissions_query_entry_point():
    """'access':'ro' user is denied at the permissions query entry point, not just the individual ops.

    This verifies the entry-point-level admin check added to query { permissions { ... } }.
    Even with full namespace WRITE, a non-admin JWT cannot reach the permissions resolver.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")  # full write, still not admin

        response = gql(
            "query { permissions { listRoles } }",
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_cannot_access_permissions_mutation_entry_point():
    """'access':'ro' user is denied at the mutation { permissions { ... } } entry point.

    Even with full namespace WRITE, a non-admin JWT is blocked before reaching any op.
    """
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")  # full write, still not admin

        response = gql(
            'mutation { permissions { createRole(name: "hacker") { success } } }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


# --- createIndex policy ---


def test_analyst_can_create_index_with_graph_write():
    """A user with WRITE on a graph can call createIndex (not admin-only)."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")

        response = gql(
            'mutation { createIndex(path: "jira", inRam: true) }',
            headers=ANALYST_HEADERS,
        )
        # Auth passed — success or a feature-not-compiled error, not an access denial
        if "errors" in response:
            assert "Access denied" not in response["errors"][0]["message"]


def test_analyst_cannot_create_index_with_read_grant():
    """READ on a graph is insufficient for createIndex — WRITE is required."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(
            'mutation { createIndex(path: "jira", inRam: true) }',
            headers=ANALYST_HEADERS,
        )
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]
