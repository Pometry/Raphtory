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
QUERY_TEAM_JIRA = """query { graph(path: "team/jira") { path } }"""
QUERY_TEAM_GRAPHS = """query { namespace(path: "team") { graphs { list { path } } } }"""


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

        # graph() resolver is denied (only INTROSPECT, not READ)
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


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
        assert response["data"] is None or response["data"].get("updateGraph") is None
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


def test_introspect_only_cannot_access_graph_data():
    """Namespace INTROSPECT is denied by graph() — READ is required to access graph data."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "INTROSPECT")  # no READ

        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


def test_no_grant_hidden_from_namespace_and_graph():
    """A role with no permissions on a graph sees it neither in listings nor via graph()."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # analyst has no grant on jira at all

        # graph() denied
        response = gql(QUERY_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]

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
        assert "INTROSPECT cannot be granted on a graph" in response["errors"][0]["message"]


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

        # graph() is still denied — INTROSPECT does not grant data access
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


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
            '{ node: { property: { name: "region", where: { eq: { str: "us-west" } } } } }',
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
            '{ edge: { property: { name: "weight", where: { ge: { i64: 5 } } } } }',
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
    """RaphtoryClient with analyst role is denied access to a graph it has no grant for."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        # No grant on jira

        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        with pytest.raises(Exception, match="Access denied"):
            client.query(QUERY_JIRA)


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

        # No grant — denied
        client = RaphtoryClient(url=RAPHTORY, token=ANALYST_JWT)
        with pytest.raises(Exception, match="Access denied"):
            client.receive_graph("team/jira")

        # Namespace INTROSPECT only — also denied for receive_graph
        grant_namespace("analyst", "team", "INTROSPECT")
        with pytest.raises(Exception, match="Access denied"):
            client.receive_graph("team/jira")

        # READ — allowed
        grant_namespace("analyst", "team", "READ")
        g = client.receive_graph("team/jira")
        assert g is not None


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
            "{ graph: { window: { start: 5, end: 15 } } }",
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

        # Direct graph access is denied
        response = gql(QUERY_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert response["data"] is None
        assert "Access denied" in response["errors"][0]["message"]


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
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "WRITE")
        grant_namespace("analyst", "*", "WRITE")

        response = gql(DELETE_JIRA, headers=ANALYST_HEADERS)
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


def test_analyst_cannot_delete_with_read_grant():
    """'a':'ro' user with READ-only grant is denied by deleteGraph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_JIRA)
        create_role("analyst")
        grant_graph("analyst", "jira", "READ")

        response = gql(DELETE_JIRA, headers=ANALYST_HEADERS)
        assert "errors" in response
        assert "Access denied" in response["errors"][0]["message"]


def test_analyst_can_delete_with_namespace_write():
    """'a':'ro' user with namespace WRITE (cascades to graph WRITE) can delete a graph."""
    work_dir = tempfile.mkdtemp()
    with make_server(work_dir).start():
        gql(CREATE_TEAM_JIRA)
        create_role("analyst")
        grant_namespace("analyst", "team", "WRITE")

        response = gql(DELETE_TEAM_JIRA, headers=ANALYST_HEADERS)
        assert "errors" not in response, response
        assert response["data"]["deleteGraph"] is True


def test_analyst_cannot_send_graph_without_namespace_write():
    """'a':'ro' user without namespace WRITE is denied by sendGraph."""
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
    """'a':'ro' user with namespace WRITE passes the auth gate in sendGraph.

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
