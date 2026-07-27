"""End-to-end tests for the typed RBAC permission-management methods on
`raphtory.graphql.RaphtoryClient`.

These wrap the server's `permissions { ... }` GraphQL mutations, which only
exist when the server is started with a `permissions_store_path` and require an
admin (`{"access": "rw"}`) token. A role-scoped reader carries
`{"access": "ro", "role": ...}` and sees only what its role has been granted.
"""

import os
import tempfile

import jwt
import pytest

from raphtory import filter
from raphtory.graphql import GraphServer, RaphtoryClient, RemotePermissionError

# EdDSA key pair (same fixture as test_auth.py).
PUB_KEY = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----"""

ADMIN_JWT = jwt.encode({"access": "rw"}, PRIVATE_KEY, algorithm="EdDSA")


def _reader_jwt(role: str) -> str:
    """Mint a read-only token scoped to a role."""
    return jwt.encode({"access": "ro", "role": role}, PRIVATE_KEY, algorithm="EdDSA")


def _url(port: int) -> str:
    return f"http://localhost:{port}"


def _server():
    """Start a server with an (initially empty) permissions store and auth enabled.

    The store path points at a not-yet-existing file inside a temp dir, so the
    store starts empty and is populated entirely through the typed client
    methods under test.
    """
    work_dir = tempfile.mkdtemp()
    store_path = os.path.join(tempfile.mkdtemp(), "permissions.json")
    return GraphServer(
        work_dir,
        permissions_store_path=store_path,
        config={"auth": {"public_key": PUB_KEY}},
    ).start()


def test_grant_and_revoke_graph_read():
    """A role-scoped reader can read a graph only within a grant/revoke window."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("secret", "EVENT")
        rg = admin.remote_graph("secret")
        rg.add_node(1, "ben")
        rg.add_node(2, "hamza")
        rg.add_edge(3, "ben", "hamza")

        assert admin.create_role("analyst") is True

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("analyst"))

        # Before any grant the reader cannot see the graph.
        with pytest.raises(Exception) as before:
            reader.remote_graph("secret").nodes.count()
        assert "secret" in str(before.value) or "access" in str(before.value).lower()

        assert admin.grant_graph("analyst", "secret", "read") is True

        # After the grant the reader sees the whole graph.
        granted = reader.remote_graph("secret")
        assert granted.nodes.count() == 2
        assert sorted(granted.nodes.id) == ["ben", "hamza"]

        assert admin.revoke_graph("analyst", "secret") is True

        # After the revoke the reader is locked out again.
        with pytest.raises(Exception) as after:
            reader.remote_graph("secret").nodes.count()
        assert "secret" in str(after.value) or "access" in str(after.value).lower()


def test_grant_graph_filtered_read_only_restricts_visible_nodes():
    """A filtered read-only grant limits the reader to the matching node subset."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("filtered", "EVENT")
        rg = admin.remote_graph("filtered")
        rg.add_node(1, "alice", {"dept": "eng"})
        rg.add_node(1, "bob", {"dept": "sales"})
        rg.add_node(1, "carol", {"dept": "eng"})

        # Admin (rw) always bypasses the filter and sees every node.
        assert sorted(admin.remote_graph("filtered").nodes.id) == [
            "alice",
            "bob",
            "carol",
        ]

        assert admin.create_role("viewer") is True

        eng_only = filter.Node.property("dept") == "eng"
        assert (
            admin.grant_graph_filtered_read_only("viewer", "filtered", eng_only)
            is True
        )

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("viewer"))
        visible = reader.remote_graph("filtered")
        assert visible.nodes.count() == 2
        assert sorted(visible.nodes.id) == ["alice", "carol"]


def test_grant_graph_filtered_read_only_hidden_properties():
    """Hidden property keys are stripped from the reader's view of nodes."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("masked", "EVENT")
        rg = admin.remote_graph("masked")
        rg.add_node(1, "alice", {"dept": "eng", "salary": "100"})
        rg.add_node(1, "carol", {"dept": "eng", "salary": "120"})

        assert admin.create_role("masked_viewer") is True

        eng_only = filter.Node.property("dept") == "eng"
        assert (
            admin.grant_graph_filtered_read_only(
                "masked_viewer",
                "masked",
                eng_only,
                hidden_properties={"node": ["salary"]},
            )
            is True
        )

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("masked_viewer"))
        visible = reader.remote_graph("masked")
        # Row-level filter still applies alongside the hidden key.
        assert sorted(visible.nodes.id) == ["alice", "carol"]


def test_grant_graph_invalid_permission_raises_value_error():
    """An unrecognized permission string is rejected before any RPC is made."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        with pytest.raises(ValueError, match="invalid permission"):
            admin.grant_graph("analyst", "secret", "superuser")

        with pytest.raises(ValueError, match="invalid permission"):
            admin.grant_namespace("analyst", "team", "owner")


def test_grant_namespace_recursive_and_revoke():
    """A recursive namespace grant reaches existing graphs under that namespace."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("team/report", "EVENT")
        admin.remote_graph("team/report").add_node(1, "n1")

        assert admin.create_role("team_reader") is True
        # A recursive grant enumerates the existing descendants of "team" and
        # grants each one, so the graph "team/report" becomes readable.
        assert (
            admin.grant_namespace("team_reader", "team", "read", recursive=True)
            is True
        )

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("team_reader"))
        assert reader.remote_graph("team/report").nodes.count() == 1

        assert (
            admin.revoke_namespace("team_reader", "team", recursive=True) is True
        )
        with pytest.raises(Exception) as revoked:
            reader.remote_graph("team/report").nodes.count()
        assert (
            "team/report" in str(revoked.value)
            or "access" in str(revoked.value).lower()
        )


def test_my_permissions_reflects_own_grants():
    """A reader sees exactly its own grants, with `filtered` set per grant."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("g1", "EVENT")
        admin.new_graph("g2", "EVENT")

        assert admin.create_role("R") is True
        assert admin.grant_graph("R", "g1", "read") is True

        eng_only = filter.Node.property("dept") == "eng"
        assert admin.grant_graph_filtered_read_only("R", "g2", eng_only) is True

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("R"))
        perms = reader.my_permissions()

        assert perms["role"] == "R"
        assert perms["namespaces"] == []

        graphs = {g["path"]: g for g in perms["graphs"]}
        assert set(graphs) == {"g1", "g2"}
        assert graphs["g1"]["permission"] == "READ"
        assert graphs["g1"]["filtered"] is False
        assert graphs["g2"]["permission"] == "READ"
        assert graphs["g2"]["filtered"] is True


def test_my_permissions_no_role_claim_is_empty():
    """A token without a role claim gets a null role and no grants."""
    with _server() as server:
        port = server.port()
        no_role = jwt.encode({"access": "ro"}, PRIVATE_KEY, algorithm="EdDSA")
        client = RaphtoryClient(url=_url(port), token=no_role)

        perms = client.my_permissions()
        assert perms["role"] is None
        assert perms["graphs"] == []
        assert perms["namespaces"] == []


def test_with_token_acts_as_the_named_reader():
    """`with_token` returns a client that reads only what the token's role may.

    An admin client re-scoped to a reader token behaves as that reader: it reads
    the graph the reader is granted and is locked out of one it is not. The
    original admin client is left untouched.
    """
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("shared", "EVENT")
        shared = admin.remote_graph("shared")
        shared.add_node(1, "a")
        shared.add_node(2, "b")

        admin.new_graph("private", "EVENT")
        admin.remote_graph("private").add_node(1, "secret")

        assert admin.create_role("limited") is True
        assert admin.grant_graph("limited", "shared", "read") is True

        # Same client object, reader identity — no reconstruction, no reconnect.
        as_reader = admin.with_token(_reader_jwt("limited"))

        # Reads exactly what the reader's role may read.
        assert as_reader.remote_graph("shared").nodes.count() == 2
        assert sorted(as_reader.remote_graph("shared").nodes.id) == ["a", "b"]

        # And is denied a graph the role has no grant for.
        with pytest.raises(Exception):
            as_reader.remote_graph("private").nodes.count()

        # The admin client is unaffected and still sees the private graph.
        assert admin.remote_graph("private").nodes.count() == 1


def test_write_denied_on_readable_graph_raises_permission_error():
    """A reader with READ but not WRITE gets the dedicated permission error.

    The graph is readable to the caller, so the server does not hide it; a write
    attempt is a genuine authorization failure and surfaces as
    `RemotePermissionError` (not a plain not-found).
    """
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("readable", "EVENT")
        admin.remote_graph("readable").add_node(1, "n1")

        assert admin.create_role("ro_role") is True
        assert admin.grant_graph("ro_role", "readable", "read") is True

        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("ro_role"))

        # The reader can read the graph it was granted.
        assert reader.remote_graph("readable").nodes.count() == 1

        # But a write is denied with the distinct permission error type.
        with pytest.raises(RemotePermissionError):
            reader.remote_graph("readable").add_node(2, "n2")


def test_forbidden_graph_indistinguishable_from_nonexistent():
    """Existence non-disclosure: a forbidden graph and a nonexistent one look the same.

    A reader with no grant must not be able to tell a graph it may not see from a
    graph that does not exist. Both requests must produce the same exception type
    and the same message (once the caller-supplied path is normalized out), and
    neither may be a permission error — both are plain not-found outcomes.
    """
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        # A real graph that exists but the reader is never granted.
        admin.new_graph("hidden", "EVENT")
        admin.remote_graph("hidden").add_node(1, "n1")

        # Role exists but carries no grants at all.
        assert admin.create_role("no_grants") is True
        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("no_grants"))

        with pytest.raises(Exception) as forbidden:
            reader.remote_graph("hidden").nodes.count()
        with pytest.raises(Exception) as missing:
            reader.remote_graph("this-graph-was-never-created").nodes.count()

        # Same exception type for both.
        assert type(forbidden.value) is type(missing.value)

        # Neither is the permission error — existence is hidden, so both are
        # reported as not-found.
        assert not isinstance(forbidden.value, RemotePermissionError)
        assert not isinstance(missing.value, RemotePermissionError)

        # Messages are identical once each caller-supplied path is normalized out,
        # so the wording leaks nothing about which graph actually exists.
        forbidden_msg = str(forbidden.value).replace("hidden", "<path>")
        missing_msg = str(missing.value).replace(
            "this-graph-was-never-created", "<path>"
        )
        assert forbidden_msg == missing_msg


def test_list_roles_and_get_role_are_admin_only():
    """`list_roles`/`get_role` work for admins and are denied for readers."""
    with _server() as server:
        port = server.port()
        admin = RaphtoryClient(url=_url(port), token=ADMIN_JWT)

        admin.new_graph("g1", "EVENT")
        assert admin.create_role("R") is True
        assert admin.grant_graph("R", "g1", "read") is True

        # Admin can enumerate and inspect roles.
        assert admin.list_roles() == ["R"]

        role = admin.get_role("R")
        assert role["name"] == "R"
        assert role["namespaces"] == []
        assert {g["path"]: g["permission"] for g in role["graphs"]} == {"g1": "READ"}

        # A missing role resolves to None rather than an error.
        assert admin.get_role("does_not_exist") is None

        # A role-scoped reader is denied both admin-only queries.
        reader = RaphtoryClient(url=_url(port), token=_reader_jwt("R"))
        with pytest.raises(Exception, match="write access required"):
            reader.list_roles()
        with pytest.raises(Exception, match="write access required"):
            reader.get_role("R")


def test_store_less_server_after_permissions_server_loads_schema():
    """A store-less server started after a permissions-backed one, in the same
    process, must still build its schema and serve normally.

    Registering the permissions plugin drains a process-global op registry, and
    a sticky "RBAC was configured" flag previously made every later server try
    to re-register the entrypoint. A store-less server (which never repopulates
    the registry) would then declare an empty permissions object and fail to
    load its schema — breaking any process that creates a permissions-backed
    server before a store-less one.
    """
    # A permissions-backed server first: populates then drains the global
    # permissions op registries during its schema build.
    with _server() as store_server:
        admin = RaphtoryClient(url=_url(store_server.port()), token=ADMIN_JWT)
        admin.create_role("R")

    # A store-less server in the same process must still load and serve.
    with GraphServer(
        tempfile.mkdtemp(), config={"auth": {"public_key": PUB_KEY}}
    ).start() as plain:
        client = RaphtoryClient(url=_url(plain.port()), token=ADMIN_JWT)
        client.new_graph("g", "EVENT")
        assert client.remote_graph("g").nodes.id == []
