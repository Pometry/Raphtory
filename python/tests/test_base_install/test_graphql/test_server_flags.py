import json
import tempfile
import urllib.error
import urllib.request

import pytest
from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient

SERVER_URL = "http://localhost:1736"


def batch_query(body):
    """POST a raw JSON body (needed for batch requests — the client only sends single queries)."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        SERVER_URL + "/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw.decode("utf-8", errors="replace")


def make_graph(client, path="g"):
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "lucas", "hamza")
    g.add_edge(3, "ben", "lucas")
    client.send_graph(path, g, overwrite=True)


def test_introspection_enabled_by_default():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir).start():
        client = RaphtoryClient(SERVER_URL)
        result = client.query("{ __schema { queryType { name } } }")
        assert result["__schema"]["queryType"]["name"]


def test_disable_introspection():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, disable_introspection=True).start():
        client = RaphtoryClient(SERVER_URL)
        client.query("{ version }")

        with pytest.raises(Exception) as excinfo:
            client.query("{ __schema { queryType { name } } }")
        msg = str(excinfo.value)
        assert "Unknown field" in msg and "__schema" in msg


def test_max_query_depth():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_query_depth=3).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)

        client.query('{ graph(path: "g") { created } }')

        with pytest.raises(Exception) as excinfo:
            client.query(
                '{ graph(path: "g") { nodes { page(limit: 5) { edges { page(limit: 5) { src { name } } } } } } }'
            )
        assert "Query is nested too deep." in str(excinfo.value)


def test_max_query_complexity():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_query_complexity=3).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)

        client.query("{ version }")

        with pytest.raises(Exception) as excinfo:
            client.query(
                '{ graph(path: "g") { nodes { page(limit: 5) { name id earliestTime latestTime } } } }'
            )
        assert "Query is too complex." in str(excinfo.value)


# (field path, query) pairs covering every list-returning resolver:
# GqlCollection, GqlNodes, GqlEdges, GqlPathFromNode, GqlHistory, GqlHistoryTimestamp,
# GqlHistoryDateTime, GqlHistoryEventId, GqlIntervals, and the six WindowSet types.
LIST_QUERIES = [
    ("collection (namespaces)", "{ namespaces { list { path } } }"),
    ("GqlNodes", '{ graph(path: "g") { nodes { list { name } } } }'),
    ("GqlEdges", '{ graph(path: "g") { edges { list { src { name } } } } }'),
    (
        "GqlPathFromNode",
        '{ graph(path: "g") { node(name: "ben") { neighbours { list { name } } } } }',
    ),
    (
        "GqlHistory",
        '{ graph(path: "g") { node(name: "ben") { history { list { timestamp } } } } }',
    ),
    (
        "GqlHistoryTimestamp",
        '{ graph(path: "g") { node(name: "ben") { history { timestamps { list } } } } }',
    ),
    (
        "GqlHistoryDateTime",
        '{ graph(path: "g") { node(name: "ben") { history { datetimes { list } } } } }',
    ),
    (
        "GqlHistoryEventId",
        '{ graph(path: "g") { node(name: "ben") { history { eventId { list } } } } }',
    ),
    (
        "GqlIntervals",
        '{ graph(path: "g") { node(name: "ben") { history { intervals { list } } } } }',
    ),
    (
        "GqlHistory.listRev",
        '{ graph(path: "g") { node(name: "ben") { history { listRev { timestamp } } } } }',
    ),
    (
        "GqlHistoryTimestamp.listRev",
        '{ graph(path: "g") { node(name: "ben") { history { timestamps { listRev } } } } }',
    ),
    (
        "GqlHistoryDateTime.listRev",
        '{ graph(path: "g") { node(name: "ben") { history { datetimes { listRev } } } } }',
    ),
    (
        "GqlHistoryEventId.listRev",
        '{ graph(path: "g") { node(name: "ben") { history { eventId { listRev } } } } }',
    ),
    (
        "GqlIntervals.listRev",
        '{ graph(path: "g") { node(name: "ben") { history { intervals { listRev } } } } }',
    ),
    (
        "GqlGraphWindowSet",
        '{ graph(path: "g") { rolling(window: {epoch: 1}) { list { earliestTime { timestamp } } } } }',
    ),
    (
        "GqlNodeWindowSet",
        '{ graph(path: "g") { node(name: "ben") { rolling(window: {epoch: 1}) { list { name } } } } }',
    ),
    (
        "GqlNodesWindowSet",
        '{ graph(path: "g") { nodes { rolling(window: {epoch: 1}) { list { count } } } } }',
    ),
    (
        "GqlPathFromNodeWindowSet",
        '{ graph(path: "g") { node(name: "ben") { neighbours { rolling(window: {epoch: 1}) { list { count } } } } } }',
    ),
    (
        "GqlEdgeWindowSet",
        '{ graph(path: "g") { edge(src: "ben", dst: "hamza") { rolling(window: {epoch: 1}) { list { src { name } } } } } }',
    ),
    (
        "GqlEdgesWindowSet",
        '{ graph(path: "g") { edges { rolling(window: {epoch: 1}) { list { count } } } } }',
    ),
]

# Same resolvers reached via `page(limit: 50)` — chosen so we can compare against a small
# `max_page_size` limit and trigger the exceeded-size error.
PAGE_QUERIES = [
    ("collection (namespaces)", "{ namespaces { page(limit: 50) { path } } }"),
    ("GqlNodes", '{ graph(path: "g") { nodes { page(limit: 50) { name } } } }'),
    (
        "GqlEdges",
        '{ graph(path: "g") { edges { page(limit: 50) { src { name } } } } }',
    ),
    (
        "GqlPathFromNode",
        '{ graph(path: "g") { node(name: "ben") { neighbours { page(limit: 50) { name } } } } }',
    ),
    (
        "GqlHistory",
        '{ graph(path: "g") { node(name: "ben") { history { page(limit: 50) { timestamp } } } } }',
    ),
    (
        "GqlHistoryTimestamp",
        '{ graph(path: "g") { node(name: "ben") { history { timestamps { page(limit: 50) } } } } }',
    ),
    (
        "GqlHistoryDateTime",
        '{ graph(path: "g") { node(name: "ben") { history { datetimes { page(limit: 50) } } } } }',
    ),
    (
        "GqlHistoryEventId",
        '{ graph(path: "g") { node(name: "ben") { history { eventId { page(limit: 50) } } } } }',
    ),
    (
        "GqlIntervals",
        '{ graph(path: "g") { node(name: "ben") { history { intervals { page(limit: 50) } } } } }',
    ),
    (
        "GqlHistory.pageRev",
        '{ graph(path: "g") { node(name: "ben") { history { pageRev(limit: 50) { timestamp } } } } }',
    ),
    (
        "GqlHistoryTimestamp.pageRev",
        '{ graph(path: "g") { node(name: "ben") { history { timestamps { pageRev(limit: 50) } } } } }',
    ),
    (
        "GqlHistoryDateTime.pageRev",
        '{ graph(path: "g") { node(name: "ben") { history { datetimes { pageRev(limit: 50) } } } } }',
    ),
    (
        "GqlHistoryEventId.pageRev",
        '{ graph(path: "g") { node(name: "ben") { history { eventId { pageRev(limit: 50) } } } } }',
    ),
    (
        "GqlIntervals.pageRev",
        '{ graph(path: "g") { node(name: "ben") { history { intervals { pageRev(limit: 50) } } } } }',
    ),
    (
        "GqlGraphWindowSet",
        '{ graph(path: "g") { rolling(window: {epoch: 1}) { page(limit: 50) { earliestTime { timestamp } } } } }',
    ),
    (
        "GqlNodeWindowSet",
        '{ graph(path: "g") { node(name: "ben") { rolling(window: {epoch: 1}) { page(limit: 50) { name } } } } }',
    ),
    (
        "GqlNodesWindowSet",
        '{ graph(path: "g") { nodes { rolling(window: {epoch: 1}) { page(limit: 50) { count } } } } }',
    ),
    (
        "GqlPathFromNodeWindowSet",
        '{ graph(path: "g") { node(name: "ben") { neighbours { rolling(window: {epoch: 1}) { page(limit: 50) { count } } } } } }',
    ),
    (
        "GqlEdgeWindowSet",
        '{ graph(path: "g") { edge(src: "ben", dst: "hamza") { rolling(window: {epoch: 1}) { page(limit: 50) { src { name } } } } } }',
    ),
    (
        "GqlEdgesWindowSet",
        '{ graph(path: "g") { edges { rolling(window: {epoch: 1}) { page(limit: 50) { count } } } } }',
    ),
]


def test_disable_lists_all_resolvers():
    """Every `list` endpoint across every paginated type rejects with the same error."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, disable_lists=True).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)

        for name, query in LIST_QUERIES:
            with pytest.raises(Exception) as excinfo:
                client.query(query)
            assert (
                "Bulk list endpoints are disabled on this server. Use `page` instead."
                in str(excinfo.value)
            ), f"{name} did not reject with the expected error: {excinfo.value}"


def test_disable_lists_page_still_works():
    """Even with `disable_lists=True`, `page` queries still succeed."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, disable_lists=True).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)
        result = client.query(
            '{ graph(path: "g") { nodes { page(limit: 10) { name } } } }'
        )
        assert len(result["graph"]["nodes"]["page"]) == 3


def test_max_page_size_all_resolvers():
    """Every `page` endpoint across every paginated type enforces max_page_size."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_page_size=2).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)

        for name, query in PAGE_QUERIES:
            with pytest.raises(Exception) as excinfo:
                client.query(query)
            assert "page limit 50 exceeds the maximum allowed page size 2" in str(
                excinfo.value
            ), f"{name} did not reject with the expected error: {excinfo.value}"


def test_max_page_size_under_cap_works():
    """Pages at or below max_page_size still succeed."""
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_page_size=2).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)
        result = client.query(
            '{ graph(path: "g") { nodes { page(limit: 2) { name } } } }'
        )
        assert len(result["graph"]["nodes"]["page"]) == 2


def test_disable_batching():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, disable_batching=True).start():
        RaphtoryClient(SERVER_URL).query("{ version }")

        status, body = batch_query([{"query": "{ version }"}, {"query": "{ version }"}])
        assert status == 400
        assert "Query batching is disabled on this server" in str(body)


def test_max_batch_size():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_batch_size=2).start():
        status, body = batch_query([{"query": "{ version }"}] * 2)
        assert status == 200
        assert isinstance(body, list) and len(body) == 2

        status, body = batch_query([{"query": "{ version }"}] * 3)
        assert status == 400
        assert "Batch size 3 exceeds the maximum allowed 2" in str(body)


def test_max_recursive_depth():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_recursive_depth=2).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)

        # depth 2: { graph { created } } — root selection set is depth 0, graph{...} pushes to 1
        client.query('{ graph(path: "g") { created } }')

        with pytest.raises(Exception) as excinfo:
            client.query('{ graph(path: "g") { nodes { page(limit: 1) { name } } } }')
        assert "recursion depth of the query cannot be greater than `2`" in str(
            excinfo.value
        )


def test_max_directives_per_field():
    work_dir = tempfile.mkdtemp()
    with GraphServer(work_dir, max_directives_per_field=1).start():
        client = RaphtoryClient(SERVER_URL)

        # 1 directive — allowed
        client.query("{ version @skip(if: false) }")

        # 2 directives on one field — rejected
        with pytest.raises(Exception) as excinfo:
            client.query("{ version @skip(if: false) @include(if: true) }")
        assert (
            "number of directives on the field `version` cannot be greater than `1`"
            in str(excinfo.value)
        )


# heavy_query_limit and exclusive_writes are concurrency knobs. Their effects only show
# up under parallel load (semaphore-parked queries, write/read serialization), and
# timing-based tests are flaky in CI. This smoke test at least verifies the flags are
# accepted and normal queries still pass through.
def test_concurrency_flags_smoke():
    work_dir = tempfile.mkdtemp()
    with GraphServer(
        work_dir,
        heavy_query_limit=4,
        exclusive_writes=True,
    ).start():
        client = RaphtoryClient(SERVER_URL)
        make_graph(client)
        # Read path: works under exclusive_writes's read lock.
        assert client.query('{ graph(path: "g") { nodes { count } } }')
        # Heavy traversal: goes through the semaphore.
        assert client.query(
            '{ graph(path: "g") { nodes { page(limit: 10) { neighbours { page(limit: 10) { name } } } } } }'
        )
