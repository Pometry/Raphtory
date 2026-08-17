"""The partial index (`vectoriseMissing`) and the atomicity of a full rebuild.

`vectoriseGraph` re-embeds everything into a new generation and switches to it only once complete;
`vectoriseMissing` embeds only entities absent from the index and never touches existing rows.

Failures here are produced by stopping the embedding server, never by raising inside the embedding
callback — an exception there leaves the request without a response and the client waits forever.
"""

import tempfile
import threading
import time

from raphtory.graphql import GraphServer, RaphtoryClient
from raphtory.vectors import embedding_server

PARTIAL_PORT = 7341


class EmbeddingControl:
    """Call counter and optional delay for the mock embedding server below."""

    def __init__(self):
        self.calls = 0
        self.delay = 0.0
        self.lock = threading.Lock()

    def reset(self):
        self.calls, self.delay = 0, 0.0


CONTROL = EmbeddingControl()


@embedding_server
def controlled_embeddings(text: str):
    # the sample used to resolve the model must always answer promptly
    if text != "raphtory" and CONTROL.delay:
        time.sleep(CONTROL.delay)
    with CONTROL.lock:
        CONTROL.calls += 1
    return [float(text.count("a")), float(text.count("b"))]


def _index(path: str, template: str, field: str) -> str:
    return (
        '{ %s(path: "%s", model: { openAI: { model: "mock", apiBase: "http://localhost:%d" } }, '
        'nodes: { custom: "%s" }, edges: { enabled: false }) }'
        % (field, path, PARTIAL_PORT, template)
    )


def rebuild(path: str, template: str) -> str:
    return _index(path, template, "vectoriseGraph")


def missing(path: str, template: str) -> str:
    return _index(path, template, "vectoriseMissing")


def search(path: str, query: str) -> str:
    return (
        '{ vectorisedGraph(path: "%s") { nodesBySimilarity(query: "%s", limit: 10) '
        "{ getDocuments { content } } } }" % (path, query)
    )


def contents(client, path: str, query: str = "aab") -> list[str]:
    vg = client.query(search(path, query)).get("vectorisedGraph")
    if vg is None:
        return []
    return sorted(d["content"] for d in vg["nodesBySimilarity"]["getDocuments"])


def seed(client, path: str, names: list[str]):
    client.new_graph(path, "EVENT")
    remote = client.remote_graph(path)
    for i, name in enumerate(names, start=1):
        remote.add_node(i, name, {"doc": name})


def query_failed(client, query: str) -> bool:
    try:
        client.query(query)
        return False
    except Exception:
        return True


DOC = "{{ properties.doc }}"
SECOND_DOC = "second {{ properties.doc }}"


def test_partial_index_only_embeds_missing_entities():
    """MISSING embeds only what is not indexed yet, and refuses a changed template.

    Writes embed inline, so the way an entity ends up in the graph but not in the index is an
    embedding failure at write time (or a bulk load that skips embedding). That is what this
    reproduces, by writing while the embedding server is down.
    """
    CONTROL.reset()
    work_dir = tempfile.TemporaryDirectory()

    with GraphServer(work_dir.name).start() as server:
        client = server.get_client()

        with controlled_embeddings.start(PARTIAL_PORT):
            seed(client, "pg", ["aab", "abb"])
            client.query(rebuild("pg", DOC))
            assert contents(client, "pg") == ["aab", "abb"]

            # nothing missing: no embedding calls at all, just the id scan
            before = CONTROL.calls
            client.query(missing("pg", DOC))
            assert CONTROL.calls == before, (
                f"a partial index with nothing missing embedded {CONTROL.calls - before} documents"
            )
            assert contents(client, "pg") == ["aab", "abb"]

        # written while embedding is unavailable: the write lands, the index does not get it
        client.remote_graph("pg").add_node(3, "bba", {"doc": "bba"})

        with controlled_embeddings.start(PARTIAL_PORT):
            assert contents(client, "pg") == ["aab", "abb"], (
                "the node written without embeddings should be missing from the index"
            )

            before = CONTROL.calls
            client.query(missing("pg", DOC))
            assert CONTROL.calls - before == 1, (
                f"expected 1 embedding for the node that was missing, got {CONTROL.calls - before}"
            )
            assert contents(client, "pg") == ["aab", "abb", "bba"], (
                "the partial index should have repaired the missing entity"
            )

            # a changed template cannot be filled in incrementally
            assert query_failed(client, missing("pg", "changed " + DOC)), (
                "a partial index against a changed template must be refused"
            )
            assert contents(client, "pg") == ["aab", "abb", "bba"], (
                "the refused call must not have touched the index"
            )


def test_rebuild_keeps_serving_while_in_flight():
    """The previous index must keep answering while a rebuild is running."""
    CONTROL.reset()
    work_dir = tempfile.TemporaryDirectory()

    with controlled_embeddings.start(PARTIAL_PORT):
        with GraphServer(work_dir.name).start() as server:
            client = server.get_client()
            seed(client, "ag", ["aab", "abb"])
            client.query(rebuild("ag", DOC))
            assert contents(client, "ag") == ["aab", "abb"]

            # slow the embeddings so the rebuild is observably in flight
            CONTROL.delay = 0.5
            outcome = []

            def run_rebuild():
                c = RaphtoryClient(f"http://localhost:{server.port()}")
                try:
                    c.query(rebuild("ag", SECOND_DOC))
                    outcome.append("ok")
                except Exception as e:
                    outcome.append(f"raised {e}")

            thread = threading.Thread(target=run_rebuild)
            thread.start()
            try:
                time.sleep(0.3)
                assert not outcome, "the rebuild should still be running"
                # "aab" is already cached, so this needs no embedding call of its own
                assert contents(client, "ag") == ["aab", "abb"], (
                    "the previous documents must still be served while a rebuild is in flight"
                )
            finally:
                thread.join(timeout=60)
            assert outcome == ["ok"], outcome

            CONTROL.delay = 0.0
            assert contents(client, "ag", "second aab") == ["second aab", "second abb"]


def test_failed_rebuild_leaves_the_previous_index_and_recovers():
    """A rebuild that cannot complete leaves the previous index serving — live and after a
    restart — and a later rebuild that does complete must work."""
    CONTROL.reset()
    work_dir = tempfile.TemporaryDirectory()

    with GraphServer(work_dir.name).start() as server:
        client = server.get_client()
        with controlled_embeddings.start(PARTIAL_PORT):
            seed(client, "rg", ["aab", "abb"])
            client.query(rebuild("rg", DOC))
            assert contents(client, "rg") == ["aab", "abb"]

        # embedding server is down: the rebuild cannot finish
        assert query_failed(client, rebuild("rg", SECOND_DOC)), (
            "a rebuild that cannot embed must report the failure"
        )
        assert contents(client, "rg") == ["aab", "abb"], (
            "a failed rebuild must leave the previous documents being served"
        )

        with controlled_embeddings.start(PARTIAL_PORT):
            # the meta was never switched, so the graph is not wedged: a rebuild still works
            client.query(rebuild("rg", SECOND_DOC))
            assert contents(client, "rg", "second aab") == ["second aab", "second abb"]

    # and the switch survives a restart
    with controlled_embeddings.start(PARTIAL_PORT):
        with GraphServer(work_dir.name).start() as server:
            client = server.get_client()
            assert contents(client, "rg", "second aab") == ["second aab", "second abb"]


def test_failed_rebuild_leaves_the_previous_index_on_disk():
    """The same, checked across a restart rather than in-process: an abandoned rebuild must not
    change what a fresh server loads."""
    CONTROL.reset()
    work_dir = tempfile.TemporaryDirectory()

    with GraphServer(work_dir.name).start() as server:
        client = server.get_client()
        with controlled_embeddings.start(PARTIAL_PORT):
            seed(client, "dg", ["aab", "abb"])
            client.query(rebuild("dg", DOC))
            assert contents(client, "dg") == ["aab", "abb"]
        assert query_failed(client, rebuild("dg", SECOND_DOC))

    with controlled_embeddings.start(PARTIAL_PORT):
        with GraphServer(work_dir.name).start() as server:
            client = server.get_client()
            assert contents(client, "dg") == ["aab", "abb"], (
                "an abandoned rebuild must leave the previous generation on disk"
            )
