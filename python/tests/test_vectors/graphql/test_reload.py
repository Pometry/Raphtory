"""Reloading a vectorised graph from disk, including when the cache evicts it."""

import tempfile
import threading

from raphtory.graphql import GraphServer, RaphtoryClient

from helpers import EMBEDDING_PORT, assert_correct_documents, embeddings, setup_graph


def test_revectorise_reloaded_graph():
    """Re-vectorising a graph that got reloaded from disk rebuilds its index instead
    of leaving the graph with no vectors, which is what clients that vectorise on
    every startup rely on."""
    vectorise = """
        {
        vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
        }
        """
    work_dir = tempfile.TemporaryDirectory()
    with embeddings.start(7340):
        with GraphServer(work_dir.name).start() as server:
            client = server.get_client()
            client.new_graph("abb", "EVENT")
            setup_graph(client.remote_graph("abb"))
            client.query(vectorise)
            assert_correct_documents(client)

        # restarting the server reloads the graph, and its vectors, from disk
        with GraphServer(work_dir.name).start() as server:
            client = server.get_client()
            assert_correct_documents(client)
            client.query(vectorise)
            assert_correct_documents(client)


def test_evicted_vectorised_graphs_stay_queryable_under_load():
    """Three vectorised graphs rotating through a cache that holds one, while they are being
    queried and updated, so most reads are served by a graph reloaded from disk.

    Three and not two: a graph is pinned in the cache while it has unflushed writes, and
    eviction is only attempted when something else is inserted, so with two graphs both end
    up resident and nothing is ever reloaded. With three, the rotation keeps forcing inserts
    (~23 evictions and ~24 reloads for the run below), which is what exercises the reload of
    a persisted vector index.
    """
    graphs = ["ga", "gb", "gc"]
    seeded, workers, rounds = 3, 2, 6

    def vectorise_query(path: str) -> str:
        return (
            '{ vectoriseGraph(path: "%s", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, '
            'nodes: { custom: "{{ properties.doc }}" }, edges: { enabled: false }) }' % path
        )

    def read_query(path: str) -> str:
        return '{ graph(path: "%s") { nodes { list { name } } } }' % path

    def search_query(path: str) -> str:
        return (
            '{ vectorisedGraph(path: "%s") { nodesBySimilarity(query: "aaa", limit: 3) '
            "{ getDocuments { content } } } }" % path
        )

    work_dir = tempfile.TemporaryDirectory()
    failures: list[str] = []
    lock = threading.Lock()

    def record(msg: str):
        with lock:
            failures.append(msg)

    with embeddings.start(7340):
        with GraphServer(work_dir.name, config={"cache": {"capacity": 1}}).start() as server:
            url = f"http://localhost:{server.port()}"
            client = server.get_client()
            for path in graphs:
                client.new_graph(path, "EVENT")
                seed = client.remote_graph(path)
                for t in range(1, seeded + 1):
                    seed.add_node(t, f"seed{t}", {"doc": f"aaa {path} seed"})
                client.query(vectorise_query(path))

            def worker(wid: int):
                c = RaphtoryClient(url)
                remotes = {path: c.remote_graph(path) for path in graphs}
                highest = {path: 0 for path in graphs}
                for i in range(rounds):
                    for path in graphs:
                        try:
                            remotes[path].add_node(
                                100 + i, f"w{wid}n{i}", {"doc": f"aaa {path} w{wid} {i}"}
                            )
                            nodes = c.query(read_query(path))["graph"]["nodes"]["list"]
                            docs = c.query(search_query(path))["vectorisedGraph"][
                                "nodesBySimilarity"
                            ]["getDocuments"]
                        except Exception as e:
                            record(f"{path} w{wid} round {i}: {e}")
                            continue
                        if not nodes:
                            record(f"{path} w{wid}: empty node list after reload")
                        elif len(nodes) < highest[path]:
                            record(
                                f"{path} w{wid}: {len(nodes)} nodes after already seeing "
                                f"{highest[path]}"
                            )
                        highest[path] = max(highest[path], len(nodes))
                        if not docs:
                            record(f"{path} w{wid}: reloaded index returned no documents")

            threads = [threading.Thread(target=worker, args=(w,)) for w in range(workers)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert failures == [], f"{len(failures)} failures, first few: {failures[:5]}"

            for path in graphs:
                nodes = client.query(read_query(path))["graph"]["nodes"]["list"]
                assert len(nodes) == seeded + workers * rounds, (
                    f"{path} lost writes: expected {seeded + workers * rounds}, got {len(nodes)}"
                )
                docs = client.query(search_query(path))["vectorisedGraph"][
                    "nodesBySimilarity"
                ]["getDocuments"]
                assert docs, f"{path} has an empty vector index at the end"


# --- partial index and rebuild atomicity ---
#
# `vectoriseGraph(mode:)` is always stated by the caller. REBUILD re-embeds everything into a new
# generation and switches to it only when complete; MISSING embeds only entities absent from the
# index and never touches existing rows.

