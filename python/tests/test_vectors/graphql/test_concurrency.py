"""Reads and writes racing each other against a vectorised graph."""

import tempfile
import threading

from raphtory.graphql import GraphServer, RaphtoryClient

from helpers import embeddings


def test_concurrent_updates_while_querying():
    """Nodes being added to a vectorised graph (so every write also embeds) while other
    clients read the graph and search it.

    Each write goes through `update_node_embeddings`, and a re-vectorise swaps the whole
    entry the readers are reading, so this covers the shapes that race in a live server:
      * no read returns an empty or null node list,
      * a client never sees fewer nodes than one of its own earlier reads (nothing is
        deleted, so counts only grow),
      * every write is present at the end, and the vector index still answers.
    """
    read_query = """{
        graph(path: "abb") {
            nodes { list { name properties { doc: get(key: "doc") { value } } } }
        }
    }"""
    search_query = """{
        vectorisedGraph(path: "abb") {
            nodesBySimilarity(query: "aaa", limit: 5) { getDocuments { content } }
        }
    }"""
    vectorise_query = """{
        vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ properties.doc }}" }, edges: { enabled: false })
    }"""

    writers, reads_per_writer, readers, vectorise_rounds = 2, 10, 2, 2
    seeded = 3
    expected_nodes = seeded + writers * reads_per_writer

    work_dir = tempfile.TemporaryDirectory()
    failures: list[str] = []
    lock = threading.Lock()
    writers_done = threading.Event()

    def record(msg: str):
        with lock:
            failures.append(msg)

    with embeddings.start(7340):
        with GraphServer(work_dir.name).start() as server:
            url = f"http://localhost:{server.port()}"
            client = server.get_client()
            client.new_graph("abb", "EVENT")
            seed = client.remote_graph("abb")
            for t in range(1, seeded + 1):
                seed.add_node(t, f"seed{t}", {"doc": "aaa seed"})
            client.query(vectorise_query)

            def writer(wid: int):
                graph = RaphtoryClient(url).remote_graph("abb")
                for i in range(reads_per_writer):
                    try:
                        graph.add_node(i + 1, f"w{wid}n{i}", {"doc": f"aaa w{wid} n{i}"})
                    except Exception as e:
                        record(f"write w{wid}n{i} failed: {e}")

            def vectoriser():
                c = RaphtoryClient(url)
                for _ in range(vectorise_rounds):
                    try:
                        c.query(vectorise_query)
                    except Exception as e:
                        record(f"re-vectorise failed: {e}")

            def reader(rid: int):
                c = RaphtoryClient(url)
                highest = 0
                # keep reading until the writers stop, then a few more for the final state
                extra = 0
                while not writers_done.is_set() or extra < 3:
                    if writers_done.is_set():
                        extra += 1
                    try:
                        nodes = c.query(read_query)["graph"]["nodes"]["list"]
                    except Exception as e:
                        record(f"reader {rid} query failed: {e}")
                        continue
                    if not nodes:
                        record(f"reader {rid} got an empty node list: {nodes!r}")
                    elif len(nodes) < highest:
                        record(
                            f"reader {rid} saw {len(nodes)} nodes after already seeing {highest}"
                        )
                    highest = max(highest, len(nodes))
                    try:
                        c.query(search_query)
                    except Exception as e:
                        record(f"reader {rid} search failed: {e}")

            write_threads = [
                threading.Thread(target=writer, args=(w,)) for w in range(writers)
            ] + [threading.Thread(target=vectoriser)]
            read_threads = [threading.Thread(target=reader, args=(r,)) for r in range(readers)]
            for t in write_threads + read_threads:
                t.start()
            for t in write_threads:
                t.join()
            writers_done.set()
            for t in read_threads:
                t.join()

            assert failures == [], f"{len(failures)} failures, first few: {failures[:5]}"

            final = client.query(read_query)["graph"]["nodes"]["list"]
            assert len(final) == expected_nodes, (
                f"expected {expected_nodes} nodes after the concurrent writes, got {len(final)}"
            )
            docs = client.query(search_query)["vectorisedGraph"]["nodesBySimilarity"][
                "getDocuments"
            ]
            assert docs, "vector index returned nothing after concurrent updates"


