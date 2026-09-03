"""Vectorising a graph for the first time, by each of the routes that can do it."""

import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer
from raphtory.vectors import OpenAIEmbeddings

from helpers import (
    EMBEDDING_PORT,
    assert_correct_documents,
    embeddings,
    setup_graph,
    vectorise_query,
)


def test_new_graph():
    print("test_new_graph")
    work_dir = tempfile.TemporaryDirectory()
    server = GraphServer(work_dir.name)
    with embeddings.start(7340):
        with server.start() as server:
            client = server.get_client()
            client.new_graph("abb", "EVENT")
            rg = client.remote_graph("abb")
            setup_graph(rg)
            client.query(
                """
                {
                    vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
                }
                """
            )
            assert_correct_documents(client)


def test_upload_graph():
    print("test_upload_graph")
    work_dir = tempfile.TemporaryDirectory()
    temp_dir = tempfile.TemporaryDirectory()
    server = GraphServer(work_dir.name)
    with embeddings.start(7340):
        with server.start() as server:
            client = server.get_client()
            g = Graph()
            setup_graph(g)
            g_path = temp_dir.name + "/abb"
            g.save_to_zip(g_path)
            client.upload_graph(path="abb", file_path=g_path, overwrite=True)
            client.query(
                """
                {
                vectoriseGraph(path: "abb", model: { openAI: { model: "whatever", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
                }
                """
            )
            assert_correct_documents(client)


def test_vectorised_graph_window_accepts_time_input_shapes():
    """`VectorisedGraphWindow.{start, end}` accepts every `TimeInput` shape —
    Int, RFC3339 string, and `{timestamp, eventId}` object.

    Verifies the schema accepts each form and that all three forms produce
    the *same* result for the same time bounds (i.e. they're parsed
    equivalently)."""
    work_dir = tempfile.TemporaryDirectory()
    server = GraphServer(work_dir.name)
    with embeddings.start(7340):
        with server.start() as server:
            client = server.get_client()
            client.new_graph("abb", "EVENT")
            rg = client.remote_graph("abb")
            setup_graph(rg)
            # `model` and `apiBase` point at the mock embedding server above,
            # so the model name is just a placeholder identifier.
            client.query(
                """
                {
                    vectoriseGraph(path: "abb", model: { openAI: { model: "mock-model", apiBase: "http://localhost:7340" } }, nodes: { custom: "{{ name }}" }, edges: { enabled: false })
                }
                """
            )

            def run(window_literal: str):
                q = (
                    """
                    {
                        vectorisedGraph(path: "abb") {
                            entitiesBySimilarity(query: "aab", limit: 5, window: %s) {
                                getDocuments { entity { ... on Node { name } } }
                            }
                        }
                    }
                    """
                    % window_literal
                )
                return client.query(q)

            # Same time bounds, three different input shapes — all should be
            # accepted by the schema and produce identical results.
            int_form = run("{ start: 0, end: 1000 }")
            str_form = run(
                '{ start: "1970-01-01T00:00:00.000Z", end: "1970-01-01T00:00:01.000Z" }'
            )
            obj_form = run(
                "{ start: {timestamp: 0, eventId: 0}, end: {timestamp: 1000, eventId: 0} }"
            )

            assert int_form == str_form == obj_form, (
                "All three TimeInput shapes should produce identical results "
                f"for equivalent time bounds.\nint:  {int_form}\nstr:  {str_form}\nobj:  {obj_form}"
            )


GRAPH_NAME = "abb"


def test_include_graph():
    work_dir = tempfile.TemporaryDirectory()
    g_path = work_dir.name + "/" + GRAPH_NAME
    g = Graph()
    setup_graph(g)
    g.save_to_file(g_path)
    server = GraphServer(work_dir.name)
    with embeddings.start(7340):
        embedding_client = OpenAIEmbeddings(api_base="http://localhost:7340")
        server.vectorise_graph(
            name=GRAPH_NAME,
            embeddings=embedding_client,
            nodes="{{ name }}",
            edges=False,
        )
        with server.start() as server:
            client = server.get_client()
            assert_correct_documents(client)
