import tempfile

from pandas.core.generic import gc
from raphtory.graphql import GraphServer, RaphtoryClient

def test_snapshot():
    work_dir = tempfile.mkdtemp()
    server = GraphServer(work_dir)
    with server.start():
        client = RaphtoryClient("http://localhost:1736")

        def query(graph: str, window: str):
            return client.query(f"""{{
                graph(path: "{graph}") {{
                    window: {window} {{
                        edges {{
                            list {{
                                src {{
                                    name
                                }}
                                dst {{
                                    name
                                }}
                            }}
                        }}
                    }}
                }}
            }}""")

        client.new_graph("event", "EVENT")
        g = client.remote_graph("event")
        g.add_edge(1, 1, 2)
        g.add_edge(2, 1, 3)

        for time in range(0, 4):
            assert query("event", f"before(time: {time + 1})") == query("event", f"snapshotAt(time: {time})")
        assert query("event", f"before(time: 1000)") == query("event", f"snapshotLatest")

        client.new_graph("persistent", "PERSISTENT")
        g = client.remote_graph("persistent")
        g.add_edge(1, 1, 2)
        g.add_edge(2, 1, 3)
        g.delete_edge(3, 1, 2)

        for time in range(0, 5):
            assert query("persistent", f"at(time: {time})") == query("persistent", f"snapshotAt(time: {time})")
        assert query("persistent", f"latest") == query("persistent", f"snapshotLatest")
