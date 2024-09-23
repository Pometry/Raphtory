import os
import tempfile

from raphtory import Graph
from raphtory.graphql import GraphServer, RaphtoryClient


def test_graph_file_time_stats():
    g = Graph()
    g.add_edge(1, "ben", "hamza")
    g.add_edge(2, "haaroon", "hamza")
    g.add_edge(3, "ben", "haaroon")

    work_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(work_dir, "shivam"), exist_ok=True)
    graph_file_path = os.path.join(work_dir, "shivam", "g3")
    g.save_to_file(graph_file_path)

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")

        query = """{graph(path: "shivam/g3") { created lastOpened lastUpdated }}"""
        result = client.query(query)

        gql_created_time = result["graph"]["created"]
        gql_last_opened_time = result["graph"]["lastOpened"]
        gql_last_updated_time = result["graph"]["lastUpdated"]

        file_stats = os.stat(graph_file_path)
        created_time_fs = file_stats.st_ctime * 1000
        last_opened_time_fs = file_stats.st_atime * 1000
        last_updated_time_fs = file_stats.st_mtime * 1000

        assert (
            abs(gql_created_time - created_time_fs) < 1000
        ), f"Mismatch in created time: FS({created_time_fs}) vs GQL({gql_created_time})"
        assert (
            abs(gql_last_opened_time - last_opened_time_fs) < 1000
        ), f"Mismatch in last opened time: FS({last_opened_time_fs}) vs GQL({gql_last_opened_time})"
        assert (
            abs(gql_last_updated_time - last_updated_time_fs) < 1000
        ), f"Mismatch in last updated time: FS({last_updated_time_fs}) vs GQL({gql_last_updated_time})"
