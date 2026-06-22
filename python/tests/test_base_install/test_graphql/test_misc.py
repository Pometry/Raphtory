import raphtory
from raphtory.graphql import GraphServer
from raphtory.graphql import RaphtoryClient
import tempfile
import time


def test_version_query():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start() as server:
        client = server.get_client()
        assert client.query("{version}")["version"] == raphtory.version()
