import raphtory
from raphtory.graphql import GraphServer
from raphtory.graphql import RaphtoryClient
import tempfile
import time


def test_version_query():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start():
        client = RaphtoryClient("http://localhost:1736")
        assert client.query("{version}")["version"] == raphtory.version()
