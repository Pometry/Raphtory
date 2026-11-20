import raphtory
from raphtory.graphql import GraphServer
import tempfile


def test_version_query():
    work_dir = tempfile.mkdtemp()

    with GraphServer(work_dir).start() as server:
        client = server.get_client()
        assert client.query("{version}")["version"] == raphtory.version()
