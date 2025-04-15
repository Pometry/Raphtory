import tempfile
import pytest
from raphtory import Graph
from raphtory.graphql import RaphtoryClient
from raphtory.graphql import GraphServer


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_latest_and_active():
    query = """
        {
          graph(path: "graph") {
            searchNodes(query: "a", limit: 1, offset: 0) {
              name
            }
          }
        }
    """
    work_dir = tempfile.mkdtemp()
    g = Graph()
    g.save_to_file(work_dir + "/graph")
    with GraphServer(work_dir).turn_off_index().start():
        client = RaphtoryClient("http://localhost:1736")
        with pytest.raises(Exception) as excinfo:
            client.query(query)
        assert (
            "An operation tried to make use of the graph index but indexing has been turned off for the server"
            in str(excinfo.value)
        )
