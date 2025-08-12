import tempfile

from raphtory.graphql import GraphServer
from raphtory.graphql import add_custom_gql_apis


def test_hello_world():
    work_dir = tempfile.mkdtemp()
    server = GraphServer(work_dir)
    add_custom_gql_apis()
    with server.start() as server:
        client = server.get_client()
        query = """query {
            plugins {
                hello_query(name:"Shivam")
            }
        }"""
        result = client.query(query)
        assert result["plugins"]["hello_query"] == "Hello, Shivam"
