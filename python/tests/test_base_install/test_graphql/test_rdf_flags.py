"""The `disable_rdf` and `disable_sparql_endpoint` settings of the GraphQL server."""

import tempfile
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

import pytest
from raphtory import RdfGraph
from raphtory.graphql import GraphServer
from utils import graphql_server

ROOT_FIELDS = "{ __schema { queryType { fields { name } } } }"


def test_rdf_and_the_endpoint_are_on_by_default():
    schema = GraphServer(tempfile.mkdtemp()).config_schema()["schema"]
    assert schema["disable_rdf"] is False
    assert schema["disable_sparql_endpoint"] is False


@pytest.mark.parametrize(
    "config, rdf, endpoint",
    [
        ({}, True, True),
        ({"disable_sparql_endpoint": True}, True, False),
        ({"disable_rdf": True, "disable_sparql_endpoint": True}, False, False),
    ],
)
def test_the_flags_hide_rdf_graph_and_the_endpoint(config, rdf, endpoint):
    with graphql_server({"schema": config}) as server:
        rg = RdfGraph()
        rg.load_rdf(1, b"<http://ex/a> <http://ex/b> <http://ex/c> .")
        client = server.get_client()
        client.send_graph(path="g", graph=rg.graph)

        fields = client.query(ROOT_FIELDS)["__schema"]["queryType"]["fields"]
        assert ("rdfGraph" in {field["name"] for field in fields}) == rdf

        url = f"http://localhost:{server.port()}/sparql/g?query={quote('ASK {}')}"
        try:
            with urlopen(url) as response:
                status = response.status
        except HTTPError as error:
            status = error.code
        assert status == (200 if endpoint else 404)


def test_the_endpoint_without_rdf_fails_at_startup():
    with pytest.raises(Exception, match="disable_sparql_endpoint"):
        GraphServer(tempfile.mkdtemp(), config={"schema": {"disable_rdf": True}})
