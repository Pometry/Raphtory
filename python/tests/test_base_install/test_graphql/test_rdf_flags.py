"""The `disable_rdf`, `disable_sparql_endpoint` and `disable_shacl` settings of the GraphQL server."""

import tempfile
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

import pytest
from raphtory import RdfGraph
from raphtory.graphql import GraphServer
from utils import graphql_server

ROOT_FIELDS = "{ __schema { queryType { fields { name } } } }"
RDF_GRAPH_FIELDS = '{ __type(name: "RdfGraph") { fields { name } } }'


def test_rdf_and_the_endpoint_are_on_by_default():
    schema = GraphServer(tempfile.mkdtemp()).config_schema()["schema"]
    assert schema["disable_rdf"] is False
    assert schema["disable_sparql_endpoint"] is False
    assert schema["disable_shacl"] is False


@pytest.mark.parametrize(
    "config, rdf, endpoint, shacl",
    [
        ({}, True, True, True),
        ({"disable_sparql_endpoint": True}, True, False, True),
        ({"disable_shacl": True}, True, True, False),
        ({"disable_sparql_endpoint": True, "disable_shacl": True}, True, False, False),
        # `disable_rdf` turns off the endpoint and SHACL too, whatever their own flags say
        ({"disable_rdf": True}, False, False, False),
        ({"disable_rdf": True, "disable_shacl": True}, False, False, False),
        ({"disable_rdf": True, "disable_sparql_endpoint": True}, False, False, False),
        (
            {
                "disable_rdf": True,
                "disable_sparql_endpoint": True,
                "disable_shacl": True,
            },
            False,
            False,
            False,
        ),
    ],
)
def test_the_flags_hide_rdf_graph_the_endpoint_and_shacl(config, rdf, endpoint, shacl):
    with graphql_server({"schema": config}) as server:
        rg = RdfGraph()
        rg.load_rdf(1, b"<http://ex/a> <http://ex/b> <http://ex/c> .")
        client = server.get_client()
        client.send_graph(path="g", graph=rg.graph)

        fields = client.query(ROOT_FIELDS)["__schema"]["queryType"]["fields"]
        assert ("rdfGraph" in {field["name"] for field in fields}) == rdf
        rdf_graph = client.query(RDF_GRAPH_FIELDS)["__type"]
        assert (rdf_graph is not None) == rdf
        rdf_graph_fields = {
            field["name"] for field in (rdf_graph or {"fields": []})["fields"]
        }
        assert ("validateShacl" in rdf_graph_fields) == shacl

        url = f"http://localhost:{server.port()}/sparql/g?query={quote('ASK {}')}"
        try:
            with urlopen(url) as response:
                status = response.status
        except HTTPError as error:
            status = error.code
        assert status == (200 if endpoint else 404)
