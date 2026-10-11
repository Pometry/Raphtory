"""The SPARQL 1.1 Protocol endpoint `/sparql/<graph path>` of the GraphQL server."""

import json

import pytest
import rdflib
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from raphtory import RdfGraph
from SPARQLWrapper import GET, JSON, POST, SPARQLWrapper
from utils import graphql_server

PEOPLE = b"""
@prefix ex: <http://ex/> .
ex:alice ex:knows ex:bob ; ex:age 42 .
ex:bob ex:knows ex:carol .
"""
KNOWS = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o"
KNOWN_BY = (
    "PREFIX ex: <http://ex/> CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }"
)


def people():
    """From 1: alice knows bob (retracted at 5), bob knows carol, alice age 42."""
    rg = RdfGraph()
    rg.load_rdf(1, PEOPLE)
    rg.retract_triple(5, "http://ex/alice", "http://ex/knows", "http://ex/bob")
    return rg


@pytest.fixture(scope="module")
def server():
    with graphql_server() as server:
        server.get_client().send_graph(path="team/people", graph=people().graph)
        yield server


@pytest.fixture(scope="module")
def endpoint(server):
    return f"http://localhost:{server.port()}/sparql/team/people"


def test_sparqlwrapper(endpoint):
    for method in [GET, POST]:
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(KNOWS)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(method)
        results = sparql.query().convert()
        assert results == json.loads(
            people().valid().sparql(KNOWS, format="json")
        ), method


def test_rdflib_sparqlstore(endpoint):
    # without an IRI identifier, so rdflib sends no `default-graph-uri`
    graph = rdflib.Graph(store=SPARQLStore(query_endpoint=endpoint))
    assert [(str(s), str(o)) for s, o in graph.query(KNOWS)] == [
        ("http://ex/bob", "http://ex/carol")
    ]
    age = graph.query("SELECT ?age { <http://ex/alice> <http://ex/age> ?age }")
    assert [row.age.toPython() for row in age] == [42]
    assert set(graph.query(KNOWN_BY).graph) == {
        (
            rdflib.URIRef("http://ex/carol"),
            rdflib.URIRef("http://ex/knownBy"),
            rdflib.URIRef("http://ex/bob"),
        )
    }
