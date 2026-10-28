"""The SPARQL 1.1 Protocol endpoint `/sparql/<graph path>` of the GraphQL server."""

import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest
import rdflib
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from raphtory import RdfGraph
from SPARQLWrapper import (
    CSV,
    GET,
    JSON,
    POST,
    POSTDIRECTLY,
    TURTLE,
    URLENCODED,
    XML,
    SPARQLWrapper,
)
from SPARQLWrapper.SPARQLExceptions import EndPointNotFound
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
KNOWN_BY_NOW = {
    (
        rdflib.URIRef("http://ex/carol"),
        rdflib.URIRef("http://ex/knownBy"),
        rdflib.URIRef("http://ex/bob"),
    )
}
NOW = [("bob", "carol")]
BEFORE_2024 = [("alice", "bob"), ("bob", "carol")]


def people():
    """From 1: alice knows bob (retracted at 2024-01-01), bob knows carol, alice age 42."""
    rg = RdfGraph()
    rg.load_rdf(1, PEOPLE)
    rg.retract_triple(
        "2024-01-01", "http://ex/alice", "http://ex/knows", "http://ex/bob"
    )
    return rg


@pytest.fixture(scope="module")
def server():
    with graphql_server() as server:
        server.get_client().send_graph(path="team/the people", graph=people().graph)
        yield server


@pytest.fixture(scope="module")
def endpoint(server):
    return f"http://localhost:{server.port()}/sparql/team/the%20people"


def results(endpoint, query, return_format=JSON, asof=None):
    sparql = SPARQLWrapper(endpoint, returnFormat=return_format)
    sparql.setQuery(query)
    if asof is not None:
        sparql.addParameter("asof", asof)
    return sparql.query().convert()


def fetch(url, method="GET", headers={}):
    with urlopen(Request(url, method=method, headers=headers)) as response:
        return response.status, response.headers, response.read()


def pairs(results):
    """The (?s, ?o) rows of SPARQL JSON results, without the `http://ex/` of the IRIs."""
    return [
        tuple(row[name]["value"].removeprefix("http://ex/") for name in ("s", "o"))
        for row in results["results"]["bindings"]
    ]


def test_sparqlwrapper(endpoint):
    for method, request_method in [
        (GET, URLENCODED),
        (POST, URLENCODED),
        (POST, POSTDIRECTLY),
    ]:
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(KNOWS)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(method)
        sparql.setRequestMethod(request_method)
        assert sparql.query().convert() == json.loads(
            people().valid().sparql(KNOWS, format="json")
        ), (method, request_method)


def test_the_accept_header_picks_the_format(endpoint):
    ask = "ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }"
    assert results(endpoint, ask) == {"head": {}, "boolean": True}
    assert results(endpoint, KNOWS, CSV).decode() == people().valid().sparql(
        KNOWS, format="csv"
    )
    # RDF/XML, what SPARQLWrapper asks for by default
    assert set(results(endpoint, KNOWN_BY, XML)) == KNOWN_BY_NOW
    turtle = results(endpoint, KNOWN_BY, TURTLE)
    assert set(rdflib.Graph().parse(data=turtle, format="turtle")) == KNOWN_BY_NOW

    known_by = f"{endpoint}?{urlencode({'query': KNOWN_BY})}"
    _, headers, turtle = fetch(known_by)
    assert headers.get_content_type() == "text/turtle"
    assert set(rdflib.Graph().parse(data=turtle, format="turtle")) == KNOWN_BY_NOW
    accept = {"Accept": "text/turtle;q=0.5, application/n-triples"}
    _, headers, _ = fetch(known_by, headers=accept)
    assert headers.get_content_type() == "application/n-triples"
    accept = {"Accept": "application/sparql-results+json;q=0, */*"}
    _, headers, _ = fetch(f"{endpoint}?{urlencode({'query': KNOWS})}", headers=accept)
    assert headers.get_content_type() == "application/json"


def test_answers_vary_by_accept_and_options_lists_the_methods(endpoint):
    url = f"{endpoint}?{urlencode({'query': KNOWS})}"
    _, headers, _ = fetch(url, headers={"Origin": "http://ex"})
    assert ", ".join(headers.get_all("vary")) == "Origin, Accept"
    status, headers, _ = fetch(endpoint, method="OPTIONS")
    assert (status, headers["allow"]) == (204, "GET, POST, OPTIONS")


def test_the_view_is_the_current_state_unless_a_time_is_given(endpoint):
    assert pairs(results(endpoint, KNOWS, asof="3")) == BEFORE_2024
    assert pairs(results(endpoint, KNOWS, asof="2023-12-31")) == BEFORE_2024
    assert pairs(results(endpoint, KNOWS, asof="2024-01-01")) == NOW
    in_time_graph = (
        "PREFIX ex: <http://ex/> SELECT ?s ?o "
        "{ GRAPH raphtory:asof:2023-12-31 { ?s ex:knows ?o } } ORDER BY ?s ?o"
    )
    assert pairs(results(endpoint, in_time_graph)) == BEFORE_2024
    # a time graph that is only an argument keeps the current state
    since = (
        "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o "
        "BIND(raphtory:validFrom(?s, ex:knows, ?o, raphtory:asof:2023-12-31) AS ?since) } "
        "ORDER BY ?s ?o"
    )
    assert pairs(results(endpoint, since)) == NOW


def test_a_missing_graph_is_not_found(server):
    missing = f"http://localhost:{server.port()}/sparql/team/missing"
    with pytest.raises(EndPointNotFound, match="Graph 'team/missing' does not exist"):
        results(missing, KNOWS)


def test_rdflib_sparqlstore(endpoint):
    graph = rdflib.Graph(store=SPARQLStore(query_endpoint=endpoint))
    assert [(str(s), str(o)) for s, o in graph.query(KNOWS)] == [
        ("http://ex/bob", "http://ex/carol")
    ]
    age = graph.query("SELECT ?age { <http://ex/alice> <http://ex/age> ?age }")
    assert [row.age.toPython() for row in age] == [42]
    assert set(graph.query(KNOWN_BY).graph) == KNOWN_BY_NOW
