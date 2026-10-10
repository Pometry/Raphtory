"""The `rdfGraph` field and the SPARQL 1.1 Protocol endpoint `/sparql/<graph path>` of the
GraphQL server.

The graph has, from time 1, `alice knows bob`, `bob knows carol` and `alice age 42`;
`alice knows bob` is retracted at 5.
"""

import json
import tempfile

import pytest
from raphtory import RdfGraph
from raphtory.graphql import GraphServer

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
    rg = RdfGraph()
    rg.load_rdf(1, PEOPLE)
    rg.retract_triple(5, "http://ex/alice", "http://ex/knows", "http://ex/bob")
    return rg


def pairs(*pairs):
    return [(f"http://ex/{s}", f"http://ex/{o}") for s, o in pairs]


@pytest.fixture(scope="module")
def server():
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            server.get_client().send_graph(path="team/people", graph=people().graph)
            yield server


@pytest.fixture(scope="module")
def endpoint(server):
    return f"http://localhost:{server.port()}/sparql/team/people"


def test_rdf_graph_field(server):
    query = json.dumps(KNOWS)
    results = server.get_client().query(
        f'{{ rdfGraph(path: "team/people") {{ valid {{ sparql(query: {query}) }} }} }}'
    )
    document = json.loads(results["rdfGraph"]["valid"]["sparql"])
    rows = [(row["s"]["value"], row["o"]["value"]) for row in document["results"]["bindings"]]
    assert rows == pairs(("bob", "carol"))


def test_sparqlwrapper(endpoint):
    SPARQLWrapper = pytest.importorskip("SPARQLWrapper")
    for method in [SPARQLWrapper.GET, SPARQLWrapper.POST]:
        sparql = SPARQLWrapper.SPARQLWrapper(endpoint)
        sparql.setQuery(KNOWS)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setMethod(method)
        results = sparql.query().convert()
        assert results == json.loads(
            people().valid().sparql(KNOWS, format="json")
        ), method


def test_rdflib_sparqlstore(endpoint):
    rdflib = pytest.importorskip("rdflib")
    from rdflib.plugins.stores.sparqlstore import SPARQLStore

    # without an IRI identifier, so rdflib sends no `default-graph-uri`
    graph = rdflib.Graph(store=SPARQLStore(query_endpoint=endpoint))
    assert [(str(s), str(o)) for s, o in graph.query(KNOWS)] == pairs(
        ("bob", "carol")
    )
    age = graph.query("SELECT ?age { <http://ex/alice> <http://ex/age> ?age }")
    assert [row.age.toPython() for row in age] == [42]
    assert set(graph.query(KNOWN_BY).graph) == {
        (
            rdflib.URIRef("http://ex/carol"),
            rdflib.URIRef("http://ex/knownBy"),
            rdflib.URIRef("http://ex/bob"),
        )
    }
