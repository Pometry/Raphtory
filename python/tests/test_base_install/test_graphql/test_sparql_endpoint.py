"""The SPARQL 1.1 Protocol endpoint `/sparql/<graph path>` of the GraphQL server,
queried with standard SPARQL clients (SPARQLWrapper and rdflib).

The graph has, from time 1, `alice knows bob`, `bob knows carol` and
`alice age 42`; `alice knows bob` is retracted at 5.
"""

import csv
import io
import json
import tempfile
import urllib.error
import urllib.parse
import urllib.request

import pytest
from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer

SPARQLWrapper = pytest.importorskip("SPARQLWrapper")
rdflib = pytest.importorskip("rdflib")
from rdflib.plugins.stores.sparqlstore import SPARQLStore  # noqa: E402

PEOPLE = b"""
@prefix ex: <http://ex/> .
ex:alice ex:knows ex:bob ; ex:age 42 .
ex:bob ex:knows ex:carol .
"""
RETRACTED = b"<http://ex/alice> <http://ex/knows> <http://ex/bob> ."
KNOWS = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o"
KNOWN_BY = (
    "PREFIX ex: <http://ex/> CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }"
)


def people():
    g = PersistentGraph()
    g.load_rdf(1, PEOPLE)
    g.retract_rdf(5, RETRACTED)
    return g


def years():
    """`alice` knows `carol`, and met `bob` in 2023: the layer `2023` is the
    predicate `<raphtory:2023>`, which RDF/XML cannot write."""
    g = Graph()
    g.add_edge(1, "alice", "bob", layer="2023")
    g.add_edge(1, "alice", "carol", layer="knows")
    return g


@pytest.fixture(scope="module")
def endpoint():
    """The URL of the endpoint of the graph `team/people` on a running server,
    which also has the graph `team/years`."""
    with GraphServer(tempfile.mkdtemp()).start() as server:
        server.get_client().send_graph(path="team/people", graph=people())
        server.get_client().send_graph(path="team/years", graph=years())
        yield f"http://localhost:{server.port()}/sparql/team/people"


def wrapper(endpoint, query, return_format):
    sparql = SPARQLWrapper.SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(return_format)
    return sparql


def pairs(*pairs):
    return [(f"http://ex/{s}", f"http://ex/{o}") for s, o in pairs]


def test_sparqlwrapper_select_json(endpoint):
    results = wrapper(endpoint, KNOWS, SPARQLWrapper.JSON).query().convert()
    assert results["head"]["vars"] == ["s", "o"]
    assert results["results"]["bindings"] == [
        {
            "s": {"type": "uri", "value": "http://ex/bob"},
            "o": {"type": "uri", "value": "http://ex/carol"},
        }
    ]
    # the same results as in Python
    assert results == json.loads(people().sparql(KNOWS, format="json"))


def test_sparqlwrapper_post(endpoint):
    sparql = wrapper(endpoint, KNOWS, SPARQLWrapper.JSON)
    sparql.setMethod(SPARQLWrapper.POST)
    results = sparql.query().convert()
    assert results == json.loads(people().sparql(KNOWS, format="json"))


def test_sparqlwrapper_select_xml(endpoint):
    document = wrapper(endpoint, KNOWS, SPARQLWrapper.XML).query().convert()
    uris = [node.firstChild.data for node in document.getElementsByTagName("uri")]
    assert uris == ["http://ex/bob", "http://ex/carol"]


def test_sparqlwrapper_select_csv(endpoint):
    data = wrapper(endpoint, KNOWS, SPARQLWrapper.CSV).query().convert()
    assert data.decode() == people().sparql(KNOWS, format="csv")
    assert list(csv.reader(io.StringIO(data.decode()))) == [
        ["s", "o"],
        ["http://ex/bob", "http://ex/carol"],
    ]


def test_sparqlwrapper_ask(endpoint):
    ask = "ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }"
    assert wrapper(endpoint, ask, SPARQLWrapper.JSON).query().convert() == {
        "head": {},
        "boolean": True,
    }
    retracted = "ASK { <http://ex/alice> <http://ex/knows> <http://ex/bob> }"
    result = wrapper(endpoint, retracted, SPARQLWrapper.JSON).query().convert()
    assert result["boolean"] is False


def test_sparqlwrapper_construct(endpoint):
    expected = {
        (
            rdflib.URIRef("http://ex/carol"),
            rdflib.URIRef("http://ex/knownBy"),
            rdflib.URIRef("http://ex/bob"),
        )
    }
    # RDF/XML, which SPARQLWrapper parses into an rdflib graph
    graph = wrapper(endpoint, KNOWN_BY, SPARQLWrapper.XML).query().convert()
    assert set(graph) == expected
    # Turtle
    turtle = wrapper(endpoint, KNOWN_BY, SPARQLWrapper.TURTLE).query().convert()
    assert set(rdflib.Graph().parse(data=turtle, format="turtle")) == expected


def test_rdf_xml_never_drops_triples(endpoint):
    years_endpoint = endpoint.replace("team/people", "team/years")
    everything = "CONSTRUCT WHERE { ?s ?p ?o }"
    # SPARQLWrapper asks for RDF/XML only, which cannot hold `<raphtory:2023>`:
    # an error rather than a graph with a triple missing
    with pytest.raises(urllib.error.HTTPError) as error:
        wrapper(years_endpoint, everything, SPARQLWrapper.XML).query()
    assert error.value.code == 406
    assert "RDF/XML cannot hold 1 of the 2 triples" in error.value.read().decode()
    # Turtle holds them all
    turtle = wrapper(years_endpoint, everything, SPARQLWrapper.TURTLE).query().convert()
    alice = rdflib.URIRef("raphtory:alice")
    assert set(rdflib.Graph().parse(data=turtle, format="turtle")) == {
        (alice, rdflib.URIRef("raphtory:2023"), rdflib.URIRef("raphtory:bob")),
        (alice, rdflib.URIRef("raphtory:knows"), rdflib.URIRef("raphtory:carol")),
    }


def test_time_travel_with_default_graph_uri(endpoint):
    sparql = wrapper(endpoint, KNOWS, SPARQLWrapper.JSON)
    sparql.addDefaultGraph("raphtory:asof:3")
    results = sparql.query().convert()
    rows = [
        (row["s"]["value"], row["o"]["value"])
        for row in results["results"]["bindings"]
    ]
    assert rows == pairs(("alice", "bob"), ("bob", "carol"))
    # the same as a snapshot in Python
    assert results == json.loads(people().snapshot_at(3).sparql(KNOWS, format="json"))


def test_rdflib_sparqlstore(endpoint):
    store = SPARQLStore(query_endpoint=endpoint)
    # without an IRI identifier, so rdflib sends no `default-graph-uri`
    graph = rdflib.Graph(store=store)
    rows = [(str(s), str(o)) for s, o in graph.query(KNOWS)]
    assert rows == pairs(("bob", "carol"))
    age = graph.query("SELECT ?age { <http://ex/alice> <http://ex/age> ?age }")
    assert [row.age.toPython() for row in age] == [42]

    constructed = graph.query(KNOWN_BY).graph
    assert set(constructed) == {
        (
            rdflib.URIRef("http://ex/carol"),
            rdflib.URIRef("http://ex/knownBy"),
            rdflib.URIRef("http://ex/bob"),
        )
    }


def test_errors_are_http_status_codes(endpoint):
    def status(url, **kwargs):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, **kwargs)):
                return 200
        except urllib.error.HTTPError as error:
            return error.code

    query = lambda q: endpoint + "?" + urllib.parse.urlencode({"query": q})
    assert status(query(KNOWS)) == 200
    assert status(query("SELECT ?x WHERE { ?x")) == 400
    assert status(query(KNOWS).replace("team/people", "team/nobody")) == 404
    # the service description, too
    assert status(endpoint) == 200
    assert status(endpoint.replace("team/people", "team/nobody")) == 404
    assert status(query(KNOWS), headers={"Accept": "image/png"}) == 406
    assert (
        status(
            endpoint,
            data=b"INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }",
            headers={"Content-Type": "application/sparql-update"},
        )
        == 501
    )
    with pytest.raises(SPARQLWrapper.SPARQLExceptions.QueryBadFormed):
        wrapper(endpoint, "SELECT ?x WHERE { ?x", SPARQLWrapper.JSON).query()
