//! SPARQL over graph views.
//!
//! The differential tests evaluate each query twice with the same evaluator: on the graph, and
//! on an `oxrdf::Dataset` holding the same triples. The `Dataset` keeps terms exactly as
//! written (an oxigraph `Store` would normalise literals such as `"042"^^xsd:integer`).
use super::{ask, select, within_60s};
use crate::{
    db::api::view::{filter_ops::Filter as _, internal::CoreGraphOps, DynamicGraph, IntoDynamic},
    errors::GraphError,
    prelude::*,
    rdf::{
        evaluator,
        model::{
            vocab::xsd, BlankNode, Dataset, GraphNameRef, Literal, NamedNode, NamedOrBlankNode,
            Term, Triple,
        },
        RaphtoryDataset, RdfError, RdfFormat, RdfParser, RdfTerm, SparqlResults, TimeGraph,
        Variable, ASOF_NS,
    },
};
use proptest::prelude::*;
use raphtory_api::core::utils::hashing::calculate_hash;
use spareval::QueryableDataset;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

pub(super) const FOAF: &str = include_str!("fixtures/foaf.ttl");
pub(super) const DATATYPES: &str = include_str!("fixtures/datatypes.nt");
pub(super) const BLANK_NODES: &str = include_str!("fixtures/blank_nodes.ttl");
pub(super) const SOCIAL: &str = include_str!("fixtures/social.ttl");

pub(super) const PREFIXES: &str = "PREFIX ex: <http://ex/>
PREFIX people: <http://example.org/people/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
";

/// How a query of the differential test is checked.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Kind {
    /// Results compared as multisets (sets for graphs); not empty on the union of the fixtures.
    Rows,
    /// As `Rows`, and the rows are also compared in order (the query has a total `ORDER BY`).
    Ordered,
    /// Empty (or `false`) on the union of the fixtures.
    Empty,
}

use Kind::{Empty, Ordered, Rows};

/// The query set of the differential harness.
pub(super) const QUERIES: &[(&str, Kind)] = &[
    // the 8 binding patterns, with IRIs
    ("SELECT * { ex:alice ex:knows ex:bob }", Rows),
    ("SELECT * { ex:alice ex:knows ex:carol }", Empty),
    ("SELECT ?o { ex:alice ex:knows ?o }", Rows),
    ("SELECT ?s { ?s ex:knows ex:bob }", Rows),
    ("SELECT ?s ?o { ?s ex:knows ?o }", Rows),
    ("SELECT ?p { ex:alice ?p ex:bob }", Rows),
    ("SELECT ?p ?o { ex:alice ?p ?o }", Rows),
    ("SELECT ?s ?p { ?s ?p ex:bob }", Rows),
    ("SELECT * { ?s ?p ?o }", Rows),
    ("SELECT ?p ?o { people:alice ?p ?o }", Rows),
    ("SELECT ?s ?o { ?s foaf:knows ?o }", Rows),
    // ... with literals, blank nodes and terms that are not in the data
    ("SELECT ?s { ?s foaf:age 30 }", Rows),
    ("SELECT ?s ?p { ?s ?p \"Alice\" }", Rows),
    ("SELECT ?s ?p { ?s ?p \"Alice\"@en }", Rows),
    ("SELECT ?s { ?s foaf:name \"Carol\"@en-gb }", Rows),
    ("SELECT ?s { ?s foaf:name \"Carol\"@EN-GB }", Rows),
    ("SELECT ?s ?p { ?s ?p \"042\"^^xsd:integer }", Rows),
    ("SELECT ?s ?p { ?s ?p 42 }", Rows),
    ("SELECT ?s ?p { ?s ?p \"1\"^^xsd:int }", Rows),
    ("SELECT ?s ?p { ?s ?p \"typed\"^^xsd:string }", Rows),
    ("SELECT ?s ?p { ?s ?p \"nope\" }", Empty),
    ("SELECT * { ex:alice ex:knows \"Alice\" }", Empty),
    ("SELECT * { ex:nobody ?p ?o }", Empty),
    ("SELECT * { ?s ex:nothing ?o }", Empty),
    ("SELECT * { ?s ?p ex:nobody }", Empty),
    ("SELECT * { ex:nobody ex:knows ex:bob }", Empty),
    ("SELECT * { ?s ?p ?o FILTER(isLiteral(?s)) }", Empty),
    ("SELECT ?p ?o { <http://ex/s> ?p ?o }", Rows),
    ("SELECT ?o { <http://ex/s> <http://ex/int> ?o }", Rows),
    ("SELECT ?s { ?s ?p \"1\"^^xsd:boolean }", Rows),
    (
        "SELECT ?s ?p ?o { ?s ?p ?o FILTER(isBlank(?s) || isBlank(?o)) }",
        Rows,
    ),
    ("SELECT ?p ?o { ?b ex:city \"Paris\" . ?b ?p ?o }", Rows),
    ("SELECT ?x { _:anything ex:member ?x }", Rows),
    // joins
    ("SELECT ?a ?b ?c { ?a ex:knows ?b . ?b ex:knows ?c }", Rows),
    ("SELECT ?a ?b { ?a ex:knows ?b . ?b ex:knows ?a }", Rows),
    ("SELECT ?x ?n { ?x a foaf:Person ; foaf:name ?n }", Rows),
    ("SELECT ?p ?l ?s ?o { ?p rdfs:label ?l . ?s ?p ?o }", Rows),
    (
        "SELECT ?s ?p ?o { ?p rdfs:subPropertyOf ex:relatedTo . ?s ?p ?o }",
        Rows,
    ),
    ("SELECT ?g ?n { ?g ex:member ?x . ?x foaf:name ?n }", Rows),
    ("SELECT * { ?a ?p ?b . ?b ?q ?c . ?c ?r ?a }", Rows),
    // OPTIONAL, UNION, MINUS, FILTER (NOT) EXISTS
    (
        "SELECT ?x ?age { ?x foaf:name ?n OPTIONAL { ?x foaf:age ?age } }",
        Rows,
    ),
    (
        "SELECT ?x ?y { ?x a foaf:Person OPTIONAL { ?x ex:likes ?y } }",
        Rows,
    ),
    (
        "SELECT ?x ?y ?z { ?x ex:knows ?y OPTIONAL { ?y ex:likes ?z } }",
        Rows,
    ),
    (
        "SELECT ?x ?y { { ?x ex:knows ?y } UNION { ?x ex:likes ?y } }",
        Rows,
    ),
    (
        "SELECT ?x ?y ?n { { ?x ex:knows ?y } UNION { ?x foaf:name ?n } }",
        Rows,
    ),
    (
        "SELECT ?x { ?x a foaf:Person MINUS { ?x ex:likes ?y } }",
        Rows,
    ),
    ("SELECT ?s ?o { ?s ?p ?o MINUS { ?s ex:knows ?o } }", Rows),
    (
        "SELECT ?x { ?x foaf:name ?n FILTER EXISTS { ?x ex:knows ?y } }",
        Rows,
    ),
    (
        "SELECT ?x { ?x foaf:name ?n FILTER NOT EXISTS { ?y ex:knows ?x } }",
        Rows,
    ),
    // numeric, language and regex filters
    (
        "SELECT ?x ?age { ?x foaf:age ?age FILTER(?age > 28) }",
        Rows,
    ),
    (
        "SELECT ?x ?age { ?x foaf:age ?age FILTER(?age = 41) }",
        Rows,
    ),
    (
        "SELECT ?x ?age { ?x foaf:age ?age FILTER(?age = 30) }",
        Rows,
    ),
    (
        "SELECT ?s ?o { ?s ?p ?o FILTER(isNumeric(?o) && ?o >= 1) }",
        Rows,
    ),
    (
        "SELECT ?o { <http://ex/s> ?p ?o FILTER(datatype(?o) = xsd:integer) }",
        Rows,
    ),
    (
        "SELECT ?x ?n { ?x foaf:name ?n FILTER(lang(?n) = \"en\") }",
        Rows,
    ),
    (
        "SELECT ?x ?n { ?x foaf:name ?n FILTER(langMatches(lang(?n), \"en\")) }",
        Rows,
    ),
    ("SELECT ?o { ?s ?p ?o FILTER(lang(?o) != \"\") }", Rows),
    (
        "SELECT ?x ?n { ?x foaf:name ?n FILTER regex(?n, \"^[a-c]\", \"i\") }",
        Rows,
    ),
    (
        "SELECT DISTINCT ?s { ?s ?p ?o FILTER regex(str(?s), \"example\\\\.org\") }",
        Rows,
    ),
    (
        "SELECT ?o { ?s ?p ?o FILTER(contains(str(?o), \"\\n\")) }",
        Rows,
    ),
    // property paths
    ("SELECT ?x { ex:alice ex:knows+ ?x }", Rows),
    ("SELECT ?x { ex:alice ex:knows* ?x }", Rows),
    ("SELECT ?x ?y { ?x ex:knows+ ?y }", Rows),
    ("SELECT ?x ?y { ?x ex:next* ?y }", Rows),
    ("SELECT ?x { ex:n1 ex:next+ ?x }", Rows),
    ("SELECT ?x { ?x ex:next+ ex:n2 }", Rows),
    ("SELECT ?x { ex:alice ex:knows? ?x }", Rows),
    ("SELECT ?x { ?x ^ex:knows ex:bob }", Rows),
    ("SELECT ?x { ex:alice ex:knows/ex:knows ?x }", Rows),
    ("SELECT ?x ?y { ?x ex:knows/foaf:name ?y }", Rows),
    ("SELECT ?x { ex:alice (ex:knows|ex:likes) ?x }", Rows),
    ("SELECT ?x ?y { ?x !ex:knows ?y }", Rows),
    (
        "SELECT ?x ?y { ?x !(ex:knows|rdf:type|foaf:name) ?y }",
        Rows,
    ),
    ("SELECT ?x { ?x !^ex:knows ex:bob }", Rows),
    ("SELECT ?x ?y { ?x (ex:knows/ex:knows)* ?y }", Rows),
    ("SELECT ?x ?y { ?x (ex:knows|^ex:likes)+ ?y }", Rows),
    (
        "SELECT ?p { ex:alice ?p ?o . ?p rdfs:subPropertyOf* ex:relatedTo }",
        Rows,
    ),
    ("ASK { ex:dave ex:knows+ ex:dave }", Rows),
    ("ASK { ex:eve ex:knows* ex:eve }", Rows),
    // a zero-length path only matches terms of the data
    ("ASK { ex:nobody ex:knows* ex:nobody }", Empty),
    // aggregates, subqueries, DISTINCT, ORDER BY + LIMIT
    (
        "SELECT ?x (COUNT(?y) AS ?n) { ?x ex:knows ?y } GROUP BY ?x",
        Rows,
    ),
    (
        "SELECT ?p (COUNT(*) AS ?n) { ?s ?p ?o } GROUP BY ?p HAVING (COUNT(*) > 1)",
        Rows,
    ),
    ("SELECT (COUNT(*) AS ?n) { ?s ?p ?o }", Rows),
    (
        "SELECT (COUNT(DISTINCT ?s) AS ?n) (COUNT(DISTINCT ?o) AS ?m) { ?s ?p ?o }",
        Rows,
    ),
    (
        "SELECT (SUM(?a) AS ?sum) (AVG(?a) AS ?avg) (MIN(?a) AS ?min) (MAX(?a) AS ?max) { ?x foaf:age ?a }",
        Rows,
    ),
    (
        "SELECT ?n (COUNT(?x) AS ?c) { ?x foaf:age ?n } GROUP BY ?n",
        Rows,
    ),
    (
        "SELECT ?x ?k { ?x foaf:name ?n { SELECT ?x (COUNT(?y) AS ?k) { ?x ex:knows ?y } GROUP BY ?x } }",
        Rows,
    ),
    (
        "SELECT ?x ?y { { SELECT ?x { ?x a foaf:Person } } ?x ex:knows ?y }",
        Rows,
    ),
    ("SELECT DISTINCT ?p { ?s ?p ?o }", Rows),
    ("SELECT DISTINCT ?s { ?s ?p ?o }", Rows),
    (
        "SELECT DISTINCT ?o { ?s ?p ?o FILTER(isLiteral(?o)) }",
        Rows,
    ),
    (
        "SELECT DISTINCT ?s { ?s ?p ?o FILTER(isIRI(?s)) } ORDER BY ?s",
        Ordered,
    ),
    (
        "SELECT DISTINCT ?s { ?s ?p ?o FILTER(isIRI(?s)) } ORDER BY ?s LIMIT 3",
        Ordered,
    ),
    (
        "SELECT DISTINCT ?s { ?s ?p ?o FILTER(isIRI(?s)) } ORDER BY DESC(?s) LIMIT 4 OFFSET 2",
        Ordered,
    ),
    (
        "SELECT ?x ?y { ?x ex:knows ?y } ORDER BY ?x DESC(?y)",
        Ordered,
    ),
    (
        "SELECT ?x (COUNT(*) AS ?n) { ?x ?p ?o FILTER(isIRI(?x)) } GROUP BY ?x ORDER BY DESC(?n) ?x LIMIT 5",
        Ordered,
    ),
    // VALUES and BIND, with terms that are not in the data
    (
        "SELECT ?x ?y { VALUES ?x { ex:alice ex:nobody \"lit\" 42 ex:knows } OPTIONAL { ?x ex:knows ?y } }",
        Rows,
    ),
    (
        "SELECT ?x ?o { VALUES (?x ?o) { (ex:alice ex:bob) (ex:nobody ex:bob) (ex:carol UNDEF) } ?x ex:knows ?o }",
        Rows,
    ),
    (
        "SELECT ?v ?x { VALUES ?v { 30 \"30\" 30.0 41 } ?x foaf:age ?v }",
        Rows,
    ),
    ("SELECT ?x ?z { ?x foaf:age ?a BIND(?a + 1 AS ?z) }", Rows),
    (
        "SELECT ?x ?z { ?x foaf:name ?n BIND(CONCAT(STR(?n), \"!\") AS ?z) }",
        Rows,
    ),
    (
        "SELECT ?z ?o { BIND(ex:bob AS ?z) ?s ex:knows ?z . ?z ex:knows ?o }",
        Rows,
    ),
    (
        "SELECT ?z ?y { BIND(IRI(\"http://ex/alice\") AS ?z) ?z ex:knows ?y }",
        Rows,
    ),
    (
        "SELECT ?z ?s { BIND(\"Alice\" AS ?z) ?s foaf:name ?z }",
        Rows,
    ),
    (
        "SELECT ?z ?s { BIND(\"nope\" AS ?z) OPTIONAL { ?s foaf:name ?z } }",
        Rows,
    ),
    ("SELECT ?p ?o { BIND(ex:knows AS ?p) ex:alice ?p ?o }", Rows),
    // sameTerm versus =
    (
        "SELECT ?x ?y { ?x foaf:age ?a . ?y foaf:age ?b FILTER(?a = ?b && ?x != ?y) }",
        Rows,
    ),
    (
        "SELECT ?x ?y { ?x foaf:age ?a . ?y foaf:age ?b FILTER(sameTerm(?a, ?b) && ?x != ?y) }",
        Rows,
    ),
    (
        "SELECT ?o { <http://ex/s> <http://ex/int> ?o FILTER(?o = 42) }",
        Rows,
    ),
    (
        "SELECT ?o { <http://ex/s> <http://ex/int> ?o FILTER(sameTerm(?o, 42)) }",
        Rows,
    ),
    (
        "SELECT ?o { <http://ex/s> ?p ?o FILTER(?o = \"typed\") }",
        Rows,
    ),
    ("SELECT ?o { <http://ex/s> ?p ?o FILTER(?o = true) }", Rows),
    ("SELECT ?s ?o { ?s ?p ?o FILTER(sameTerm(?s, ?o)) }", Rows),
    (
        "SELECT ?s ?p { ?s ?p ?o FILTER(sameTerm(?p, ex:knows)) }",
        Rows,
    ),
    // ASK, CONSTRUCT and DESCRIBE
    ("ASK { ex:alice ex:knows ex:bob }", Rows),
    ("ASK { ex:alice ex:knows ex:nobody }", Empty),
    ("ASK { ?s ?p 42 }", Rows),
    ("ASK { ?s ?p \"042\" }", Empty),
    ("ASK {}", Rows),
    (
        "CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }",
        Rows,
    ),
    ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", Rows),
    ("CONSTRUCT WHERE { ?s foaf:name ?n }", Rows),
    (
        "CONSTRUCT { ?s ex:age ?a . ?s ex:older true } WHERE { ?s foaf:age ?a FILTER(?a > 29) }",
        Rows,
    ),
    (
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o FILTER(false) }",
        Empty,
    ),
    ("DESCRIBE ex:alice", Rows),
    ("DESCRIBE ?x WHERE { ?x a foaf:Person }", Rows),
    ("DESCRIBE <http://ex/s> ex:knows", Rows),
    ("DESCRIBE ex:nobody", Empty),
    // repeated variables, predicates as subjects, named graphs
    ("SELECT ?s ?p { ?s ?p ?s }", Rows),
    ("SELECT ?s ?o { ?s ?s ?o }", Empty),
    ("SELECT ?s ?p { ?s ?p ?p }", Empty),
    ("SELECT DISTINCT ?p ?q ?v { ?s ?p ?o . ?p ?q ?v }", Rows),
    ("SELECT ?l { ex:knows rdfs:label ?l }", Rows),
    ("SELECT ?s { ?s ex:mentions ?p . ?a ?p ?b }", Rows),
    ("SELECT * { GRAPH ?g { } }", Empty),
    ("SELECT * { GRAPH ?g { ?s ?p ?o } }", Empty),
    ("SELECT * { GRAPH ex:g { ?s ?p ?o } }", Empty),
    ("SELECT * FROM ex:g { ?s ?p ?o }", Empty),
    ("SELECT * FROM NAMED ex:g { GRAPH ?g { ?s ?p ?o } }", Empty),
];

/// Parses a document keeping blank-node labels.
pub(super) fn parse(doc: &str, format: RdfFormat) -> Vec<Triple> {
    RdfParser::from_format(format)
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap()))
        .collect()
}

/// The same triples in an `oxrdf::Dataset` and (asserted at time 0) in a `PersistentGraph`.
fn load(triples: &[Triple]) -> (Dataset, PersistentGraph) {
    let mut dataset = Dataset::new();
    let pg = PersistentGraph::new();
    for triple in triples {
        dataset.insert(triple.as_ref().in_graph(GraphNameRef::DefaultGraph));
        pg.add_triple(0, triple).unwrap();
    }
    (dataset, pg)
}

fn oracle(dataset: &Dataset, query: &str) -> SparqlResults {
    let results = evaluator()
        .parse_query(query)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .on_queryable_dataset(dataset)
        .execute()
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    SparqlResults::from_query_results(results).unwrap_or_else(|e| panic!("{query}: {e}"))
}

/// Results in a comparable form: rows (sorted unless `ordered`), a boolean, or a set of
/// triples, all in N-Triples form.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Normalized {
    Rows(Vec<String>, Vec<Vec<String>>),
    Boolean(bool),
    Triples(BTreeSet<String>),
}

impl Normalized {
    fn new(results: &SparqlResults, ordered: bool) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => {
                let mut rows: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| v.as_ref().map_or_else(|| "UNDEF".into(), Term::to_string))
                            .collect()
                    })
                    .collect();
                if !ordered {
                    rows.sort();
                }
                Self::Rows(variables.iter().map(|v| v.to_string()).collect(), rows)
            }
            SparqlResults::Boolean(value) => Self::Boolean(*value),
            SparqlResults::Graph(triples) => {
                Self::Triples(triples.iter().map(|t| t.to_string()).collect())
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Rows(_, rows) => rows.is_empty(),
            Self::Boolean(value) => !value,
            Self::Triples(triples) => triples.is_empty(),
        }
    }
}

/// Evaluates `query` on the dataset and on the graph and checks the results are equal.
/// Returns the normalized results.
pub(super) fn assert_same<G: RdfViewOps>(
    dataset: &Dataset,
    g: &G,
    query: &str,
    ordered: bool,
) -> Normalized {
    let expected = Normalized::new(&oracle(dataset, query), ordered);
    let actual = Normalized::new(
        &g.sparql(query).unwrap_or_else(|e| panic!("{query}: {e}")),
        ordered,
    );
    assert_eq!(actual, expected, "{query}");
    actual
}

/// The differential harness over every fixture and their union.
#[test]
fn queries_match_the_oracle() {
    let fixtures = [
        ("foaf", parse(FOAF, RdfFormat::Turtle)),
        ("datatypes", parse(DATATYPES, RdfFormat::NTriples)),
        ("blank_nodes", parse(BLANK_NODES, RdfFormat::Turtle)),
        ("social", parse(SOCIAL, RdfFormat::Turtle)),
    ];
    let union: Vec<Triple> = fixtures.iter().flat_map(|(_, t)| t.clone()).collect();
    for (_, triples) in &fixtures {
        let (dataset, pg) = load(triples);
        for (query, kind) in QUERIES {
            let query = format!("{PREFIXES}{query}");
            assert_same(&dataset, &pg, &query, *kind == Ordered);
        }
    }

    let (dataset, pg) = load(&union);
    // duplicate triples are one quad in the dataset and one SPARQL row in the graph
    assert!(dataset.len() < union.len());
    for (query, kind) in QUERIES {
        let full_query = format!("{PREFIXES}{query}");
        let results = assert_same(&dataset, &pg, &full_query, *kind == Ordered);
        assert_eq!(
            results.is_empty(),
            *kind == Empty,
            "unexpected (non-)empty result for {query}: {results:?}"
        );
        // the same query through other views of the same data
        assert_same(
            &dataset,
            &pg.snapshot_latest(),
            &full_query,
            *kind == Ordered,
        );
        assert_same(&dataset, &pg.event_graph(), &full_query, *kind == Ordered);
    }
}

/// Random graphs over a small vocabulary that mixes IRIs, a blank node, literals
/// that are equal by value but different terms, and an IRI used as predicate and subject.
pub(super) const RANDOM_QUERIES: &[&str] = &[
    "SELECT * { ?s ?p ?o }",
    "SELECT ?s ?o { ?s <http://ex/p> ?o }",
    "SELECT ?p ?o { <http://ex/p> ?p ?o }",
    "SELECT ?s ?p ?q ?v { ?s ?p ?o . ?p ?q ?v }",
    "SELECT ?x ?y { ?x <http://ex/p>+ ?y }",
    "SELECT ?s ?p { ?s ?p 1 }",
    "SELECT ?s ?o { ?s ?p ?o FILTER(?o = 1) }",
    "ASK { ?s <http://ex/q> ?s }",
    "CONSTRUCT { ?o <http://ex/inv> ?s } WHERE { ?s <http://ex/q> ?o FILTER(!isLiteral(?o)) }",
];

pub(super) fn random_triple() -> impl Strategy<Value = Triple> {
    let iri = |s: &str| NamedNode::new_unchecked(s);
    let subjects: Vec<NamedOrBlankNode> = vec![
        iri("http://ex/a").into(),
        iri("http://ex/b").into(),
        iri("http://ex/p").into(),
        BlankNode::new_unchecked("b1").into(),
    ];
    let predicates = vec![iri("http://ex/p"), iri("http://ex/q")];
    let objects: Vec<Term> = vec![
        iri("http://ex/a").into(),
        iri("http://ex/b").into(),
        iri("http://ex/p").into(),
        BlankNode::new_unchecked("b1").into(),
        Literal::new_typed_literal("1", xsd::INTEGER).into(),
        Literal::new_typed_literal("01", xsd::INTEGER).into(),
        Literal::new_language_tagged_literal_unchecked("x", "en").into(),
    ];
    (
        prop::sample::select(subjects),
        prop::sample::select(predicates),
        prop::sample::select(objects),
    )
        .prop_map(|(s, p, o)| Triple::new(s, p, o))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn random_graphs_match_the_oracle(triples in prop::collection::vec(random_triple(), 0..=60)) {
        let (dataset, pg) = load(&triples);
        for query in RANDOM_QUERIES {
            assert_same(&dataset, &pg, query, false);
        }
    }
}

/// Queries with time graphs: the times `T` of the `<raphtory:asof:T>` graphs each names, and
/// whether its result is empty.
pub(super) const TIME_GRAPH_QUERIES: &[(&str, &[&str], Kind)] = &[
    // `?g` bound by `VALUES`, with `OPTIONAL`, `MINUS` and a sub-`SELECT` inside `GRAPH`
    (
        "SELECT ?s { VALUES ?g { raphtory:asof:2 } GRAPH ?g { ?s ?p ?o } }",
        &["2"],
        Rows,
    ),
    (
        "SELECT ?s ?o ?r { VALUES ?g { raphtory:asof:2 } GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }",
        &["2"],
        Rows,
    ),
    (
        "SELECT ?s ?o { VALUES ?g { raphtory:asof:4 } GRAPH ?g { ?s ?p ?o MINUS { ?s ?p ex:c } } }",
        &["4"],
        Rows,
    ),
    // spareval puts `?g` into both sides of a `MINUS` inside `GRAPH ?g`, so they share it and
    // this removes every row (SPARQL would remove none); the oracle does the same
    (
        "SELECT ?s ?o { VALUES ?g { raphtory:asof:4 } GRAPH ?g { ?s ?p ?o MINUS { ?x ?y ?z } } }",
        &["4"],
        Empty,
    ),
    (
        "SELECT ?s { VALUES ?g { raphtory:asof:2 } GRAPH ?g { { SELECT ?s { ?s ?p ?o } } } }",
        &["2"],
        Rows,
    ),
    // `?g` bound by `BIND`, as an IRI or with `IRI("..")`
    (
        "SELECT ?s { BIND(raphtory:asof:2 AS ?g) GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }",
        &["2"],
        Rows,
    ),
    (
        "SELECT ?s { BIND(IRI(\"raphtory:asof:2\") AS ?g) GRAPH ?g { ?s ?p ?o } }",
        &["2"],
        Rows,
    ),
    (
        "SELECT ?s { BIND(IRI(\"raphtory:asof:2\") AS ?g) OPTIONAL { GRAPH ?g { ?s ?p ?o } } }",
        &["2"],
        Rows,
    ),
    // `GRAPH ?g` inside `OPTIONAL`, `MINUS`, a sub-`SELECT` and `FILTER (NOT) EXISTS`
    (
        "SELECT ?g ?p { VALUES ?g { raphtory:asof:0 raphtory:asof:2 } OPTIONAL { GRAPH ?g { ?s ?p ?o } } }",
        &["0", "2"],
        Rows,
    ),
    (
        "SELECT ?g { VALUES ?g { raphtory:asof:0 raphtory:asof:2 } MINUS { GRAPH ?g { ?s ?p ?o } } }",
        &["0", "2"],
        Rows,
    ),
    (
        "SELECT ?g ?p { VALUES ?g { raphtory:asof:2 } { SELECT ?g ?p { GRAPH ?g { ?s ?p ?o } } } }",
        &["2"],
        Rows,
    ),
    (
        "SELECT ?g { VALUES ?g { raphtory:asof:0 raphtory:asof:2 } FILTER EXISTS { GRAPH ?g { ?s ?p ?o } } }",
        &["0", "2"],
        Rows,
    ),
    (
        "SELECT ?g { VALUES ?g { raphtory:asof:0 raphtory:asof:2 }
            FILTER NOT EXISTS { GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } } }",
        &["0", "2"],
        Rows,
    ),
    (
        "SELECT ?s ?g { ?s ex:q ?o OPTIONAL { GRAPH ?g { ?s ex:p ?x } }
            VALUES ?g { raphtory:asof:2 raphtory:asof:6 } }",
        &["2", "6"],
        Rows,
    ),
    // a series of times, and two spellings of one time
    (
        "SELECT ?g (COUNT(*) AS ?n) {
            VALUES ?g { raphtory:asof:0 raphtory:asof:2 raphtory:asof:4 raphtory:asof:6 }
            GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } }
        } GROUP BY ?g",
        &["0", "2", "4", "6"],
        Rows,
    ),
    (
        "SELECT ?g (COUNT(*) AS ?n) {
            VALUES ?g { raphtory:asof:2 <raphtory:asof:+2> } GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } }
        } GROUP BY ?g",
        &["2", "+2"],
        Rows,
    ),
    // `GRAPH ?g` with `?g` unbound visits the time graphs the query names
    ("SELECT ?g ?s ?p ?o { GRAPH ?g { ?s ?p ?o } }", &[], Empty),
    ("ASK { GRAPH ?g { } }", &[], Empty),
    (
        "SELECT ?g { GRAPH ?g { } VALUES ?g { raphtory:asof:2 raphtory:asof:4 } }",
        &["2", "4"],
        Rows,
    ),
    (
        "SELECT ?g ?s { GRAPH ?g { ?s ?p ?o } FILTER(?g = raphtory:asof:4) }",
        &["4"],
        Rows,
    ),
    (
        "SELECT ?g ?s ?o { GRAPH raphtory:asof:2 { ?s ex:p ?o } GRAPH ?g { ?s ?p ?o } }",
        &["2"],
        Rows,
    ),
    // a time graph built from other values is only reliably matched inside `LATERAL`
    (
        "SELECT ?t ?s ?r { VALUES ?t { 2 4 } BIND(IRI(CONCAT(\"raphtory:asof:\", STR(?t))) AS ?g)
            LATERAL { GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } } }",
        &["2", "4"],
        Rows,
    ),
    (
        "SELECT ?t ?s { VALUES ?t { 2 4 } BIND(IRI(CONCAT(\"raphtory:asof:\", STR(?t))) AS ?g)
            LATERAL { GRAPH ?g { ?s ?p ?o MINUS { ?s ex:p ex:c } } } }",
        &["2", "4"],
        Rows,
    ),
    // `FROM NAMED`, and `CONSTRUCT`
    (
        "SELECT ?g ?s FROM NAMED raphtory:asof:2 FROM NAMED raphtory:asof:4
            { GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }",
        &["2", "4"],
        Rows,
    ),
    (
        "CONSTRUCT { ?s ?p ?o } WHERE {
            VALUES ?g { raphtory:asof:4 } GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }",
        &["4"],
        Rows,
    ),
];

/// An `oxrdf::Dataset` with the view's triples as default graph and `view.snapshot_at(T)` as
/// named graph `<raphtory:asof:T>` for each `T` of `times`.
fn time_graph_dataset(view: &DynamicGraph, times: &[&str]) -> Dataset {
    let triples = |view: &DynamicGraph| {
        let mut doc = Vec::new();
        view.to_rdf(&mut doc, RdfFormat::NTriples).unwrap();
        parse(&String::from_utf8(doc).unwrap(), RdfFormat::NTriples)
    };
    let mut dataset = Dataset::new();
    for triple in triples(view) {
        dataset.insert(triple.as_ref().in_graph(GraphNameRef::DefaultGraph));
    }
    for t in times {
        let iri = NamedNode::new(format!("{ASOF_NS}{t}")).unwrap();
        let at = TimeGraph::parse(&iri).unwrap().unwrap().at;
        for triple in triples(&view.snapshot_at(at).into_dynamic()) {
            dataset.insert(triple.as_ref().in_graph(iri.as_ref()));
        }
    }
    dataset
}

/// Time graphs match the oracle whatever binds `?g` and wherever the `GRAPH` pattern is.
#[test]
fn time_graph_queries_match_the_oracle() {
    let pg = PersistentGraph::new();
    let iri = |local: &str| NamedNode::new(format!("http://ex/{local}")).unwrap();
    let triple = |s: &str, p: &str, o: Term| Triple::new(iri(s), iri(p), o);
    let x: Term = Literal::new_simple_literal("x").into();
    let [ab, ac, ax, bc, ca] = [
        triple("a", "p", iri("b").into()),
        triple("a", "p", iri("c").into()),
        triple("a", "q", x),
        triple("b", "q", iri("c").into()),
        triple("c", "p", iri("a").into()),
    ];
    for t in [&ab, &ac, &ax] {
        pg.add_triple(1, t).unwrap();
    }
    pg.delete_triple(3, &ac).unwrap();
    pg.add_triple(3, &bc).unwrap();
    pg.add_triple(5, &ca).unwrap();
    pg.delete_triple(5, &ab).unwrap();

    for view in [pg.clone().into_dynamic(), pg.event_graph().into_dynamic()] {
        for (query, times, kind) in TIME_GRAPH_QUERIES {
            let query = format!("PREFIX ex: <http://ex/>\n{query}");
            let results = assert_same(&time_graph_dataset(&view, times), &view, &query, false);
            assert_eq!(
                results.is_empty(),
                *kind == Empty,
                "unexpected (non-)empty result for {query}: {results:?}"
            );
        }
    }
}

/// The 8 binding patterns of `s p o`: each position is the constant or a variable.
fn binding_patterns(s: &str, p: &str, o: &str) -> Vec<String> {
    (0..8)
        .map(|mask| {
            let pick = |bit: usize, var: &str, constant: &str| {
                if mask & (1 << bit) != 0 {
                    var.to_owned()
                } else {
                    constant.to_owned()
                }
            };
            format!(
                "SELECT * {{ {} {} {} }}",
                pick(0, "?s", s),
                pick(1, "?p", p),
                pick(2, "?o", o)
            )
        })
        .collect()
}

/// Every binding pattern returns each matching triple once, even with duplicate
/// assertions, several layers on one pair, self-loops and re-assertions.
#[test]
fn binding_patterns_are_duplicate_free() {
    let pg = PersistentGraph::new();
    let iri = NamedNode::new_unchecked;
    let (a, b, p, q) = (
        iri("http://ex/a"),
        iri("http://ex/b"),
        iri("http://ex/p"),
        iri("http://ex/q"),
    );
    for t in [1, 2, 2, 3] {
        pg.add_triple(t, Triple::new(a.clone(), p.clone(), b.clone()).as_ref())
            .unwrap();
        pg.add_triple(t, Triple::new(a.clone(), q.clone(), b.clone()).as_ref())
            .unwrap();
        pg.add_triple(t, Triple::new(a.clone(), p.clone(), a.clone()).as_ref())
            .unwrap();
        pg.add_triple(t, Triple::new(b.clone(), p.clone(), a.clone()).as_ref())
            .unwrap();
    }
    pg.delete_triple(4, Triple::new(a.clone(), q.clone(), b.clone()).as_ref())
        .unwrap();
    pg.add_triple(5, Triple::new(a.clone(), q.clone(), b.clone()).as_ref())
        .unwrap();

    let views: Vec<(&str, DynamicGraph)> = vec![
        ("pg", pg.clone().into_dynamic()),
        ("pg@2", pg.snapshot_at(2).into_dynamic()),
        ("pg[0,6)", pg.window(0, 6).into_dynamic()),
        ("events", pg.event_graph().into_dynamic()),
        ("events[2,3)", pg.event_graph().window(2, 3).into_dynamic()),
    ];
    for (name, view) in views {
        assert_eq!(select(&view, "SELECT * { ?s ?p ?o }").len(), 4, "{name}");
        for (s, p, o) in [
            ("<http://ex/a>", "<http://ex/p>", "<http://ex/b>"),
            ("<http://ex/a>", "<http://ex/q>", "<http://ex/b>"),
            ("<http://ex/a>", "<http://ex/p>", "<http://ex/a>"),
            ("<http://ex/b>", "<http://ex/p>", "<http://ex/a>"),
        ] {
            for query in binding_patterns(s, p, o) {
                let SparqlResults::Solutions { rows, .. } = view.sparql(&query).unwrap() else {
                    panic!("{query}")
                };
                let unique: BTreeSet<_> = rows.iter().map(|r| format!("{r:?}")).collect();
                assert!(!rows.is_empty(), "{name}: {query}");
                assert_eq!(unique.len(), rows.len(), "{name}: {query}: {rows:?}");
            }
        }
    }
}

/// Graphs that were not loaded from RDF.
#[test]
fn non_rdf_graphs() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
    g.add_edge(2, "Alice Smith", "12:30", NO_PROPS, Some("met at"))
        .unwrap();
    g.add_edge(3, "Bob", "http://ex/x", NO_PROPS, Some("http://ex/p"))
        .unwrap();
    assert!(ask(
        &g,
        "ASK { raphtory:Alice raphtory:_default raphtory:Bob }"
    ));
    assert!(ask(
        &g,
        "ASK { <raphtory:Alice%20Smith> <raphtory:met%20at> <raphtory:12%3A30> }"
    ));
    assert!(ask(&g, "ASK { raphtory:Bob <http://ex/p> <http://ex/x> }"));
    // the private layer `_static_graph` is never a predicate
    assert!(!ask(&g, "ASK { ?s <raphtory:_static_graph> ?o }"));
    assert!(!ask(
        &g,
        "ASK { raphtory:Alice <raphtory:_static_graph> raphtory:Bob }"
    ));
    assert_eq!(
        select(&g, "SELECT DISTINCT ?p { ?s ?p ?o }"),
        vec![
            vec!["<http://ex/p>".to_owned()],
            vec!["<raphtory:_default>".to_owned()],
            vec!["<raphtory:met%20at>".to_owned()],
        ]
    );
    // non-canonical spellings of a name do not match it
    assert!(!ask(&g, "ASK { <raphtory:%41lice> ?p ?o }"));

    // a u64-id graph
    let g = Graph::new();
    g.add_edge(1, 42, 43, NO_PROPS, None).unwrap();
    g.add_edge(1, 43, 7, NO_PROPS, Some("next")).unwrap();
    assert!(ask(&g, "ASK { raphtory:42 raphtory:_default raphtory:43 }"));
    assert_eq!(
        select(&g, "SELECT ?p ?o { raphtory:43 ?p ?o }"),
        vec![vec![
            "<raphtory:next>".to_owned(),
            "<raphtory:7>".to_owned()
        ]]
    );
    assert_eq!(
        select(&g, "SELECT ?s { ?s ?p raphtory:43 }"),
        vec![vec!["<raphtory:42>".to_owned()]]
    );
    // on a u64-id graph, `raphtory:abc` must not match the node its hash resolves to
    let hash = calculate_hash("abc");
    let g = Graph::new();
    g.add_edge(1, hash, 43u64, NO_PROPS, None).unwrap();
    // Raphtory itself resolves "abc" to that node
    assert_eq!(g.node("abc").unwrap().name(), hash.to_string());
    assert!(!ask(&g, "ASK { raphtory:abc ?p ?o }"));
    assert!(!ask(
        &g,
        "ASK { raphtory:abc raphtory:_default raphtory:43 }"
    ));
    assert!(!ask(
        &g,
        "ASK { ?s ?p ?o FILTER(sameTerm(?s, raphtory:abc)) }"
    ));
    assert_eq!(
        select(&g, "SELECT ?s { ?s ?p raphtory:43 }"),
        vec![vec![format!("<raphtory:{hash}>")]]
    );
    assert!(ask(
        &g,
        &format!("ASK {{ raphtory:{hash} raphtory:_default raphtory:43 }}")
    ));
    // a node named like a layer is one term in both positions
    let g = Graph::new();
    g.add_edge(1, "a", "knows", NO_PROPS, Some("type")).unwrap();
    g.add_edge(1, "a", "b", NO_PROPS, Some("knows")).unwrap();
    assert_eq!(
        select(
            &g,
            "SELECT ?s ?o { raphtory:a raphtory:type ?p . ?s ?p ?o }"
        ),
        vec![vec!["<raphtory:a>".to_owned(), "<raphtory:b>".to_owned()]]
    );
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// The triples of `CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }` as N-Triples lines.
fn construct_all<G: RdfViewOps>(view: &G) -> BTreeSet<String> {
    let SparqlResults::Graph(triples) = view
        .sparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
        .unwrap()
    else {
        panic!("not a graph")
    };
    let set: BTreeSet<String> = triples.iter().map(|t| format!("{t} .")).collect();
    assert_eq!(set.len(), triples.len(), "duplicate triples");
    set
}

/// The lines of the N-Triples export.
fn exported<G: RdfViewOps>(view: &G) -> BTreeSet<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, RdfFormat::NTriples).unwrap();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Views change what SPARQL sees, and SPARQL sees what `to_rdf` writes.
#[test]
fn views_decide_what_queries_see() {
    let pg = PersistentGraph::new();
    let doc = "<http://ex/a> <http://ex/knows> <http://ex/b> .
<http://ex/a> <http://ex/likes> <http://ex/b> .
<http://ex/b> <http://ex/knows> <http://ex/c> .
<http://ex/c> <http://ex/knows> <http://ex/a> .
<http://ex/c> <http://ex/name> \"C\" .
<http://ex/knows> <http://ex/label> \"knows\" .";
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    pg.retract_rdf(
        5,
        "<http://ex/b> <http://ex/knows> <http://ex/c> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let all = "SELECT ?s ?p ?o { ?s ?p ?o }";
    let row = |s: &str, p: &str, o: &str| vec![s.to_owned(), p.to_owned(), o.to_owned()];
    let (a, b, c) = ("<http://ex/a>", "<http://ex/b>", "<http://ex/c>");
    let (knows, likes) = ("<http://ex/knows>", "<http://ex/likes>");

    assert_eq!(select(&pg, all).len(), 5);
    assert_eq!(select(&pg.snapshot_at(3), all).len(), 6);

    // layers
    let knows_only = pg.layers("http://ex/knows").unwrap();
    assert_eq!(
        select(&knows_only, all),
        sorted(vec![row(c, knows, a), row(a, knows, b)])
    );
    assert!(!ask(&knows_only, "ASK { ?s <http://ex/likes> ?o }"));
    assert!(!ask(
        &knows_only,
        "ASK { <http://ex/a> <http://ex/likes> <http://ex/b> }"
    ));
    // a predicate that is also a node joins in a layer-restricted view
    let knows_label = pg
        .layers(vec!["http://ex/knows", "http://ex/label"])
        .unwrap();
    let by_label = "SELECT ?s ?o { ?p <http://ex/label> \"knows\" . ?s ?p ?o }";
    assert_eq!(
        select(&knows_label, by_label),
        sorted(vec![
            vec![a.to_owned(), b.to_owned()],
            vec![c.to_owned(), a.to_owned()]
        ])
    );
    assert_eq!(
        select(&pg.layers("http://ex/label").unwrap(), by_label),
        vec![] as Vec<Vec<String>>
    );
    // with the label layer hidden the join has nothing to start from
    assert_eq!(
        select(
            &knows_only,
            "SELECT ?l { ?p <http://ex/label> ?l . ?s ?p ?o }"
        ),
        vec![] as Vec<Vec<String>>
    );
    assert_eq!(
        select(
            &pg,
            "SELECT DISTINCT ?l { ?p <http://ex/label> ?l . ?s ?p ?o }"
        ),
        vec![vec!["\"knows\"".to_owned()]]
    );
    let likes_at_3 = pg.snapshot_at(3).valid_layers("http://ex/likes");
    assert_eq!(select(&likes_at_3, all), vec![row(a, likes, b)]);

    // subgraph and exclude_nodes: triples need both ends in the view
    let ab = pg.subgraph(["http://ex/a", "http://ex/b"]);
    assert_eq!(
        select(&ab, all),
        sorted(vec![row(a, knows, b), row(a, likes, b)])
    );
    assert!(!ask(
        &ab,
        "ASK { <http://ex/c> <http://ex/knows> <http://ex/a> }"
    ));
    assert!(!ask(&ab, "ASK { ?s <http://ex/knows> <http://ex/a> }"));
    assert!(!ask(&ab, "ASK { <http://ex/c> ?p ?o }"));
    // the predicate's own node is not in the subgraph, but a bound predicate still matches
    assert!(!ask(&ab, "ASK { <http://ex/knows> ?p ?o }"));
    for query in [
        "SELECT ?s ?o { ?s <http://ex/knows> ?o }",
        "SELECT ?s ?o { BIND(<http://ex/knows> AS ?p) ?s ?p ?o }",
        "SELECT ?s ?o { ?s ?p ?o FILTER(sameTerm(?p, <http://ex/knows>)) }",
    ] {
        assert_eq!(
            select(&ab, query),
            vec![vec![a.to_owned(), b.to_owned()]],
            "{query}"
        );
    }
    let no_b = pg.exclude_nodes(["http://ex/b"]);
    assert_eq!(
        select(&no_b, all),
        sorted(vec![
            row(c, knows, a),
            row(c, "<http://ex/name>", "\"C\""),
            row(knows, "<http://ex/label>", "\"knows\""),
        ])
    );
    assert!(!ask(
        &no_b,
        "ASK { <http://ex/a> <http://ex/knows> <http://ex/b> }"
    ));
    assert!(!ask(&no_b, "ASK { <http://ex/a> ?p <http://ex/b> }"));

    // node filters
    let no_c = pg.filter(NodeFilter::name().ne("\"C\"")).unwrap();
    assert_eq!(
        select(&no_c, all),
        sorted(vec![
            row(c, knows, a),
            row(a, knows, b),
            row(a, likes, b),
            row(knows, "<http://ex/label>", "\"knows\""),
        ])
    );
    assert!(!ask(&no_c, "ASK { <http://ex/c> <http://ex/name> \"C\" }"));
    assert!(!ask(&no_c, "ASK { ?s ?p \"C\" }"));
    assert!(!ask(&no_c, "ASK { <http://ex/c> ?p \"C\" }"));

    // to_rdf writes exactly what CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } returns
    let views: Vec<DynamicGraph> = vec![
        pg.clone().into_dynamic(),
        pg.snapshot_at(3).into_dynamic(),
        pg.snapshot_at(0).into_dynamic(),
        knows_only.into_dynamic(),
        ab.into_dynamic(),
        no_b.into_dynamic(),
        no_c.into_dynamic(),
        pg.event_graph().into_dynamic(),
        pg.event_graph().window(5, 10).into_dynamic(),
    ];
    for view in views {
        assert_eq!(construct_all(&view), exported(&view));
    }
    assert_eq!(construct_all(&pg).len(), 5);
}

/// The triples of a `CONSTRUCT` or `DESCRIBE` query as N-Triples lines.
fn graph_result<G: RdfViewOps>(view: &G, query: &str) -> BTreeSet<String> {
    match view
        .sparql(query)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Graph(triples) => triples.iter().map(|t| format!("{t} .")).collect(),
        other => panic!("{query}: not a graph: {other:?}"),
    }
}

/// `CONSTRUCT` and `DESCRIBE` results have no duplicate triples, including ones with stored blank nodes.
#[test]
fn graph_results_have_no_duplicates() {
    let pg = PersistentGraph::new();
    let doc = "<http://ex/a> <http://ex/p> _:b .
        <http://ex/c> <http://ex/p> _:b .
        _:b <http://ex/q> \"x\" .";
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let triples = |query: &str| match pg.sparql(query).unwrap_or_else(|e| panic!("{query}: {e}")) {
        SparqlResults::Graph(triples) => triples,
        other => panic!("{query}: not a graph: {other:?}"),
    };
    let all = triples("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
    assert_eq!(all.len(), 3);

    for (query, expected) in [
        // 5 solutions with 2 triples each
        ("CONSTRUCT WHERE { ?s ?p ?o . ?x ?y ?o }", all.len()),
        (
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . ?x ?y ?z }",
            all.len(),
        ),
        // `_:b q "x"` once for each of the 3 triples
        (
            "CONSTRUCT { ?s <http://ex/q> ?o } WHERE { ?s <http://ex/q> ?o . ?x ?y ?z }",
            1,
        ),
        ("DESCRIBE ?o WHERE { ?s <http://ex/p> ?o }", 1),
    ] {
        let result = triples(query);
        let distinct: BTreeSet<_> = result.iter().map(ToString::to_string).collect();
        assert_eq!(result.len(), distinct.len(), "{query}: {result:?}");
        assert_eq!(result.len(), expected, "{query}: {result:?}");
        assert!(
            result.iter().all(|t| all.contains(t)),
            "{query}: {result:?}"
        );
    }

    // blank nodes of the template are fresh for every solution, so they are never merged
    let minted =
        triples("CONSTRUCT { _:n <http://ex/r> ?s } WHERE { ?s <http://ex/p> ?o . ?x ?y ?z }");
    assert_eq!(minted.len(), 6, "{minted:?}");
    let subjects: BTreeSet<_> = minted.iter().map(|t| t.subject.to_string()).collect();
    assert_eq!(subjects.len(), 6, "{minted:?}");
}

/// Generalized triples are matched by patterns, dropped by `CONSTRUCT` and end `DESCRIBE` early.
#[test]
fn generalized_triples_in_sparql() {
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(1, "Alice", "Carol", NO_PROPS, Some("_:l"))
        .unwrap();
    g.add_edge(1, "\"lit\"", "Bob", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(1, "Alice", "Dave", NO_PROPS, Some("likes"))
        .unwrap();
    let row = |s: &str, p: &str, o: &str| vec![s.to_owned(), p.to_owned(), o.to_owned()];
    let (alice, bob, carol, dave) = (
        "<raphtory:Alice>",
        "<raphtory:Bob>",
        "<raphtory:Carol>",
        "<raphtory:Dave>",
    );
    let knows = "<raphtory:knows>";

    // SPARQL patterns see them
    assert_eq!(
        select(&g, "SELECT ?s ?p ?o { ?s ?p ?o }"),
        sorted(vec![
            row(alice, knows, bob),
            row(alice, "_:l", carol),
            row("\"lit\"", knows, bob),
            row(alice, "<raphtory:likes>", dave),
        ])
    );
    assert_eq!(select(&g, "SELECT ?p ?o { raphtory:Alice ?p ?o }").len(), 3);
    assert!(ask(&g, "ASK { ?s ?p ?o FILTER(isLiteral(?s)) }"));
    assert!(ask(&g, "ASK { \"lit\" raphtory:knows raphtory:Bob }"));
    assert_eq!(
        select(&g, "SELECT ?s ?o { ?s ?p ?o FILTER(isBlank(?p)) }"),
        vec![vec![alice.to_owned(), carol.to_owned()]]
    );
    assert_eq!(
        select(&g, "SELECT ?s { ?s raphtory:knows raphtory:Bob }"),
        sorted(vec![vec!["\"lit\"".to_owned()], vec![alice.to_owned()]])
    );

    // CONSTRUCT drops them, as export skips them
    assert_eq!(construct_all(&g), exported(&g));
    assert_eq!(construct_all(&g).len(), 2);
    let stats = g.to_rdf(std::io::sink(), RdfFormat::NTriples).unwrap();
    assert_eq!(stats.skipped, 2);

    // DESCRIBE stops, without an error, at the first generalized triple (`_:l`, before `likes`)
    let about_alice = graph_result(
        &g,
        "CONSTRUCT { raphtory:Alice ?p ?o } WHERE { raphtory:Alice ?p ?o }",
    );
    assert_eq!(about_alice.len(), 2, "{about_alice:?}");
    let described = graph_result(&g, "DESCRIBE raphtory:Alice");
    assert!(described.is_subset(&about_alice), "{described:?}");
    assert!(described.len() < about_alice.len(), "{described:?}");
    // a view without generalized triples describes Alice completely
    let rdf_layers = g.layers(vec!["knows", "likes"]).unwrap();
    assert_eq!(
        graph_result(&rdf_layers, "DESCRIBE raphtory:Alice"),
        about_alice
    );
}

/// Internal terms are canonical: a term has one internal value, whatever position it is
/// used in, and unknown terms are accepted.
#[test]
fn internal_terms_are_canonical() {
    let pg = PersistentGraph::new();
    let doc = "<http://ex/a> <http://ex/knows> <http://ex/b> .
<http://ex/knows> <http://ex/label> \"knows\" .";
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let dataset = RaphtoryDataset::new(pg.clone());
    let term = |s: &str| -> Term { NamedNode::new(s).unwrap().into() };
    let knows_node = pg.node("http://ex/knows").unwrap().node;
    let label_layer = pg.get_layer_id("http://ex/label").unwrap();

    let knows = dataset.internalize_term(term("http://ex/knows")).unwrap();
    assert_eq!(knows, RdfTerm::Node(knows_node));
    let label = dataset.internalize_term(term("http://ex/label")).unwrap();
    assert_eq!(label, RdfTerm::Layer(label_layer));
    let unknown = dataset.internalize_term(term("http://ex/zzz")).unwrap();
    assert_eq!(unknown, RdfTerm::Other(term("http://ex/zzz")));
    let literal: Term = Literal::new_simple_literal("knows").into();
    assert!(matches!(
        dataset.internalize_term(literal.clone()).unwrap(),
        RdfTerm::Node(_)
    ));
    let other_literal: Term = Literal::new_simple_literal("nope").into();
    assert_eq!(
        dataset.internalize_term(other_literal.clone()).unwrap(),
        RdfTerm::Other(other_literal)
    );
    // `_static_graph` (layer 0) is private: it is not a layer of the dataset
    assert_eq!(
        dataset
            .internalize_term(term("raphtory:_static_graph"))
            .unwrap(),
        RdfTerm::Other(term("raphtory:_static_graph"))
    );
    // a non-canonical spelling of a name is another term
    assert!(matches!(
        dataset
            .internalize_term(NamedNode::new_unchecked("raphtory:%41").into())
            .unwrap(),
        RdfTerm::Other(_)
    ));

    // the quad of `a knows b` uses the node for the predicate
    let quads: Vec<_> = dataset
        .internal_quads_for_pattern(None, Some(&knows), None, Some(None))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(quads.len(), 1);
    assert_eq!(quads[0].predicate, knows);
    assert_eq!(quads[0].graph_name, None);
    let quads: Vec<_> = dataset
        .internal_quads_for_pattern(Some(&knows), None, None, Some(None))
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(quads.len(), 1);
    assert_eq!(quads[0].predicate, label);
    // named graphs: none stored, none listed
    for graph in [Some(Some(&knows)), Some(Some(&unknown)), None] {
        assert_eq!(
            dataset
                .internal_quads_for_pattern(None, None, None, graph)
                .count(),
            0
        );
    }
    assert_eq!(dataset.internal_named_graphs().count(), 0);
    assert!(!dataset.contains_internal_graph_name(&unknown).unwrap());
    // a bound subject or object that is not a node, or a predicate that is not a layer,
    // matches nothing
    for (s, p, o) in [
        (Some(&unknown), None, None),
        (Some(&label), None, None),
        (None, None, Some(&unknown)),
        (None, Some(&unknown), None),
        (
            None,
            Some(&RdfTerm::Node(pg.node("http://ex/a").unwrap().node)),
            None,
        ),
    ] {
        assert_eq!(
            dataset
                .internal_quads_for_pattern(s, p, o, Some(None))
                .count(),
            0
        );
    }

    // externalize is the inverse of internalize
    for t in [
        term("http://ex/knows"),
        term("http://ex/label"),
        term("http://ex/zzz"),
        literal,
    ] {
        let internal = dataset.internalize_term(t.clone()).unwrap();
        assert_eq!(dataset.externalize_term(internal).unwrap(), t);
    }

    // a node created after the dataset was built: the memo keeps the first answer
    pg.add_edge(2, "http://ex/zzz", "http://ex/a", NO_PROPS, None)
        .unwrap();
    assert_eq!(
        dataset.internalize_term(term("http://ex/zzz")).unwrap(),
        RdfTerm::Other(term("http://ex/zzz"))
    );
    // and its layer (`_default`) is not in the dataset, so its triple is skipped
    assert_eq!(
        dataset
            .internal_quads_for_pattern(None, None, None, Some(None))
            .count(),
        2
    );
    assert_eq!(select(&pg, "SELECT * { ?s ?p ?o }").len(), 3);
}

/// A node named like a layer, created after the dataset was built, does not hide the layer's triples.
#[test]
fn predicate_terms_are_fixed_when_the_dataset_is_built() {
    let pg = PersistentGraph::new();
    pg.load_rdf(
        1,
        "<http://ex/a> <http://ex/knows> <http://ex/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let knows_layer = pg.get_layer_id("http://ex/knows").unwrap();
    let queries = [
        "SELECT ?s ?o { ?s <http://ex/knows> ?o }",
        "SELECT ?s ?o { ?s ?p ?o FILTER(sameTerm(?p, <http://ex/knows>)) }",
        "SELECT ?s ?o { BIND(<http://ex/knows> AS ?p) ?s ?p ?o }",
        "SELECT ?s ?o { VALUES ?p { <http://ex/knows> } ?s ?p ?o }",
    ];
    let datasets: Vec<_> = queries
        .iter()
        .map(|_| RaphtoryDataset::new(pg.clone()))
        .collect();
    let dataset = RaphtoryDataset::new(pg.clone());
    // a node named like the layer (as RDF ingestion of `ex:knows rdfs:label "knows"` creates)
    pg.load_rdf(
        2,
        "<http://ex/x> <http://ex/mentions> <http://ex/knows> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let knows_node = pg.node("http://ex/knows").unwrap().node;

    let (a, b): (Term, Term) = (
        NamedNode::new("http://ex/a").unwrap().into(),
        NamedNode::new("http://ex/b").unwrap().into(),
    );
    for (query, dataset) in queries.into_iter().zip(datasets) {
        let results = evaluator()
            .parse_query(query)
            .unwrap()
            .on_queryable_dataset(dataset)
            .execute()
            .unwrap();
        let SparqlResults::Solutions { rows, .. } =
            SparqlResults::from_query_results(results).unwrap()
        else {
            panic!("{query}: not a SELECT result")
        };
        assert_eq!(
            rows,
            vec![vec![Some(a.clone()), Some(b.clone())]],
            "{query}"
        );
    }

    // the constant is the predicate term fixed when `dataset` was built, not the new node
    let knows: Term = NamedNode::new("http://ex/knows").unwrap().into();
    assert_eq!(
        dataset.internalize_term(knows.clone()).unwrap(),
        RdfTerm::Layer(knows_layer)
    );
    assert_eq!(
        dataset
            .externalize_term(RdfTerm::Layer(knows_layer))
            .unwrap(),
        knows
    );

    // a dataset built now has the node as the predicate term, and the node joins
    let dataset = RaphtoryDataset::new(pg.clone());
    assert_eq!(
        dataset.internalize_term(knows).unwrap(),
        RdfTerm::Node(knows_node)
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?x ?s ?o { ?x <http://ex/mentions> ?p . ?s ?p ?o }"
        ),
        vec![vec![
            "<http://ex/x>".to_owned(),
            "<http://ex/a>".to_owned(),
            "<http://ex/b>".to_owned()
        ]]
    );
}

/// A writer thread adds and deletes edges (creating nodes and a layer) while queries
/// run; nothing deadlocks.
#[test]
fn queries_run_while_the_graph_is_written() {
    const N: usize = 2000;
    let node = |i: usize| format!("http://ex/n{}", i % N);
    let pg = PersistentGraph::new();
    for i in 0..N {
        pg.add_edge(0, node(i), node(i + 1), NO_PROPS, Some("http://ex/p"))
            .unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(AtomicUsize::new(0));
    let writer = {
        let pg = pg.clone();
        let stop = stop.clone();
        let progress = progress.clone();
        std::thread::spawn(move || {
            // bounded so that a slow machine does not grow the graph without limit
            for i in 0..5_000_000usize {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let t = i as i64 + 1;
                // the layer `q` is created while the first queries run
                let layer = if (i / N).is_multiple_of(2) {
                    "http://ex/p"
                } else {
                    "http://ex/q"
                };
                pg.add_edge(t, node(i), node(i + 1), NO_PROPS, Some(layer))
                    .unwrap();
                if i.is_multiple_of(3) {
                    pg.delete_edge(t, node(i + 7), node(i + 8), Some("http://ex/p"))
                        .unwrap();
                }
                if i.is_multiple_of(5) {
                    // new nodes (and node segments), a bounded number of them
                    let new = format!("http://ex/new{}", i % (3 * N));
                    pg.add_edge(t, new, node(i), NO_PROPS, Some("http://ex/p"))
                        .unwrap();
                }
                progress.store(i + 1, Ordering::Relaxed);
            }
        })
    };
    let results = within_60s({
        let pg = pg.clone();
        let progress = progress.clone();
        move || {
            (0..50)
                .map(|_| {
                    let before = progress.load(Ordering::Relaxed);
                    let SparqlResults::Solutions { rows, .. } =
                        pg.sparql("SELECT * { ?a ?p ?b . ?b ?q ?c }").unwrap()
                    else {
                        panic!("not a SELECT result")
                    };
                    let written_meanwhile = progress.load(Ordering::Relaxed) > before;
                    (rows.len(), written_meanwhile)
                })
                .collect::<Vec<_>>()
        }
    });
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(results.len(), 50);
    assert!(results.iter().all(|(rows, _)| *rows > 0), "{results:?}");
    // the writer really did write while queries ran
    assert!(results.iter().any(|(_, written)| *written), "{results:?}");
}

/// Predicate-only scans run while a writer adds edges to the same layer; nothing deadlocks.
#[test]
fn predicate_patterns_run_while_the_layer_is_written() {
    const N: usize = 2000;
    let node = |i: usize| format!("http://ex/n{}", i % N);
    let pg = PersistentGraph::new();
    for i in 0..N {
        pg.add_edge(0, node(i), node(i + 1), NO_PROPS, Some("http://ex/p"))
            .unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(AtomicUsize::new(0));
    let writer = {
        let pg = pg.clone();
        let stop = stop.clone();
        let progress = progress.clone();
        std::thread::spawn(move || {
            for i in 0..5_000_000usize {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let t = i as i64 + 1;
                pg.add_edge(t, node(i), node(i + 2), NO_PROPS, Some("http://ex/p"))
                    .unwrap();
                if i.is_multiple_of(3) {
                    pg.delete_edge(t, node(i), node(i + 1), Some("http://ex/p"))
                        .unwrap();
                }
                if i.is_multiple_of(5) {
                    let new = format!("http://ex/new{}", i % (3 * N));
                    pg.add_edge(t, node(i), new, NO_PROPS, Some("http://ex/p"))
                        .unwrap();
                }
                progress.store(i + 1, Ordering::Relaxed);
            }
        })
    };
    let results = within_60s({
        let pg = pg.clone();
        let progress = progress.clone();
        move || {
            (0..50)
                .map(|i| {
                    let before = progress.load(Ordering::Relaxed);
                    let query = if i % 2 == 0 {
                        "SELECT * { ?s <http://ex/p> ?o }".to_owned()
                    } else {
                        format!("SELECT * {{ GRAPH <raphtory:asof:{before}> {{ ?s <http://ex/p> ?o }} }}")
                    };
                    let rows = select(&pg, &query).len();
                    (rows, progress.load(Ordering::Relaxed) > before)
                })
                .collect::<Vec<_>>()
        }
    });
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(results.len(), 50);
    assert!(results.iter().all(|(rows, _)| *rows > 0), "{results:?}");
    // the writer really did write while queries ran
    assert!(results.iter().any(|(_, written)| *written), "{results:?}");
}

/// Datasets are built and queried while a writer creates layers; nothing deadlocks.
#[test]
fn datasets_are_built_while_layers_are_created() {
    const LAYERS: usize = 3000;
    let pg = PersistentGraph::new();
    for i in 0..LAYERS {
        let layer = format!("http://ex/l{i}");
        pg.add_edge(0, "http://ex/a", "http://ex/b", NO_PROPS, Some(&layer))
            .unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let created = Arc::new(AtomicUsize::new(0));
    let writer = {
        let pg = pg.clone();
        let stop = stop.clone();
        let created = created.clone();
        std::thread::spawn(move || {
            for i in 0..100_000usize {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let layer = format!("http://ex/new{i}");
                pg.add_edge(1, "http://ex/a", "http://ex/c", NO_PROPS, Some(&layer))
                    .unwrap();
                created.store(i + 1, Ordering::Relaxed);
            }
        })
    };
    let results = within_60s({
        let pg = pg.clone();
        let created = created.clone();
        move || {
            (0..50)
                .map(|_| {
                    let before = created.load(Ordering::Relaxed);
                    drop(RaphtoryDataset::new(pg.clone()));
                    let found = ask(&pg, "ASK { <http://ex/a> <http://ex/l1> <http://ex/b> }");
                    (found, created.load(Ordering::Relaxed) > before)
                })
                .collect::<Vec<_>>()
        }
    });
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(results.iter().all(|(found, _)| *found), "{results:?}");
    // layers really were created while datasets were built
    assert!(results.iter().any(|(_, created)| *created), "{results:?}");
}

/// A query on a `read_only()` view sees the locked graph, even for names a waiting writer has
/// already resolved.
#[test]
fn read_only_view_is_queried_while_a_writer_waits() {
    let pg = PersistentGraph::new();
    pg.load_rdf(
        1,
        "<http://e/a> <http://e/p> <http://e/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let handle = pg.read_only();
    let writer = {
        let pg = pg.clone();
        std::thread::spawn(move || {
            let doc = "<http://e/c> <http://e/p> <http://e/d> .
                       <http://e/a> <http://e/q> <http://e/b> .";
            pg.load_rdf(2, doc.as_bytes(), RdfFormat::NTriples, None)
                .unwrap();
        })
    };
    // give the writer time to resolve the new names and block on the locked storage
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(!writer.is_finished(), "the writer did not wait");
    let (new_subject, new_object, new_predicate, all, layer, layer_asof) = within_60s(move || {
        let results = (
            ask(&handle, "ASK { <http://e/c> ?p ?o }"),
            ask(&handle, "ASK { ?s ?p <http://e/d> }"),
            ask(&handle, "ASK { ?s <http://e/q> ?o }"),
            select(&handle, "SELECT ?s ?p ?o { ?s ?p ?o }"),
            // read from the edges of the layer in the locked storage
            select(&handle, "SELECT ?s ?o { ?s <http://e/p> ?o }"),
            select(
                &handle,
                "SELECT ?s ?o { GRAPH <raphtory:asof:5> { ?s <http://e/p> ?o } }",
            ),
        );
        drop(handle);
        results
    });
    assert!(!new_subject && !new_object && !new_predicate);
    assert_eq!(
        all,
        vec![vec!["<http://e/a>", "<http://e/p>", "<http://e/b>"]]
    );
    assert_eq!(layer, vec![vec!["<http://e/a>", "<http://e/b>"]]);
    assert_eq!(layer_asof, layer);
    // the writer goes ahead once the view is dropped
    writer.join().unwrap();
    assert!(ask(&pg, "ASK { <http://e/c> <http://e/p> <http://e/d> }"));
    assert!(ask(&pg, "ASK { <http://e/a> <http://e/q> <http://e/b> }"));
}

/// Result shapes and errors.
#[test]
fn result_shapes_and_errors() {
    let pg = PersistentGraph::new();
    pg.load_rdf(1, SOCIAL.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let query = |q: &str| pg.sparql(&format!("{PREFIXES}{q}"));

    let SparqlResults::Solutions { variables, rows } =
        query("SELECT ?n ?x { ?x foaf:age 25 OPTIONAL { ?x foaf:nope ?n } }").unwrap()
    else {
        panic!("not a SELECT result")
    };
    assert_eq!(
        variables,
        vec![Variable::new("n").unwrap(), Variable::new("x").unwrap()]
    );
    assert_eq!(
        rows,
        vec![vec![
            None,
            Some(NamedNode::new("http://ex/bob").unwrap().into())
        ]]
    );

    assert_eq!(
        query("ASK { ex:alice ex:knows ex:bob }").unwrap(),
        SparqlResults::Boolean(true)
    );
    assert_eq!(
        query("ASK { ex:bob ex:knows ex:alice }").unwrap(),
        SparqlResults::Boolean(false)
    );

    let triple = |s: &str, p: &str, o: &str| {
        Triple::new(
            NamedNode::new(s).unwrap(),
            NamedNode::new(p).unwrap(),
            NamedNode::new(o).unwrap(),
        )
    };
    assert_eq!(
        query(
            "CONSTRUCT { ?o ex:knownBy ?s } WHERE { ex:alice ex:knows ?o . BIND(ex:alice AS ?s) }"
        )
        .unwrap(),
        SparqlResults::Graph(vec![triple(
            "http://ex/bob",
            "http://ex/knownBy",
            "http://ex/alice"
        )])
    );
    let SparqlResults::Graph(described) = query("DESCRIBE ex:bob").unwrap() else {
        panic!("not a graph")
    };
    assert_eq!(described.len(), 4, "{described:?}");
    assert!(described.contains(&triple(
        "http://ex/bob",
        "http://ex/knows",
        "http://ex/carol"
    )));

    // results can be sent to another thread
    fn assert_send<T: Send + Sync>(_: &T) {}
    let results = query("SELECT * { ?s ?p ?o }").unwrap();
    assert_send(&results);
    let rows = std::thread::spawn(move || match results {
        SparqlResults::Solutions { rows, .. } => rows.len(),
        _ => 0,
    })
    .join()
    .unwrap();
    assert!(rows > 30);

    let err = pg.sparql("SELECT * WHERE { ?s ?p }").unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::SparqlSyntax(_))),
        "{err:?}"
    );
    // undefined prefix
    let err = pg.sparql("SELECT * { nope:a ?p ?o }").unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::SparqlSyntax(_))),
        "{err:?}"
    );
    // there is no SERVICE handler
    let err = pg
        .sparql("SELECT * { SERVICE <http://ex/endpoint> { ?s ?p ?o } }")
        .unwrap_err();
    assert!(
        matches!(err, GraphError::Rdf(RdfError::SparqlEvaluation(_))),
        "{err:?}"
    );

    // the `raphtory:` prefix is registered but can be overridden
    let g = Graph::new();
    g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
    assert_eq!(
        g.sparql("ASK { raphtory:Alice ?p raphtory:Bob }").unwrap(),
        SparqlResults::Boolean(true)
    );
    assert_eq!(
        g.sparql("PREFIX raphtory: <http://ex/> ASK { raphtory:Alice ?p raphtory:Bob }")
            .unwrap(),
        SparqlResults::Boolean(false)
    );
    // an empty graph
    assert_eq!(
        Graph::new().sparql("ASK { ?s ?p ?o }").unwrap(),
        SparqlResults::Boolean(false)
    );
}
