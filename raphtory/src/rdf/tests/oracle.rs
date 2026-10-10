//! SPARQL over graph views.
//!
//! The differential tests evaluate each query twice with the same evaluator: on the graph, and
//! on an `oxrdf::Dataset` holding the same triples. The `Dataset` keeps terms exactly as
//! written (an oxigraph `Store` would normalise literals such as `"042"^^xsd:integer`).
use super::{ask, select, within_60s};
use crate::{
    db::api::view::{internal::CoreGraphOps, IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::{
        dataset::{RaphtoryDataset, RdfTerm},
        model::{Dataset, GraphNameRef, Literal, NamedNode, Term, Triple},
        query::evaluator,
        RdfFormat, SparqlResults,
    },
};
use oxigraph::io::RdfParser;
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
    /// Empty (or `false`) on the union of the fixtures.
    Empty,
}

use Kind::{Empty, Rows};

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
    // ... with literals, blank nodes and terms that are not in the data
    ("SELECT ?s { ?s foaf:age 30 }", Rows),
    ("SELECT ?s ?p { ?s ?p \"Alice\" }", Rows),
    ("SELECT ?s ?p { ?s ?p \"Alice\"@en }", Rows),
    ("SELECT ?s { ?s foaf:name \"Carol\"@en-gb }", Rows),
    ("SELECT ?s { ?s foaf:name \"Carol\"@EN-GB }", Rows),
    ("SELECT ?s ?p { ?s ?p \"042\"^^xsd:integer }", Rows),
    ("SELECT ?s ?p { ?s ?p 42 }", Rows),
    ("SELECT ?s ?p { ?s ?p \"typed\"^^xsd:string }", Rows),
    ("SELECT ?s ?p { ?s ?p \"nope\" }", Empty),
    ("SELECT * { ex:nobody ?p ?o }", Empty),
    ("SELECT * { ?s ex:nothing ?o }", Empty),
    ("SELECT * { ?s ?p ex:nobody }", Empty),
    (
        "SELECT ?s ?p ?o { ?s ?p ?o FILTER(isBlank(?s) || isBlank(?o)) }",
        Rows,
    ),
    ("SELECT ?p ?o { ?b ex:city \"Paris\" . ?b ?p ?o }", Rows),
    // joins
    ("SELECT ?a ?b ?c { ?a ex:knows ?b . ?b ex:knows ?c }", Rows),
    ("SELECT ?p ?l ?s ?o { ?p rdfs:label ?l . ?s ?p ?o }", Rows),
    // OPTIONAL
    (
        "SELECT ?x ?age { ?x foaf:name ?n OPTIONAL { ?x foaf:age ?age } }",
        Rows,
    ),
    // property paths
    ("SELECT ?x { ex:alice ex:knows* ?x }", Rows),
    ("SELECT ?x ?y { ?x ex:knows+ ?y }", Rows),
    ("SELECT ?x ?y { ?x !ex:knows ?y }", Rows),
    ("SELECT ?x { ?x !^ex:knows ex:bob }", Rows),
    // a zero-length path only matches terms of the data
    ("ASK { ex:nobody ex:knows* ex:nobody }", Empty),
    // VALUES and BIND, with terms that are not in the data
    (
        "SELECT ?x ?y { VALUES ?x { ex:alice ex:nobody \"lit\" 42 ex:knows } OPTIONAL { ?x ex:knows ?y } }",
        Rows,
    ),
    (
        "SELECT ?v ?x { VALUES ?v { 30 \"30\" 30.0 41 } ?x foaf:age ?v }",
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
        "SELECT ?s ?p { ?s ?p ?o FILTER(sameTerm(?p, ex:knows)) }",
        Rows,
    ),
    // ASK, CONSTRUCT and DESCRIBE
    ("ASK { ex:alice ex:knows ex:bob }", Rows),
    ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", Rows),
    ("DESCRIBE ex:alice", Rows),
    // repeated variables, predicates as subjects, named graphs
    ("SELECT ?s ?p { ?s ?p ?s }", Rows),
    ("SELECT ?s ?o { ?s ?s ?o }", Empty),
    ("SELECT DISTINCT ?p ?q ?v { ?s ?p ?o . ?p ?q ?v }", Rows),
    ("SELECT ?l { ex:knows rdfs:label ?l }", Rows),
    ("SELECT ?s { ?s ex:mentions ?p . ?a ?p ?b }", Rows),
    ("SELECT * { GRAPH ?g { ?s ?p ?o } }", Empty),
    ("SELECT * FROM ex:g { ?s ?p ?o }", Empty),
];

/// Parses a document keeping blank-node labels.
pub(super) fn parse(doc: &str, format: RdfFormat) -> Vec<Triple> {
    RdfParser::from_format(format)
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap()))
        .collect()
}

/// The same triples in an `oxrdf::Dataset` and (asserted at time 0) in an `RdfGraph`.
fn load(triples: &[Triple]) -> (Dataset, RdfGraph) {
    let mut dataset = Dataset::new();
    let rg = RdfGraph::new();
    for triple in triples {
        dataset.insert(triple.as_ref().in_graph(GraphNameRef::DefaultGraph));
        rg.add_triple(0, triple).unwrap();
    }
    (dataset, rg)
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

/// Results in a comparable form: sorted rows, a boolean, or a set of triples, all in
/// N-Triples form.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Normalized {
    Rows(Vec<String>, Vec<Vec<String>>),
    Boolean(bool),
    Triples(BTreeSet<String>),
}

impl Normalized {
    fn new(results: &SparqlResults) -> Self {
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
                rows.sort();
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
pub(super) fn assert_same<G: StaticGraphViewOps + IntoDynamic>(
    dataset: &Dataset,
    g: &RdfView<G>,
    query: &str,
) -> Normalized {
    let expected = Normalized::new(&oracle(dataset, query));
    let actual = Normalized::new(&g.sparql(query).unwrap_or_else(|e| panic!("{query}: {e}")));
    assert_eq!(actual, expected, "{query}");
    actual
}

/// The differential harness over the union of the fixtures.
#[test]
fn queries_match_the_oracle() {
    let triples = [
        parse(FOAF, RdfFormat::Turtle),
        parse(DATATYPES, RdfFormat::NTriples),
        parse(BLANK_NODES, RdfFormat::Turtle),
        parse(SOCIAL, RdfFormat::Turtle),
    ]
    .concat();
    let (dataset, pg) = load(&triples);
    // duplicate triples are one quad in the dataset and one SPARQL row in the graph
    assert!(dataset.len() < triples.len());
    for (query, kind) in QUERIES {
        let results = assert_same(&dataset, &pg, &format!("{PREFIXES}{query}"));
        assert_eq!(
            results.is_empty(),
            *kind == Empty,
            "unexpected (non-)empty result for {query}: {results:?}"
        );
    }
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// A predicate that is also a node joins under a layer view, and a bound predicate matches in a
/// subgraph without the predicate's node.
#[test]
fn views_decide_what_queries_see() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/knows> <http://ex/b> .
<http://ex/a> <http://ex/likes> <http://ex/b> .
<http://ex/b> <http://ex/knows> <http://ex/c> .
<http://ex/c> <http://ex/knows> <http://ex/a> .
<http://ex/c> <http://ex/name> \"C\" .
<http://ex/knows> <http://ex/label> \"knows\" .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    rg.retract_rdf(
        5,
        "<http://ex/b> <http://ex/knows> <http://ex/c> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let (a, b, c) = ("<http://ex/a>", "<http://ex/b>", "<http://ex/c>");

    // a predicate that is also a node joins in a layer-restricted view
    let knows_label = rg
        .valid()
        .layers(vec!["http%3A%2F%2Fex%2Fknows", "http%3A%2F%2Fex%2Flabel"])
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
        select(&rg.layers("http%3A%2F%2Fex%2Flabel").unwrap(), by_label),
        vec![] as Vec<Vec<String>>
    );
    // with the label layer hidden the join has nothing to start from
    let knows_only = rg.layers("http%3A%2F%2Fex%2Fknows").unwrap();
    assert_eq!(
        select(
            &knows_only,
            "SELECT ?l { ?p <http://ex/label> ?l . ?s ?p ?o }"
        ),
        vec![] as Vec<Vec<String>>
    );
    assert_eq!(
        select(
            &rg,
            "SELECT DISTINCT ?l { ?p <http://ex/label> ?l . ?s ?p ?o }"
        ),
        vec![vec!["\"knows\"".to_owned()]]
    );
    let ab = rg.subgraph(["http://ex/a", "http://ex/b"]);
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
}

/// Internal terms are canonical: a term has one internal value, whatever position it is
/// used in, and unknown terms are accepted.
#[test]
fn internal_terms_are_canonical() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/knows> <http://ex/b> .
<http://ex/knows> <http://ex/label> \"knows\" .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let dataset = RaphtoryDataset::new(rg.graph().clone());
    let term = |s: &str| -> Term { NamedNode::new(s).unwrap().into() };
    let knows_node = rg.graph().node("http://ex/knows").unwrap().node;
    let label_layer = rg.graph().get_layer_id("http%3A%2F%2Fex%2Flabel").unwrap();

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
            Some(&RdfTerm::Node(rg.graph().node("http://ex/a").unwrap().node)),
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
    rg.graph()
        .add_edge(2, "http://ex/zzz", "http://ex/a", NO_PROPS, None)
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
    assert_eq!(select(&rg, "SELECT * { ?s ?p ?o }").len(), 3);

    // the predicate terms are fixed when the dataset is built: a node named like a layer,
    // created later, does not hide the layer's triples
    let rg = RdfGraph::new();
    rg.load_rdf(
        1,
        "<http://ex/a> <http://ex/knows> <http://ex/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let knows_layer = rg.graph().get_layer_id("http%3A%2F%2Fex%2Fknows").unwrap();
    let (dataset, queried) = (
        RaphtoryDataset::new(rg.graph().clone()),
        RaphtoryDataset::new(rg.graph().clone()),
    );
    rg.load_rdf(
        2,
        "<http://ex/x> <http://ex/mentions> <http://ex/knows> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let knows_node = rg.graph().node("http://ex/knows").unwrap().node;
    let query = "SELECT ?s ?o { ?s <http://ex/knows> ?o }";
    let results = evaluator()
        .parse_query(query)
        .unwrap()
        .on_queryable_dataset(queried)
        .execute()
        .unwrap();
    let SparqlResults::Solutions { rows, .. } = SparqlResults::from_query_results(results).unwrap()
    else {
        panic!("{query}: not a SELECT result")
    };
    assert_eq!(
        rows,
        vec![vec![Some(term("http://ex/a")), Some(term("http://ex/b"))]]
    );
    let knows = term("http://ex/knows");
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
    assert_eq!(
        RaphtoryDataset::new(rg.graph().clone())
            .internalize_term(knows)
            .unwrap(),
        RdfTerm::Node(knows_node)
    );
    assert_eq!(
        select(
            &rg,
            "SELECT ?x ?s ?o { ?x <http://ex/mentions> ?p . ?s ?p ?o }"
        ),
        vec![vec![
            "<http://ex/x>".to_owned(),
            "<http://ex/a>".to_owned(),
            "<http://ex/b>".to_owned()
        ]]
    );

    // on a u64-id graph, `raphtory:abc` must not match the node its hash resolves to
    let hash = calculate_hash("abc");
    let g = Graph::new();
    g.add_edge(1, hash, 43u64, NO_PROPS, None).unwrap();
    // Raphtory itself resolves "abc" to that node
    assert_eq!(g.node("abc").unwrap().name(), hash.to_string());
    let g = RdfGraph::from(g);
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
}

/// A writer thread adds and deletes edges, creating nodes and layers, while datasets are built
/// and queried; nothing deadlocks.
#[test]
fn queries_run_while_the_graph_is_written() {
    const N: usize = 2000;
    const P: &str = "http%3A%2F%2Fex%2Fp";
    let node = |i: usize| format!("http://ex/n{}", i % N);
    let pg = PersistentGraph::new();
    for i in 0..N {
        pg.add_edge(0, node(i), node(i + 1), NO_PROPS, Some(P))
            .unwrap();
    }
    for i in 0..3000 {
        let layer = format!("http%3A%2F%2Fex%2Fl{i}");
        pg.add_edge(0, "http://ex/a", "http://ex/b", NO_PROPS, Some(&layer))
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
                pg.add_edge(t, node(i), node(i + 1), NO_PROPS, Some(P))
                    .unwrap();
                if i.is_multiple_of(3) {
                    pg.delete_edge(t, node(i + 7), node(i + 8), Some(P))
                        .unwrap();
                }
                if i.is_multiple_of(5) {
                    // new nodes (and node segments), a bounded number of them
                    let new = format!("http://ex/new{}", i % (3 * N));
                    pg.add_edge(t, new, node(i), NO_PROPS, Some(P)).unwrap();
                }
                if i < 100_000 {
                    let layer = format!("http%3A%2F%2Fex%2Fnew{i}");
                    pg.add_edge(t, "http://ex/a", "http://ex/c", NO_PROPS, Some(&layer))
                        .unwrap();
                }
                progress.store(i + 1, Ordering::Relaxed);
            }
        })
    };
    let rg = RdfGraph::from(pg);
    let results = within_60s({
        let rg = rg.clone();
        let progress = progress.clone();
        move || {
            (0..60)
                .map(|i| {
                    let before = progress.load(Ordering::Relaxed);
                    let query = match i % 3 {
                        0 => "SELECT * { ?a ?p ?b . ?b ?q ?c }".to_owned(),
                        // read from the edges of the layer
                        1 => "SELECT * { ?s <http://ex/p> ?o }".to_owned(),
                        _ => format!(
                            "SELECT * {{ GRAPH <raphtory:asof:{before}> {{ ?s <http://ex/p> ?o }} }}"
                        ),
                    };
                    let rows = select(&rg, &query).len();
                    (rows, progress.load(Ordering::Relaxed) > before)
                })
                .collect::<Vec<_>>()
        }
    });
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert_eq!(results.len(), 60);
    assert!(results.iter().all(|(rows, _)| *rows > 0), "{results:?}");
    // the writer really did write while queries ran
    assert!(results.iter().any(|(_, written)| *written), "{results:?}");
    assert!(ask(
        &rg,
        "ASK { <http://ex/a> <http://ex/l1> <http://ex/b> }"
    ));
}

/// A query on a `read_only()` view sees the locked graph, even for names a waiting writer has
/// already resolved.
#[test]
fn read_only_view_is_queried_while_a_writer_waits() {
    let rg = RdfGraph::new();
    rg.load_rdf(
        1,
        "<http://e/a> <http://e/p> <http://e/b> .".as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let handle = RdfGraph::from(rg.graph().read_only());
    let writer = {
        let rg = rg.clone();
        std::thread::spawn(move || {
            let doc = "<http://e/c> <http://e/p> <http://e/d> .
                       <http://e/a> <http://e/q> <http://e/b> .";
            rg.load_rdf(2, doc.as_bytes(), RdfFormat::NTriples, None)
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
    assert!(ask(&rg, "ASK { <http://e/c> <http://e/p> <http://e/d> }"));
    assert!(ask(&rg, "ASK { <http://e/a> <http://e/q> <http://e/b> }"));
}
