//! SPARQL over graph views, checked against an `oxrdf::Dataset` of the same triples, which keeps
//! terms as written (an oxigraph `Store` would normalise literals such as `"042"^^xsd:integer`).
use super::{ask, select, sorted_rows, BLANK_NODES, DATATYPES, FOAF, SOCIAL};
use crate::{
    dataset::{NodeTerm, RaphtoryDataset, RdfTerm},
    query::{evaluator, parse as parse_sparql},
    Literal, NamedNode, RdfFormat, RdfGraph, SparqlOptions, SparqlResults, Term, Triple,
};
use oxigraph::{
    io::RdfParser,
    model::{Dataset, GraphNameRef},
};
use raphtory::{db::api::view::IntoDynamic, prelude::*};
use spareval::QueryableDataset;
use std::collections::BTreeSet;
use Kind::{Empty, Rows};

const PREFIXES: &str = "PREFIX ex: <http://ex/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Rows,
    Empty,
}

const QUERIES: &[(&str, Kind)] = &[
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
    ("SELECT ?a ?b ?c { ?a ex:knows ?b . ?b ex:knows ?c }", Rows),
    ("SELECT ?p ?l ?s ?o { ?p rdfs:label ?l . ?s ?p ?o }", Rows),
    (
        "SELECT ?x ?age { ?x foaf:name ?n OPTIONAL { ?x foaf:age ?age } }",
        Rows,
    ),
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

fn parse(doc: &str, format: RdfFormat) -> Vec<Triple> {
    RdfParser::from_format(format.to_oxigraph())
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap()))
        .collect()
}

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
        .for_query(parse_sparql(query).unwrap_or_else(|e| panic!("{query}: {e}")))
        .on_queryable_dataset(dataset)
        .execute()
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    SparqlResults::from_query_results(results).unwrap_or_else(|e| panic!("{query}: {e}"))
}

#[derive(Debug, PartialEq, Eq)]
enum Normalized {
    Rows(Vec<String>, Vec<Vec<String>>),
    Boolean(bool),
    Triples(BTreeSet<String>),
}

impl Normalized {
    fn new(results: &SparqlResults) -> Self {
        match results {
            SparqlResults::Solutions { variables, rows } => Self::Rows(
                variables.iter().map(|v| v.to_string()).collect(),
                sorted_rows(rows),
            ),
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
        let query = format!("{PREFIXES}{query}");
        let expected = Normalized::new(&oracle(&dataset, &query));
        let actual = Normalized::new(
            &pg.sparql(&query, &SparqlOptions::default())
                .unwrap_or_else(|e| panic!("{query}: {e}")),
        );
        assert_eq!(actual, expected, "{query}");
        assert_eq!(
            actual.is_empty(),
            *kind == Empty,
            "unexpected (non-)empty result for {query}: {actual:?}"
        );
    }
}

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
        .layers(vec!["http://ex/knows", "http://ex/label"])
        .unwrap();
    let by_label = "SELECT ?s ?o { ?p <http://ex/label> \"knows\" . ?s ?p ?o }";
    assert_eq!(
        select(&knows_label, by_label),
        vec![
            vec![a.to_owned(), b.to_owned()],
            vec![c.to_owned(), a.to_owned()]
        ]
    );
    assert_eq!(
        select(&rg.layers("http://ex/label").unwrap(), by_label),
        vec![] as Vec<Vec<String>>
    );
    // with the label layer hidden the join has nothing to start from
    let knows_only = rg.layers("http://ex/knows").unwrap();
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

#[test]
fn internal_terms_are_canonical() {
    let rg = RdfGraph::new();
    let doc = "<http://ex/a> <http://ex/knows> <http://ex/b> .
<http://ex/knows> <http://ex/label> \"knows\" .";
    rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    let view = rg.graph().clone().into_dynamic();
    let dataset = RaphtoryDataset::new(view.clone());
    let term = |s: &str| -> Term { NamedNode::new(s).unwrap().into() };
    let node = |s: &str| RdfTerm::Node(NodeTerm(view.node(s).unwrap()));

    let knows = dataset.internalize_term(term("http://ex/knows")).unwrap();
    assert_eq!(knows, node("http://ex/knows"));
    let label = dataset.internalize_term(term("http://ex/label")).unwrap();
    assert_eq!(label, RdfTerm::Layer("http://ex/label".into()));
    let unknown = dataset.internalize_term(term("http://ex/zzz")).unwrap();
    assert_eq!(unknown, RdfTerm::Other(term("http://ex/zzz")));
    let literal: Term = Literal::new_simple_literal("knows").into();
    assert_eq!(
        dataset.internalize_term(literal.clone()).unwrap(),
        node("\"knows\"")
    );
    let other_literal: Term = Literal::new_simple_literal("nope").into();
    assert_eq!(
        dataset.internalize_term(other_literal.clone()).unwrap(),
        RdfTerm::Other(other_literal)
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
    let a = node("http://ex/a");
    for (s, p, o) in [
        (Some(&unknown), None, None),
        (Some(&label), None, None),
        (None, None, Some(&unknown)),
        (None, Some(&unknown), None),
        (None, Some(&a), None),
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

    // a node the view hides is not a node of the query
    assert_eq!(
        RaphtoryDataset::new(
            rg.subgraph(["http://ex/a", "http://ex/knows"])
                .graph()
                .clone()
                .into_dynamic()
        )
        .internalize_term(term("http://ex/b"))
        .unwrap(),
        RdfTerm::Other(term("http://ex/b"))
    );
}
