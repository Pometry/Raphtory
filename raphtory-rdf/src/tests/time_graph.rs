//! Time graphs `<raphtory:asof:T>`.
use super::{
    ask, layer_of, select,
    temporal::{assert_at, nt, retract_at, spo, O, O2, P, Q, S, SPO},
};
use crate::{
    query::parse, time_graph::time_graphs_in, NamedNode, QueryForm, QueryOutline, RdfError,
    RdfFormat, RdfGraph, RdfView, SparqlDataset, SparqlOptions, SparqlResults, ASOF_NS,
};
use oxigraph::sparql::QueryEvaluationError;
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};
use std::{collections::BTreeSet, fmt::Display};

/// The IRI of the time graph `T`, in SPARQL (and N-Triples) form.
fn asof(t: impl Display) -> String {
    format!("<{ASOF_NS}{t}>")
}

fn iri(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

/// The triples of the time graph `T` of the view, read with `GRAPH`.
fn in_graph<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    t: impl Display,
) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o {{ GRAPH {} {{ ?s ?p ?o }} }}", asof(t));
    spo(view, &query)
}

/// An invalid time graph is an error.
#[test]
fn invalid_time_graph() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    let graph = format!("{ASOF_NS}garbage");
    let query = format!("SELECT * {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}");
    let Err(RdfError::SparqlEvaluation(QueryEvaluationError::Dataset(e))) =
        rg.sparql(&query, &SparqlOptions::default())
    else {
        panic!("{query}")
    };
    assert!(matches!(
        e.downcast_ref::<RdfError>(),
        Some(RdfError::InvalidTimeGraph { iri, .. }) if iri.as_str() == graph
    ));
}

#[test]
fn query_outlines() {
    let outline = QueryOutline::parse(
        "CONSTRUCT { ?s ?p ?o } FROM raphtory:asof:3 FROM NAMED raphtory:asof:5 \
         WHERE { GRAPH raphtory:asof:5 { ?s ?p ?o } }",
    )
    .unwrap();
    assert_eq!(outline.form, QueryForm::Construct);
    assert!(outline.reads_time_graphs);
    let outline = QueryOutline::parse("SELECT * { ?s ?p ?o }").unwrap();
    assert_eq!(outline.form, QueryForm::Select);
    // a time graph built by the query
    let outline = QueryOutline::parse(
        "SELECT ?s ?o { VALUES ?t {\"3\"} BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) \
         LATERAL { GRAPH ?g { ?s <http://ex/knows> ?o } } }",
    )
    .unwrap();
    assert!(outline.reads_time_graphs);
    // a reference time of a temporal function names a time graph but reads none
    let outline = QueryOutline::parse(
        "SELECT * { ?s ?p ?o BIND(raphtory:validFrom(?s, ?p, ?o, raphtory:asof:3) AS ?t) }",
    )
    .unwrap();
    assert!(!outline.reads_time_graphs);
    assert!(matches!(
        QueryOutline::parse("SELECT * {"),
        Err(RdfError::SparqlSyntax(_))
    ));
}

/// The time graphs a query names are the constant IRIs under `raphtory:asof:` written anywhere
/// in it.
#[test]
fn time_graphs_named_by_a_query() {
    let named = |query: &str| -> BTreeSet<String> {
        let query = parse(query).unwrap_or_else(|e| panic!("{query}: {e}"));
        let graphs = time_graphs_in(&query);
        let names: BTreeSet<String> = graphs.iter().map(|g| g.iri.as_str().to_owned()).collect();
        assert_eq!(names.len(), graphs.len(), "duplicates in {graphs:?}");
        names
    };
    let asof = |times: &[i64]| -> BTreeSet<String> {
        times.iter().map(|t| format!("{ASOF_NS}{t}")).collect()
    };
    assert_eq!(named("SELECT * { GRAPH ?g { ?s ?p ?o } }"), asof(&[]));
    assert_eq!(
        named(
            "SELECT * {
                VALUES (?g ?h) {
                    (raphtory:asof:1 UNDEF) (UNDEF <raphtory:asof:2>) (raphtory:asof:1 \"raphtory:asof:9\")
                }
                GRAPH ?g { ?s ?p ?o }
            }"
        ),
        asof(&[1, 2])
    );
    assert_eq!(
        named(
            "SELECT * {
                GRAPH raphtory:asof:1 { ?s ?p ?o }
                BIND(raphtory:asof:2 AS ?a)
                BIND(IRI(\"raphtory:asof:3\") AS ?b)
                BIND(URI(\"raphtory:asof:4\") AS ?c)
                FILTER(?a != raphtory:asof:5)
                OPTIONAL { ?s ?p ?o FILTER(?o IN (raphtory:asof:6)) }
                FILTER EXISTS { GRAPH raphtory:asof:7 { } }
                MINUS { raphtory:asof:8 ?p ?o }
            }"
        ),
        asof(&[1, 2, 3, 4, 5, 6, 7, 8])
    );
    assert_eq!(
        named(
            "SELECT ?s (COUNT(IF(?o = raphtory:asof:1, 1, 0)) AS ?n) {
                { ?s ?p ?o } UNION { SELECT ?s ?o { ?s ?p raphtory:asof:2 } }
                LATERAL { GRAPH raphtory:asof:3 { ?s ?p ?x } }
            } GROUP BY ?s ORDER BY (?s = raphtory:asof:4)"
        ),
        asof(&[1, 2, 3, 4])
    );
    assert_eq!(
        named(
            "CONSTRUCT { ?s ?p raphtory:asof:1 } FROM NAMED raphtory:asof:2
            WHERE { GRAPH ?g { ?s ?p ?o } }"
        ),
        asof(&[1])
    );
    assert_eq!(
        named("ASK { VALUES ?g { <raphtory:asof:2024-01-01> } }"),
        BTreeSet::from(["raphtory:asof:2024-01-01".to_owned()])
    );
    assert_eq!(
        named("DESCRIBE ?s { GRAPH <raphtory:asof:+2> { ?s ?p ?o } }"),
        BTreeSet::from(["raphtory:asof:+2".to_owned()])
    );
    // not constant time graphs: strings, IRIs built from other values, invalid times
    assert_eq!(
        named(
            "SELECT * {
                BIND(\"raphtory:asof:1\" AS ?a)
                BIND(IRI(CONCAT(\"raphtory:asof:\", \"2\")) AS ?b)
                BIND(raphtory:asof:garbage AS ?d)
            }"
        ),
        asof(&[])
    );
}

/// Time graphs of a windowed view intersect with its window.
#[test]
fn time_graphs_of_windows() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    let window = rg.window(3, 7);
    assert!(in_graph(&window, 2).is_empty());
    assert_eq!(in_graph(&window, 5), BTreeSet::from([nt(SPO)]));
    assert!(in_graph(&window, 7).is_empty());
}

/// Time graphs keep the layer and node filters of the view.
#[test]
fn time_graphs_of_other_views() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    assert_at(&rg, 1, [S, Q, O]);
    assert_at(&rg, 1, [O, P, O2]);
    retract_at(&rg, 3, SPO);
    assert_eq!(
        in_graph(&rg.layers(layer_of(P)).unwrap(), 2),
        BTreeSet::from([nt(SPO), nt([O, P, O2])])
    );
    assert_eq!(
        in_graph(&rg.exclude_nodes([O2]), 3),
        BTreeSet::from([nt([S, Q, O])])
    );
}

/// Each spelling of an instant is its own time graph, named as written, with the same triples;
/// every time graph exists.
#[test]
fn spellings_of_an_instant() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1_704_067_200_000, SPO);
    let rows = select(
        &rg,
        "SELECT ?g ?s ?p ?o { VALUES ?g { <raphtory:asof:1704067200000> \
         <raphtory:asof:2024-01-01> <raphtory:asof:2024-01-01T01:00:00+01:00> } \
         GRAPH ?g { ?s ?p ?o } }",
    );
    let mut expected: Vec<Vec<String>> =
        ["1704067200000", "2024-01-01", "2024-01-01T01:00:00+01:00"]
            .into_iter()
            .map(|form| {
                vec![
                    asof(form),
                    "<http://ex/s>".to_owned(),
                    "<http://ex/p>".to_owned(),
                    "<http://ex/o>".to_owned(),
                ]
            })
            .collect();
    expected.sort();
    assert_eq!(rows, expected);
    assert!(ask(&rg, "ASK { GRAPH raphtory:asof:7 { } }"));
}

/// `SparqlOptions::dataset` replaces the `FROM` and `FROM NAMED` clauses of the query.
#[test]
fn a_dataset_option_replaces_the_dataset_of_the_query() {
    // `alice knows bob` holds from 1 to 5, `bob knows carol` from 3 on
    let rg = RdfGraph::new();
    let knows = |s: &str, o: &str| format!("<http://ex/{s}> <http://ex/knows> <http://ex/{o}> .");
    let load = |t: i64, doc: String| rg.load_rdf(t, doc.as_bytes(), RdfFormat::NTriples, None);
    load(1, knows("alice", "bob")).unwrap();
    load(3, knows("bob", "carol")).unwrap();
    rg.retract_rdf(
        5,
        knows("alice", "bob").as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    let with = |default: &[&str], named: &[&str]| {
        SparqlOptions::default().with_dataset(SparqlDataset {
            default_graphs: default.iter().map(|g| iri(g)).collect(),
            named_graphs: named.iter().map(|g| iri(g)).collect(),
        })
    };
    let subjects = |query: &str, options: &SparqlOptions| -> Vec<String> {
        let SparqlResults::Solutions { rows, .. } = rg.sparql(query, options).unwrap() else {
            panic!("{query}: not a SELECT result")
        };
        let mut subjects: Vec<String> = rows
            .iter()
            .map(|row| row[0].as_ref().unwrap().to_string())
            .collect();
        subjects.sort();
        subjects
    };
    let s = |names: &[&str]| -> Vec<String> {
        names.iter().map(|n| format!("<http://ex/{n}>")).collect()
    };
    let default = "SELECT ?s { ?s ?p ?o }";

    assert_eq!(
        subjects(default, &SparqlOptions::default()),
        s(&["alice", "bob"])
    );
    assert_eq!(
        subjects(default, &with(&["raphtory:asof:2"], &[])),
        s(&["alice"])
    );
    // it replaces `FROM`
    let from_4 = "SELECT ?s FROM raphtory:asof:4 { ?s ?p ?o }";
    assert_eq!(
        subjects(from_4, &SparqlOptions::default()),
        s(&["alice", "bob"])
    );
    assert_eq!(
        subjects(from_4, &with(&["raphtory:asof:2"], &[])),
        s(&["alice"])
    );
    // a graph that is not a time graph is an empty one
    assert_eq!(subjects(default, &with(&["http://ex/g"], &[])), s(&[]));
    let from_named = "SELECT ?s FROM NAMED raphtory:asof:6 { GRAPH ?g { ?s ?p ?o } }";
    assert_eq!(subjects(from_named, &SparqlOptions::default()), s(&["bob"]));
    assert_eq!(
        subjects(from_named, &with(&[], &["raphtory:asof:2"])),
        s(&["alice"])
    );
}
