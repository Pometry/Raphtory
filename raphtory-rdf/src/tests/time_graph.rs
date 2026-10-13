//! Time graphs `<raphtory:asof:T>`.
use super::{
    ask, layer_of, select,
    temporal::{assert_at, nt, retract_at, spo, O, O2, P, Q, S, SPO},
};
use crate::{
    query::parse, time_graph::time_graphs_in, QueryForm, QueryOutline, RdfError, RdfGraph, RdfView,
    SparqlOptions, ASOF_NS,
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

/// The triples of the time graph `T` of the view, read with `GRAPH`.
fn in_graph<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    t: impl Display,
) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o {{ GRAPH {} {{ ?s ?p ?o }} }}", asof(t));
    spo(view, &query)
}

#[test]
fn query_outlines() {
    let options = SparqlOptions::default();
    let outline = QueryOutline::parse(
        "CONSTRUCT { ?s ?p ?o } FROM raphtory:asof:3 FROM NAMED raphtory:asof:5 \
         WHERE { GRAPH raphtory:asof:5 { ?s ?p ?o } }",
        &options,
    )
    .unwrap();
    assert_eq!(outline.form, QueryForm::Construct);
    assert!(outline.reads_time_graphs);
    let outline = QueryOutline::parse("SELECT * { ?s ?p ?o }", &options).unwrap();
    assert_eq!(outline.form, QueryForm::Select);
    // a time graph built by the query
    let outline = QueryOutline::parse(
        "SELECT ?s ?o { VALUES ?t {\"3\"} BIND(IRI(CONCAT(\"raphtory:asof:\", ?t)) AS ?g) \
         LATERAL { GRAPH ?g { ?s <http://ex/knows> ?o } } }",
        &options,
    )
    .unwrap();
    assert!(outline.reads_time_graphs);
    // a time graph argument of a temporal function names a time graph but reads none
    let outline = QueryOutline::parse(
        "SELECT * { ?s ?p ?o BIND(raphtory:validFrom(?s, ?p, ?o, raphtory:asof:3) AS ?t) }",
        &options,
    )
    .unwrap();
    assert!(!outline.reads_time_graphs);
    assert!(matches!(
        QueryOutline::parse("SELECT * {", &options),
        Err(RdfError::SparqlSyntax(_))
    ));
    assert!(matches!(
        QueryOutline::parse("SELECT * {", &options.with_max_query_length(5)),
        Err(RdfError::QueryTooLong { .. })
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

/// A time graph of a view is `view.before(T + 1).valid()`.
#[test]
fn time_graphs_are_valid_before() {
    let rg = RdfGraph::new();
    assert_at(&rg, 10, SPO);
    retract_at(&rg, 30, SPO);
    assert_at(&rg, 50, SPO);
    assert_at(&rg, 20, [S, Q, O]);
    let sqo = nt([S, Q, O]);
    assert_eq!(in_graph(&rg, 25), BTreeSet::from([nt(SPO), sqo.clone()]));
    assert_eq!(in_graph(&rg, 30), BTreeSet::from([sqo.clone()]));
    assert_eq!(in_graph(&rg.window(15, 35), 60), BTreeSet::from([sqo]));
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
/// every time graph exists, and a time that does not parse is an error.
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
    let query = format!("SELECT * {{ GRAPH {} {{ ?s ?p ?o }} }}", asof("garbage"));
    assert!(matches!(
        rg.sparql(&query, &SparqlOptions::default()),
        Err(RdfError::SparqlEvaluation(QueryEvaluationError::Dataset(_)))
    ));
}
