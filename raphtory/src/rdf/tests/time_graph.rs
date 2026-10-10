//! Time graphs `<raphtory:asof:T>`.
use super::{
    layer_of, select,
    temporal::{
        assert_at, log, model, model_visible_at, nt, retract_at, triples, Event, LOG_TRIPLES, O,
        O2, P, Q, S, SPO,
    },
};
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::*,
    rdf::{
        dataset::{RaphtoryDataset, RdfTerm},
        model::{NamedNode, Term},
        time_graph::time_graphs_in,
        QueryForm, QueryOutline, RdfError, RdfFormat, SparqlDataset, SparqlOptions, SparqlResults,
        TimeGraph, ASOF_NS, RESERVED_NS,
    },
};
use oxigraph::sparql::QueryEvaluationError;
use proptest::prelude::*;
use spareval::QueryableDataset;
use spargebra::SparqlParser;
use std::{collections::BTreeSet, error::Error, fmt::Display, sync::Arc};

/// The IRI of the time graph `T`, in SPARQL (and N-Triples) form.
fn asof(t: impl Display) -> String {
    format!("<{ASOF_NS}{t}>")
}

fn iri(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

/// The rows of a query that projects `?s ?p ?o`, as a set. Fails on duplicate rows.
fn spo<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
) -> BTreeSet<[String; 3]> {
    let rows = select(view, query);
    let set: BTreeSet<[String; 3]> = rows
        .iter()
        .map(|r| [r[0].clone(), r[1].clone(), r[2].clone()])
        .collect();
    assert_eq!(set.len(), rows.len(), "{query}: duplicate rows: {rows:?}");
    set
}

/// The triples of the time graph `T` of the view, read with `GRAPH`.
fn in_graph<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    t: impl Display,
) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o {{ GRAPH {} {{ ?s ?p ?o }} }}", asof(t));
    spo(view, &query)
}

/// The triples of the time graph `T` of the view, read as the default graph with `FROM`.
fn from_graph<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    t: impl Display,
) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o FROM {} {{ ?s ?p ?o }}", asof(t));
    spo(view, &query)
}

fn apply(g: &RdfGraph, log: &[Event]) {
    for e in log {
        if e.assert {
            assert_at(g, e.t, LOG_TRIPLES[e.triple]);
        } else {
            retract_at(g, e.t, LOG_TRIPLES[e.triple]);
        }
    }
}

/// The time graph `<raphtory:asof:{t}>`.
fn time_graph(t: &str) -> TimeGraph {
    TimeGraph::parse(&iri(&format!("{ASOF_NS}{t}")))
        .unwrap()
        .unwrap_or_else(|| panic!("{t}"))
}

/// The IRI of the `InvalidTimeGraph` error a query fails with. Its message holds the cause,
/// so an error chain does not repeat it.
fn invalid_time_graph<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
) -> String {
    let err = view
        .sparql(query)
        .err()
        .unwrap_or_else(|| panic!("{query}: no error"));
    assert!(err.source().is_none(), "{query}: {err}");
    if let GraphError::Rdf(RdfError::SparqlEvaluation(QueryEvaluationError::Dataset(e))) = &err {
        if let Some(RdfError::InvalidTimeGraph { iri, .. }) = e.downcast_ref::<RdfError>() {
            return iri.as_str().to_owned();
        }
    }
    panic!("{query}: unexpected error {err:?}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Time graphs match a reference model of the log, read with `GRAPH`, with `FROM`, and all
    /// at once with `FROM NAMED` and with `VALUES`.
    #[test]
    fn time_graphs_match_a_reference_model(log in log()) {
        let rg = RdfGraph::new();
        apply(&rg, &log);
        let times: Vec<i64> = (-1..=11).collect();
        let mut by_graph = Vec::new();
        for &t in &times {
            let expected = model(|i| model_visible_at(&log, i, Some(t)));
            prop_assert_eq!(in_graph(&rg, t), expected.clone(), "GRAPH asof:{}", t);
            prop_assert_eq!(from_graph(&rg, t), expected.clone(), "FROM asof:{}", t);
            by_graph.extend(expected.into_iter().map(|[s, p, o]| vec![asof(t), s, p, o]));
        }
        by_graph.sort();

        let named: String = times
            .iter()
            .map(|t| format!("FROM NAMED {} ", asof(t)))
            .collect();
        let query = format!("SELECT ?g ?s ?p ?o {named} {{ GRAPH ?g {{ ?s ?p ?o }} }}");
        prop_assert_eq!(select(&rg, &query), by_graph.clone());
        let values: Vec<String> = times.iter().map(asof).collect();
        let query = format!(
            "SELECT ?g ?s ?p ?o {{ VALUES ?g {{ {} }} GRAPH ?g {{ ?s ?p ?o }} }}",
            values.join(" ")
        );
        prop_assert_eq!(select(&rg, &query), by_graph);
    }
}

/// Invalid time graphs are errors.
#[test]
fn invalid_time_graphs() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    let graph = format!("{ASOF_NS}garbage");
    for query in [
        format!("SELECT * {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}"),
        format!("SELECT * {{ VALUES ?g {{ <{graph}> }} GRAPH ?g {{ ?s ?p ?o }} }}"),
    ] {
        assert_eq!(invalid_time_graph(&rg, &query), graph, "{query}");
    }
}

#[test]
fn query_outlines() {
    let outline = QueryOutline::parse(
        "CONSTRUCT { ?s ?p ?o } FROM raphtory:asof:3 FROM NAMED raphtory:asof:5 \
         WHERE { GRAPH raphtory:asof:5 { ?s ?p ?o } }",
    )
    .unwrap();
    assert_eq!(outline.form, QueryForm::Construct);
    assert_eq!(outline.time_graphs, [time_graph("5"), time_graph("3")]);
    assert!(outline.reads_time_graphs);
    let outline = QueryOutline::parse("SELECT * { ?s ?p ?o }").unwrap();
    assert_eq!(outline.form, QueryForm::Select);
    assert_eq!(outline.time_graphs, []);
    // a reference time of a temporal function names a time graph but reads none
    let outline = QueryOutline::parse(
        "SELECT * { ?s ?p ?o BIND(raphtory:validFrom(?s, ?p, ?o, raphtory:asof:3) AS ?t) }",
    )
    .unwrap();
    assert_eq!(outline.time_graphs, [time_graph("3")]);
    assert!(!outline.reads_time_graphs);
    assert!(matches!(
        QueryOutline::parse("SELECT * {"),
        Err(GraphError::Rdf(RdfError::SparqlSyntax(_)))
    ));
}

/// The time graphs a query names are the constant IRIs under `raphtory:asof:` written anywhere
/// in it.
#[test]
fn time_graphs_named_by_a_query() {
    let named = |query: &str| -> BTreeSet<String> {
        let query = SparqlParser::new()
            .with_prefix("raphtory", RESERVED_NS)
            .unwrap()
            .parse_query(query)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
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
    retract_at(&rg, 4, SPO);
    assert_at(&rg, 6, SPO);
    assert_at(&rg, 2, [S, Q, O]);
    retract_at(&rg, 2, [S, Q, O]);
    retract_at(&rg, 3, [S, P, O2]);
    assert_at(&rg, 5, [S, P, O2]);
    retract_at(&rg, 8, [S, P, O2]);
    assert_at(&rg, 0, [O, P, S]);
    assert_at(&rg, 7, [O, P, S]);
    let window = rg.window(3, 7);
    for t in [2, 5, 7] {
        let as_of = triples(&rg.snapshot_at(t));
        assert!(!as_of.is_empty(), "t = {t}");
        let expected = if (3..7).contains(&t) {
            as_of
        } else {
            BTreeSet::new()
        };
        assert_eq!(in_graph(&window, t), expected, "t = {t}");
    }
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

/// The internal terms of time graphs, and the quads and views of the dataset.
#[test]
fn time_graph_internals() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    assert_at(&rg, 1, [S, Q, O]);
    retract_at(&rg, 3, SPO);
    let dataset = RaphtoryDataset::new(rg.graph().clone());
    let graph = |t: &str| dataset.internalize_term(iri(t).into()).unwrap();
    let at_2 = graph("raphtory:asof:2");
    let also_at_2 = graph("raphtory:asof:+2");
    let at_5 = graph("raphtory:asof:5");
    assert_eq!(at_2, RdfTerm::TimeGraph(Arc::new(time_graph("2"))));
    assert_ne!(at_2, also_at_2);
    assert_eq!(graph("raphtory:asof:2"), at_2);
    let p = graph(P);
    let s = graph(S);

    // every time graph exists, but none is listed
    assert!(dataset.contains_internal_graph_name(&at_2).unwrap());
    assert!(dataset.contains_internal_graph_name(&at_5).unwrap());
    assert!(!dataset.contains_internal_graph_name(&p).unwrap());
    assert!(!dataset
        .contains_internal_graph_name(&graph("http://ex/g"))
        .unwrap());
    assert_eq!(dataset.internal_named_graphs().count(), 0);
    assert_eq!(
        dataset
            .internal_quads_for_pattern(None, None, None, None)
            .count(),
        0
    );

    // quads of a time graph carry its name
    let quads = |s: Option<&RdfTerm>, p: Option<&RdfTerm>, g: Option<Option<&RdfTerm>>| {
        dataset
            .internal_quads_for_pattern(s, p, None, g)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let in_2 = quads(None, None, Some(Some(&at_2)));
    assert_eq!(in_2.len(), 2);
    assert!(in_2.iter().all(|q| q.graph_name.as_ref() == Some(&at_2)));
    let in_5 = quads(Some(&s), None, Some(Some(&at_5)));
    assert_eq!(in_5.len(), 1);
    assert_eq!(in_5[0].graph_name, Some(at_5.clone()));
    let default = quads(None, None, Some(None));
    assert_eq!(default.len(), 2);
    assert!(default.iter().all(|q| q.graph_name.is_none()));
    // ... and the time graph is not a node, so it is never a subject
    assert!(quads(Some(&at_2), None, Some(Some(&at_2))).is_empty());
    assert!(quads(None, Some(&at_2), Some(None)).is_empty());

    // every spelling of a time reads the same triples, and a predicate reads its own layer
    assert_eq!(quads(None, None, Some(Some(&also_at_2))).len(), 2);
    assert_eq!(quads(Some(&s), None, Some(Some(&at_2))).len(), 2);
    assert_eq!(quads(None, Some(&p), Some(Some(&at_2))).len(), 1);
    assert_eq!(quads(None, Some(&p), Some(Some(&also_at_2))).len(), 1);
    assert!(quads(None, Some(&p), Some(Some(&at_5))).is_empty());

    // the spellings of one instant are distinct terms, externalized as written, with the same
    // triples
    let spellings: BTreeSet<String> = ["1704067200000", "2024-01-01", "2024-01-01T01:00:00+01:00"]
        .into_iter()
        .map(|form| {
            assert_eq!(time_graph(form).at, 1_704_067_200_000, "{form}");
            let term: Term = iri(&format!("{ASOF_NS}{form}")).into();
            let internal = dataset.internalize_term(term.clone()).unwrap();
            assert_eq!(dataset.externalize_term(internal.clone()).unwrap(), term);
            assert_eq!(quads(None, None, Some(Some(&internal))).len(), 1, "{form}");
            format!("{internal:?}")
        })
        .collect();
    assert_eq!(spellings.len(), 3);

    // a dataset lists the time graphs it is given, in order and once per IRI ...
    let listed = RaphtoryDataset::new(rg.graph().clone())
        .with_time_graphs([time_graph("2"), time_graph("+2"), time_graph("2")])
        .with_time_graphs([time_graph("5"), time_graph("+2")]);
    let names: Vec<RdfTerm> = listed
        .internal_named_graphs()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, [at_2.clone(), also_at_2.clone(), at_5.clone()]);
    // ... and a pattern on every named graph reads each of them, with its name
    let in_all = |s: Option<&RdfTerm>, p: Option<&RdfTerm>| {
        let mut names: Vec<_> = listed
            .internal_quads_for_pattern(s, p, None, None)
            .map(|quad| quad.unwrap().graph_name.unwrap())
            .collect();
        names.sort_by_key(|name| format!("{name:?}"));
        names
    };
    let mut expected = vec![
        at_2.clone(),
        at_2.clone(),
        also_at_2.clone(),
        also_at_2,
        at_5,
    ];
    expected.sort_by_key(|name| format!("{name:?}"));
    assert_eq!(in_all(None, None), expected);
    assert_eq!(in_all(Some(&s), Some(&p)).len(), 2);
    assert!(in_all(Some(&at_2), None).is_empty());
    // listed time graphs exist like any other, and other graphs still do not
    assert!(listed.contains_internal_graph_name(&at_2).unwrap());
    assert!(listed
        .contains_internal_graph_name(&graph("raphtory:asof:7"))
        .unwrap());
    assert!(!listed.contains_internal_graph_name(&p).unwrap());
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
        let SparqlResults::Solutions { rows, .. } = rg.sparql_with(query, options).unwrap() else {
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
    // no default graph is an empty one, as is a graph that is not a time graph
    assert_eq!(subjects(default, &with(&[], &["raphtory:asof:2"])), s(&[]));
    assert_eq!(subjects(default, &with(&["http://ex/g"], &[])), s(&[]));

    // `GRAPH` matches only the named graphs of the dataset
    let in_graphs = "SELECT ?s ?g { GRAPH ?g { ?s ?p ?o } }";
    assert_eq!(
        subjects(
            in_graphs,
            &with(&[], &["raphtory:asof:2", "raphtory:asof:6"])
        ),
        s(&["alice", "bob"])
    );
    let in_6 = "SELECT ?s { GRAPH raphtory:asof:6 { ?s ?p ?o } }";
    assert_eq!(subjects(in_6, &SparqlOptions::default()), s(&["bob"]));
    assert_eq!(subjects(in_6, &with(&[], &["raphtory:asof:2"])), s(&[]));
    assert_eq!(subjects(in_6, &with(&["raphtory:asof:6"], &[])), s(&[]));
    let from_named = "SELECT ?s FROM NAMED raphtory:asof:6 { GRAPH ?g { ?s ?p ?o } }";
    assert_eq!(subjects(from_named, &SparqlOptions::default()), s(&["bob"]));
    assert_eq!(
        subjects(from_named, &with(&[], &["raphtory:asof:2"])),
        s(&["alice"])
    );
}
