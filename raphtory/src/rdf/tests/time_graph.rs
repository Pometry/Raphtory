//! Time graphs `<raphtory:asof:T>`.
use super::{
    ask, select,
    temporal::{
        assert_at, log, model, model_asserted_by, model_visible_at, nt, retract_at, triples,
        visible, Event, LOG_TRIPLES, O, O2, P, Q, S, SPO,
    },
};
use crate::{
    db::api::view::IntoDynamic,
    errors::GraphError,
    prelude::*,
    rdf::{
        model::{NamedNode, Term},
        name_of, term_of,
        time_graph::time_graphs_in,
        RaphtoryDataset, RdfError, RdfFormat, RdfTerm, RdfViewOps, SparqlResults, TimeGraph,
        ASOF_NS, RESERVED_NS,
    },
};
use oxigraph::sparql::QueryEvaluationError;
use proptest::prelude::*;
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::TryIntoTime};
use spareval::QueryableDataset;
use spargebra::SparqlParser;
use std::{collections::BTreeSet, fmt::Display, sync::Arc};

/// The IRI of the time graph `T`, in SPARQL (and N-Triples) form.
fn asof(t: impl Display) -> String {
    format!("<{ASOF_NS}{t}>")
}

fn iri(s: &str) -> NamedNode {
    NamedNode::new(s).unwrap()
}

/// The rows of a query that projects `?s ?p ?o`, as a set. Fails on duplicate rows.
fn spo<G: RdfViewOps>(view: &G, query: &str) -> BTreeSet<[String; 3]> {
    let rows = select(view, query);
    let set: BTreeSet<[String; 3]> = rows
        .iter()
        .map(|r| [r[0].clone(), r[1].clone(), r[2].clone()])
        .collect();
    assert_eq!(set.len(), rows.len(), "{query}: duplicate rows: {rows:?}");
    set
}

/// The triples of the time graph `T` of the view, read with `GRAPH`.
fn in_graph<G: RdfViewOps>(view: &G, t: impl Display) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o {{ GRAPH {} {{ ?s ?p ?o }} }}", asof(t));
    spo(view, &query)
}

/// The triples of the time graph `T` of the view, read as the default graph with `FROM`.
fn from_graph<G: RdfViewOps>(view: &G, t: impl Display) -> BTreeSet<[String; 3]> {
    let query = format!("SELECT ?s ?p ?o FROM {} {{ ?s ?p ?o }}", asof(t));
    spo(view, &query)
}

/// Whether `<s> <p> <o>` is in the time graph `T` of the view. It is checked with each of the 8
/// binding patterns inside `GRAPH`, which must agree.
fn visible_in<G: RdfViewOps>(view: &G, t: i64, spo: [&str; 3]) -> bool {
    let vars = ["?s", "?p", "?o"];
    let answers: Vec<bool> = (0..8)
        .map(|mask| {
            let mut pattern = Vec::new();
            let mut projection = vec!["?x"];
            let mut expected = vec!["\"x\"".to_owned()];
            for i in 0..3 {
                let constant = format!("<{}>", spo[i]);
                if mask & (1 << i) != 0 {
                    pattern.push(vars[i].to_owned());
                    projection.push(vars[i]);
                    expected.push(constant);
                } else {
                    pattern.push(constant);
                }
            }
            let query = format!(
                "SELECT {} {{ GRAPH {} {{ {} }} BIND(\"x\" AS ?x) }}",
                projection.join(" "),
                asof(t),
                pattern.join(" ")
            );
            select(view, &query).contains(&expected)
        })
        .collect();
    assert!(
        answers.iter().all(|a| *a == answers[0]),
        "binding patterns disagree for {spo:?} at {t}: {answers:?}"
    );
    answers[0]
}

fn apply<G: RdfMutationOps>(g: &G, log: &[Event]) {
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

/// The IRI of the `InvalidTimeGraph` error a query fails with.
fn invalid_time_graph<G: RdfViewOps>(view: &G, query: &str) -> String {
    let err = view
        .sparql(query)
        .err()
        .unwrap_or_else(|| panic!("{query}: no error"));
    if let GraphError::Rdf(RdfError::SparqlEvaluation(QueryEvaluationError::Dataset(e))) = &err {
        if let Some(RdfError::InvalidTimeGraph { iri, .. }) = e.downcast_ref::<RdfError>() {
            return iri.clone();
        }
    }
    panic!("{query}: unexpected error {err:?}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Time graphs match the reference model of `temporal::as_of_matches_a_reference_model`,
    /// read with `GRAPH`, with `FROM`, and all at once with `FROM NAMED` and with `VALUES`.
    #[test]
    fn time_graphs_match_a_reference_model(log in log()) {
        let pg = PersistentGraph::new();
        apply(&pg, &log);
        let events = pg.event_graph();
        let times: Vec<i64> = (-1..=11).collect();
        let mut by_graph = Vec::new();
        for &t in &times {
            let expected = model(|i| model_visible_at(&log, i, Some(t)));
            prop_assert_eq!(in_graph(&pg, t), expected.clone(), "GRAPH asof:{}", t);
            prop_assert_eq!(from_graph(&pg, t), expected.clone(), "FROM asof:{}", t);
            by_graph.extend(expected.into_iter().map(|[s, p, o]| vec![asof(t), s, p, o]));
            // on an event graph: asserted at or before `t`
            let expected = model(|i| model_asserted_by(&log, i, t));
            prop_assert_eq!(in_graph(&events, t), expected.clone(), "events GRAPH asof:{}", t);
            prop_assert_eq!(from_graph(&events, t), expected, "events FROM asof:{}", t);
        }
        by_graph.sort();

        let named: String = times
            .iter()
            .map(|t| format!("FROM NAMED {} ", asof(t)))
            .collect();
        let query = format!("SELECT ?g ?s ?p ?o {named} {{ GRAPH ?g {{ ?s ?p ?o }} }}");
        prop_assert_eq!(select(&pg, &query), by_graph.clone());
        let values: Vec<String> = times.iter().map(asof).collect();
        let query = format!(
            "SELECT ?g ?s ?p ?o {{ VALUES ?g {{ {} }} GRAPH ?g {{ ?s ?p ?o }} }}",
            values.join(" ")
        );
        prop_assert_eq!(select(&pg, &query), by_graph);

        // the default graph is the current state
        prop_assert_eq!(triples(&pg), model(|i| model_visible_at(&log, i, None)));
    }
}

/// Each of the 8 binding patterns inside `GRAPH <raphtory:asof:T>` sees what it sees on
/// `snapshot_at(T)`.
#[test]
fn binding_patterns_in_time_graphs() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 4, SPO);
    assert_at(&pg, 6, SPO);
    assert_at(&pg, 2, [S, Q, O]);
    retract_at(&pg, 3, [S, P, O2]);
    assert_at(&pg, 5, [S, P, O2]);
    assert_at(&pg, 0, [O, P, S]);
    retract_at(&pg, 7, [O, P, S]);
    let all = [SPO, [S, Q, O], [S, P, O2], [O, P, S]];
    for t in -1..=9 {
        let snapshot = pg.snapshot_at(t);
        for spo in all {
            assert_eq!(
                visible_in(&pg, t, spo),
                visible(&snapshot, spo),
                "{spo:?} at {t}"
            );
        }
        assert_eq!(in_graph(&pg, t), triples(&snapshot), "t = {t}");
        // the quads of a time graph, built back into triples, are the export of the snapshot
        let SparqlResults::Graph(constructed) = pg
            .sparql(&format!(
                "CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH {} {{ ?s ?p ?o }} }}",
                asof(t)
            ))
            .unwrap()
        else {
            panic!("not a graph")
        };
        let constructed: BTreeSet<String> = constructed.iter().map(|t| format!("{t} .")).collect();
        let mut exported = Vec::new();
        snapshot.to_rdf(&mut exported, RdfFormat::NTriples).unwrap();
        let exported: BTreeSet<String> = String::from_utf8(exported)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(constructed, exported, "t = {t}");
    }
}

const WORKS_FOR: &str = "http://ex/worksFor";

/// The history of `comparing_dates_in_one_query`: who works for whom, by date.
fn employment() -> PersistentGraph {
    let pg = PersistentGraph::new();
    let fact = |who: &str, org: &str| {
        [
            format!("http://ex/{who}"),
            WORKS_FOR.to_owned(),
            format!("http://ex/{org}"),
        ]
    };
    let at = |date: &str, who: &str, org: &str, assert: bool| {
        let [s, p, o] = fact(who, org);
        let triple = crate::rdf::model::Triple::new(iri(&s), iri(&p), iri(&o));
        if assert {
            pg.add_triple(date, &triple).unwrap();
        } else {
            pg.delete_triple(date, &triple).unwrap();
        }
    };
    at("2020-01-01", "carol", "acme", true);
    at("2021-01-01", "dave", "initech", true);
    at("2021-06-01", "alice", "acme", true);
    at("2022-03-01", "bob", "acme", true);
    at("2022-06-01", "carol", "acme", false);
    at("2022-06-01", "carol", "globex", true);
    at("2022-12-01", "erin", "acme", true);
    at("2023-06-01", "alice", "acme", false);
    at("2023-06-01", "alice", "initech", true);
    at("2023-09-01", "dave", "initech", false);
    at("2023-09-01", "dave", "acme", true);
    pg
}

fn ex(local: &str) -> String {
    format!("<http://ex/{local}>")
}

/// Queries compare dates in one query with time graphs, as in the time travel guide.
#[test]
fn comparing_dates_in_one_query() {
    let pg = employment();
    // the dates are the times of the history
    assert_eq!(
        time_graph("2023-01-01").at,
        "2023-01-01".try_into_time().unwrap().t()
    );

    // who worked for acme on 2023-01-01, and for someone else on 2024-01-01
    let moved = "SELECT ?p ?new WHERE {
        GRAPH raphtory:asof:2023-01-01 { ?p <http://ex/worksFor> <http://ex/acme> }
        GRAPH raphtory:asof:2024-01-01 {
            ?p <http://ex/worksFor> ?new FILTER(?new != <http://ex/acme>)
        }
    }";
    assert_eq!(select(&pg, moved), vec![vec![ex("alice"), ex("initech")]]);
    // who joined acme in 2023
    let joined = "SELECT ?p {
        GRAPH raphtory:asof:2024-01-01 { ?p <http://ex/worksFor> <http://ex/acme> }
        FILTER NOT EXISTS {
            GRAPH raphtory:asof:2023-01-01 { ?p <http://ex/worksFor> <http://ex/acme> }
        }
    }";
    assert_eq!(select(&pg, joined), vec![vec![ex("dave")]]);
    // who left acme in 2023
    let left = "SELECT ?p {
        GRAPH raphtory:asof:2023-01-01 { ?p <http://ex/worksFor> <http://ex/acme> }
        MINUS { GRAPH raphtory:asof:2024-01-01 { ?p <http://ex/worksFor> <http://ex/acme> } }
    }";
    assert_eq!(select(&pg, left), vec![vec![ex("alice")]]);
    // the current state (default graph) next to a time graph
    let previous = "SELECT ?p ?old {
        ?p <http://ex/worksFor> <http://ex/acme>
        OPTIONAL { GRAPH raphtory:asof:2022-01-01 { ?p <http://ex/worksFor> ?old } }
    }";
    assert_eq!(
        select(&pg, previous),
        vec![
            vec![ex("bob"), "UNDEF".to_owned()],
            vec![ex("dave"), ex("initech")],
            vec![ex("erin"), "UNDEF".to_owned()],
        ]
    );

    // the head count of acme at each date, with `FROM NAMED`
    let series = "SELECT ?g (COUNT(*) AS ?n)
        FROM NAMED raphtory:asof:2022-01-01 FROM NAMED raphtory:asof:2023-01-01
        WHERE { GRAPH ?g { ?x <http://ex/worksFor> <http://ex/acme> } } GROUP BY ?g";
    let n = |n: u32| format!("\"{n}\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    assert_eq!(
        select(&pg, series),
        vec![
            vec![asof("2022-01-01"), n(2)],
            vec![asof("2023-01-01"), n(3)],
        ]
    );
    // ... and who it is
    let who = "SELECT ?g ?x
        FROM NAMED raphtory:asof:2022-01-01
        FROM NAMED raphtory:asof:2023-01-01
        FROM NAMED raphtory:asof:2024-01-01
        WHERE { GRAPH ?g { ?x <http://ex/worksFor> <http://ex/acme> } }";
    assert_eq!(
        select(&pg, who),
        vec![
            vec![asof("2022-01-01"), ex("alice")],
            vec![asof("2022-01-01"), ex("carol")],
            vec![asof("2023-01-01"), ex("alice")],
            vec![asof("2023-01-01"), ex("bob")],
            vec![asof("2023-01-01"), ex("erin")],
            vec![asof("2024-01-01"), ex("bob")],
            vec![asof("2024-01-01"), ex("dave")],
            vec![asof("2024-01-01"), ex("erin")],
        ]
    );
    // `FROM NAMED` without `FROM`: the default graph is empty
    assert!(!ask(
        &pg,
        "ASK FROM NAMED raphtory:asof:2023-01-01 { ?s ?p ?o }"
    ));
    // one time graph as the default graph
    assert_eq!(
        select(
            &pg,
            "SELECT ?x FROM raphtory:asof:2022-01-01 { ?x <http://ex/worksFor> <http://ex/acme> }"
        ),
        vec![vec![ex("alice")], vec![ex("carol")]]
    );
    // several: their union, with DISTINCT
    assert_eq!(
        select(
            &pg,
            "SELECT DISTINCT ?x FROM raphtory:asof:2022-01-01 FROM raphtory:asof:2024-01-01
            { ?x <http://ex/worksFor> <http://ex/acme> }"
        ),
        vec![
            vec![ex("alice")],
            vec![ex("bob")],
            vec![ex("carol")],
            vec![ex("dave")],
            vec![ex("erin")],
        ]
    );
    // spareval concatenates the `FROM` graphs instead of merging them, so a triple visible in
    // several is matched once per graph (pinned so a spareval upgrade that changes it is noticed)
    let two_dates = "FROM raphtory:asof:2022-01-01 FROM raphtory:asof:2023-01-01";
    let acme = "{ ?x <http://ex/worksFor> <http://ex/acme> }";
    assert_eq!(
        select(&pg, &format!("SELECT ?x {two_dates} {acme}")),
        vec![
            vec![ex("alice")],
            vec![ex("alice")],
            vec![ex("bob")],
            vec![ex("carol")],
            vec![ex("erin")],
        ]
    );
    assert_eq!(
        select(&pg, &format!("SELECT DISTINCT ?x {two_dates} {acme}")),
        vec![
            vec![ex("alice")],
            vec![ex("bob")],
            vec![ex("carol")],
            vec![ex("erin")],
        ]
    );
    assert_eq!(
        select(
            &pg,
            &format!(
                "SELECT (COUNT(*) AS ?all) (COUNT(DISTINCT ?x) AS ?distinct) {two_dates} {acme}"
            )
        ),
        vec![vec![n(5), n(4)]]
    );
    // `CONSTRUCT` builds a set, so it is not affected
    let SparqlResults::Graph(constructed) = pg
        .sparql(&format!(
            "CONSTRUCT {{ ?x <http://ex/worksFor> <http://ex/acme> }} {two_dates} WHERE {acme}"
        ))
        .unwrap()
    else {
        panic!("not a graph")
    };
    assert_eq!(constructed.len(), 4);
    // with `FROM` the named graphs are the `FROM NAMED` ones only (here: none)
    assert!(!ask(
        &pg,
        "ASK FROM raphtory:asof:2022-01-01 {
            GRAPH raphtory:asof:2022-01-01 { ?s ?p ?o }
        }"
    ));
    assert!(ask(
        &pg,
        "ASK FROM raphtory:asof:2022-01-01 FROM NAMED raphtory:asof:2024-01-01 {
            ?p <http://ex/worksFor> <http://ex/acme>
            GRAPH raphtory:asof:2024-01-01 { ?p <http://ex/worksFor> <http://ex/initech> }
        }"
    ));
}

/// The forms of a time resolve to the same instant, but are different graphs.
#[test]
fn time_forms() {
    const INSTANT: i64 = 1_704_067_200_000;
    let forms = [
        "1704067200000",
        "2024-01-01",
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:00.000",
        "2024-01-01T00:00:00",
        "2024-01-01T01:00:00+01:00",
        "+1704067200000",
    ];
    for form in forms {
        let graph = time_graph(form);
        assert_eq!(graph.at, INSTANT, "{form}");
        assert_eq!(graph.iri.as_str(), format!("raphtory:asof:{form}"));
    }
    assert_eq!(time_graph("-5").at, -5);
    assert_eq!(time_graph("0").at, 0);
    assert_eq!(time_graph("2024-01-01T00:00:00.001Z").at, INSTANT + 1);
    // not time graphs
    for other in [
        "raphtory:asof",
        "raphtory:asof%3A1",
        "raphtory:Asof:1",
        "raphtory:asofx:1",
        "http://ex/asof:1",
        "raphtory:raphtory:asof:1",
    ] {
        assert_eq!(TimeGraph::parse(&iri(other)).unwrap(), None, "{other}");
    }

    let pg = PersistentGraph::new();
    assert_at(&pg, INSTANT - 1, [S, P, O]);
    assert_at(&pg, INSTANT, [S, Q, O]);
    assert_at(&pg, INSTANT + 1, [O, P, S]);
    let expected = BTreeSet::from([nt([S, P, O]), nt([S, Q, O])]);
    for form in forms {
        assert_eq!(in_graph(&pg, form), expected, "{form}");
        assert_eq!(from_graph(&pg, form), expected, "{form}");
    }

    // different IRIs are different terms
    let dataset = RaphtoryDataset::new(pg.clone());
    let internal: Vec<RdfTerm> = forms
        .iter()
        .map(|form| {
            let term: Term = iri(&format!("{ASOF_NS}{form}")).into();
            let internal = dataset.internalize_term(term.clone()).unwrap();
            assert_eq!(
                internal,
                RdfTerm::TimeGraph(Arc::new(time_graph(form))),
                "{form}"
            );
            assert_eq!(dataset.externalize_term(internal.clone()).unwrap(), term);
            internal
        })
        .collect();
    let distinct: BTreeSet<String> = internal.iter().map(|t| format!("{t:?}")).collect();
    assert_eq!(distinct.len(), forms.len());
    assert!(!ask(
        &pg,
        "ASK { FILTER(sameTerm(raphtory:asof:2024-01-01, <raphtory:asof:1704067200000>)) }"
    ));
    assert!(ask(
        &pg,
        "ASK { VALUES (?a ?b) { (raphtory:asof:2024-01-01 <raphtory:asof:2024-01-01>) }
            FILTER(sameTerm(?a, ?b)) }"
    ));
    // ... so each is its own group
    let named: String = forms
        .iter()
        .map(|form| format!("FROM NAMED {} ", asof(form)))
        .collect();
    let counts = select(
        &pg,
        &format!("SELECT ?g (COUNT(*) AS ?n) {named} {{ GRAPH ?g {{ ?s ?p ?o }} }} GROUP BY ?g"),
    );
    let two = "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned();
    let mut expected: Vec<Vec<String>> = forms
        .iter()
        .map(|form| vec![asof(form), two.clone()])
        .collect();
    expected.sort();
    assert_eq!(counts, expected);
}

/// Invalid time graphs are errors.
#[test]
fn invalid_time_graphs() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    for bad in [
        "garbage",
        "",
        "2024-13-01",
        "2024-01-01T25:00:00Z",
        "1.5",
        "99999999999999999999",
        "2024-01-01T00:00:00ZZ",
    ] {
        let graph = format!("{ASOF_NS}{bad}");
        let err = TimeGraph::parse(&iri(&graph)).unwrap_err();
        let RdfError::InvalidTimeGraph { iri: name, reason } = &err else {
            panic!("{bad}: {err:?}")
        };
        assert_eq!(name, &graph);
        assert!(reason.contains(&format!("'{bad}'")), "{reason}");
        assert!(
            err.to_string()
                .starts_with(&format!("invalid time graph <{graph}>: ")),
            "{err}"
        );
        // ... and abort a query that uses them in a pattern, a dataset clause, VALUES or BIND
        let built = format!("SELECT ?g {{ BIND(IRI(\"{graph}\") AS ?g) }}");
        let graph = format!("<{graph}>");
        for query in [
            built,
            format!("SELECT * {{ GRAPH {graph} {{ ?s ?p ?o }} }}"),
            format!("SELECT * FROM {graph} {{ ?s ?p ?o }}"),
            format!("SELECT * FROM NAMED {graph} {{ GRAPH ?g {{ ?s ?p ?o }} }}"),
            format!("SELECT * {{ VALUES ?g {{ {graph} }} GRAPH ?g {{ ?s ?p ?o }} }}"),
            format!("SELECT * {{ {graph} ?p ?o }}"),
            format!("ASK {{ ?s ?p {graph} }}"),
        ] {
            assert_eq!(
                invalid_time_graph(&pg, &query),
                graph.trim_matches(['<', '>']),
                "{query}"
            );
        }
    }
    let internal = RaphtoryDataset::new(pg.clone())
        .internalize_term(iri("raphtory:asof:garbage").into())
        .unwrap_err();
    assert!(matches!(internal, RdfError::InvalidTimeGraph { .. }));
}

/// Time graphs cannot be enumerated: `GRAPH ?g` visits the ones the query names.
#[test]
fn time_graphs_are_not_enumerated() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 3, [S, Q, O]);
    // without `FROM NAMED`, `GRAPH ?g` finds no graph
    assert!(select(&pg, "SELECT * { GRAPH ?g { ?s ?p ?o } }").is_empty());
    assert!(select(&pg, "SELECT ?g { GRAPH ?g { } }").is_empty());
    assert!(!ask(&pg, "ASK { GRAPH ?g { } }"));
    // ... but every time graph exists
    assert!(ask(&pg, "ASK { GRAPH raphtory:asof:0 { } }"));
    assert!(ask(&pg, "ASK { GRAPH <raphtory:asof:-100> { } }"));
    assert!(!ask(&pg, "ASK { GRAPH <http://ex/g> { } }"));
    assert!(!ask(&pg, "ASK { GRAPH raphtory:asof:0 { ?s ?p ?o } }"));
    assert!(ask(&pg, "ASK { GRAPH raphtory:asof:1 { ?s ?p ?o } }"));
    // ... but the time graphs the query names are listed, so `GRAPH ?g` visits them wherever it
    // is and whatever binds `?g` (spareval may evaluate `GRAPH ?g` with `?g` unbound, then join)
    let bound = vec![
        vec![asof(2), format!("<{P}>")],
        vec![asof(4), format!("<{P}>")],
        vec![asof(4), format!("<{Q}>")],
    ];
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?p { VALUES ?g { raphtory:asof:2 raphtory:asof:4 } GRAPH ?g { ?s ?p ?o } }"
        ),
        bound
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?p { GRAPH ?g { ?s ?p ?o } VALUES ?g { raphtory:asof:2 raphtory:asof:4 } }"
        ),
        bound
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?p { VALUES ?g { raphtory:asof:2 raphtory:asof:4 }
                GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }"
        ),
        bound
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?q { BIND(raphtory:asof:2 AS ?g) GRAPH ?g { ?s ?q ?o } }"
        ),
        vec![vec![asof(2), format!("<{P}>")]]
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?q { BIND(IRI(\"raphtory:asof:2\") AS ?g) GRAPH ?g { ?s ?q ?o } }"
        ),
        vec![vec![asof(2), format!("<{P}>")]]
    );
    assert_eq!(
        select(
            &pg,
            "SELECT ?g ?p { GRAPH ?g { ?s ?p ?o } FILTER(?g = raphtory:asof:4) }"
        ),
        bound[1..]
    );
    let two = "VALUES ?g { raphtory:asof:0 raphtory:asof:2 }";
    assert_eq!(
        select(
            &pg,
            &format!("SELECT ?g ?p {{ {two} OPTIONAL {{ GRAPH ?g {{ ?s ?p ?o }} }} }}")
        ),
        vec![
            vec![asof(0), "UNDEF".to_owned()],
            vec![asof(2), format!("<{P}>")],
        ]
    );
    let exists = |filter: &str| {
        select(
            &pg,
            &format!("SELECT ?g {{ {two} FILTER {filter} {{ GRAPH ?g {{ ?s ?p ?o }} }} }}"),
        )
    };
    assert_eq!(exists("EXISTS"), vec![vec![asof(2)]]);
    assert_eq!(exists("NOT EXISTS"), vec![vec![asof(0)]]);
    // ... also inside `MINUS` and a sub-`SELECT`, which spareval evaluates on their own
    let minus = format!("SELECT ?g {{ {two} MINUS {{ GRAPH ?g {{ ?s ?p ?o }} }} }}");
    assert_eq!(select(&pg, &minus), vec![vec![asof(0)]]);
    let sub_select = "{ VALUES ?g { raphtory:asof:2 } { SELECT ?g ?p { GRAPH ?g { ?s ?p ?o } } } }";
    let in_2 = vec![vec![asof(2), format!("<{P}>")]];
    assert_eq!(select(&pg, &format!("SELECT ?g ?p {sub_select}")), in_2);
    // with `FROM NAMED` they agree with the other forms
    let named = "FROM NAMED raphtory:asof:0 FROM NAMED raphtory:asof:2";
    assert_eq!(
        select(
            &pg,
            &format!("SELECT ?g {named} {{ {two} MINUS {{ GRAPH ?g {{ ?s ?p ?o }} }} }}")
        ),
        vec![vec![asof(0)]]
    );
    assert_eq!(
        select(&pg, &format!("SELECT ?g ?p {named} {sub_select}")),
        in_2
    );
    // a time graph built from other values is not listed, so `GRAPH ?g` matches it only where
    // `?g` is already bound; here spareval evaluates it with `?g` unbound and finds nothing ...
    let built = "VALUES ?t { 2 } BIND(IRI(CONCAT(\"raphtory:asof:\", STR(?t))) AS ?g)";
    let optional = "GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } }";
    assert!(select(&pg, &format!("SELECT ?s {{ {built} {optional} }}")).is_empty());
    // ... while `LATERAL` always evaluates it after the binding
    assert_eq!(
        select(
            &pg,
            &format!("SELECT ?s {{ {built} LATERAL {{ {optional} }} }}")
        ),
        vec![vec![format!("<{S}>")]]
    );
    // `FROM NAMED` restricts `GRAPH` to the graphs it names
    assert!(!ask(
        &pg,
        "ASK FROM NAMED raphtory:asof:2 { GRAPH raphtory:asof:4 { } }"
    ));
    assert!(ask(
        &pg,
        "ASK FROM NAMED raphtory:asof:2 { GRAPH raphtory:asof:2 { } }"
    ));
    // a time graph is not a node or a predicate
    assert!(!ask(&pg, "ASK { raphtory:asof:2 ?p ?o }"));
    assert!(!ask(&pg, "ASK { ?s raphtory:asof:2 ?o }"));
    assert!(!ask(&pg, "ASK { ?s ?p raphtory:asof:2 }"));
    // ... but a value like any other
    assert_eq!(
        select(&pg, "SELECT ?g { VALUES ?g { raphtory:asof:2 } }"),
        vec![vec![asof(2)]]
    );
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
    // not constant time graphs: strings, IRIs built from other values, invalid times, other IRIs
    assert_eq!(
        named(
            "SELECT * {
                BIND(\"raphtory:asof:1\" AS ?a)
                BIND(IRI(CONCAT(\"raphtory:asof:\", \"2\")) AS ?b)
                BIND(\"3\"^^raphtory:asof:3 AS ?c)
                BIND(raphtory:asof:garbage AS ?d)
                BIND(IRI(\"raphtory:asof:x\") AS ?e)
                BIND(<http://ex/g> AS ?f)
                BIND(IRI(\"raphtory:asof:4\"@en) AS ?h)
            }"
        ),
        asof(&[])
    );
}

/// Time graphs of a windowed view intersect with its window.
#[test]
fn time_graphs_of_windows() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 4, SPO);
    assert_at(&pg, 6, SPO);
    assert_at(&pg, 2, [S, Q, O]);
    retract_at(&pg, 2, [S, Q, O]);
    retract_at(&pg, 3, [S, P, O2]);
    assert_at(&pg, 5, [S, P, O2]);
    retract_at(&pg, 8, [S, P, O2]);
    assert_at(&pg, 0, [O, P, S]);
    assert_at(&pg, 7, [O, P, S]);
    let as_of: Vec<_> = (-2..=10)
        .map(|t| (t, triples(&pg.snapshot_at(t))))
        .collect();
    let mut seen = BTreeSet::new();
    for a in -1..8 {
        for b in a + 1..=9 {
            let window = pg.window(a, b);
            for (t, expected) in &as_of {
                let expected = if (a..b).contains(t) {
                    seen.insert(expected.len());
                    expected.clone()
                } else {
                    BTreeSet::new()
                };
                assert_eq!(in_graph(&window, t), expected, "[{a}, {b}) at {t}");
            }
        }
    }
    // the history is not trivial
    assert!(seen.len() >= 3, "{seen:?}");
    // other windows: `before(5)` is `[-inf, 5)`, `after(5)` is `[6, inf)`, `snapshot_at(5)` is
    // `[5, 6)` and `snapshot_latest()` is `[8, 9)` (8 is the latest time)
    let empty = BTreeSet::new();
    for (t, expected) in &as_of {
        let only = |inside: bool| if inside { expected } else { &empty };
        assert_eq!(
            &in_graph(&pg.before(5), t),
            only(*t < 5),
            "before(5) at {t}"
        );
        assert_eq!(&in_graph(&pg.after(5), t), only(*t > 5), "after(5) at {t}");
        assert_eq!(
            &in_graph(&pg.snapshot_at(5), t),
            only(*t == 5),
            "snapshot_at(5) at {t}"
        );
        assert_eq!(
            &in_graph(&pg.snapshot_latest(), t),
            only(*t == 8),
            "snapshot_latest() at {t}"
        );
    }
}

/// On an event graph a time graph holds the triples asserted at or before its time.
#[test]
fn time_graphs_of_event_graphs() {
    let g = Graph::new();
    assert_at(&g, 1, SPO);
    retract_at(&g, 5, SPO);
    assert_at(&g, 7, [S, Q, O]);
    for t in -1..=12 {
        let mut expected = BTreeSet::new();
        if t >= 1 {
            expected.insert(nt(SPO));
        }
        if t >= 7 {
            expected.insert(nt([S, Q, O]));
        }
        assert_eq!(in_graph(&g, t), expected, "t = {t}");
        assert_eq!(from_graph(&g, t), expected, "t = {t}");
        // `snapshot_at` on a dynamic event graph is `before(t + 1)`
        let dynamic = g.clone().into_dynamic();
        assert_eq!(triples(&dynamic.snapshot_at(t)), expected, "t = {t}");
        assert_eq!(triples(&g.before(t + 1)), expected, "t = {t}");
        assert_eq!(in_graph(&dynamic, t), expected, "t = {t}");
        // a window starting after the assertion (a time at or after the end of the window, 10,
        // is not empty: it is every triple asserted in the window)
        let late: BTreeSet<_> = expected
            .iter()
            .filter(|triple| *triple != &nt(SPO))
            .cloned()
            .collect();
        assert_eq!(in_graph(&g.window(2, 10), t), late, "t = {t}");

        // the persistent view of the same storage is as of `t`
        let pg = g.persistent_graph();
        let mut expected = BTreeSet::new();
        if (1..5).contains(&t) {
            expected.insert(nt(SPO));
        }
        if t >= 7 {
            expected.insert(nt([S, Q, O]));
        }
        assert_eq!(in_graph(&pg, t), expected, "persistent t = {t}");
        // `snapshot_at` on a dynamic persistent graph is as of `t`
        let dynamic = pg.clone().into_dynamic();
        assert_eq!(triples(&dynamic.snapshot_at(t)), expected, "t = {t}");
        assert_eq!(in_graph(&dynamic, t), expected, "t = {t}");
    }
    // Windows intersect: on an event graph a time at or after the end of the window gives
    // every triple asserted in the window, while on a persistent graph it gives an empty graph.
    let window = g.window(2, 10);
    let in_window = BTreeSet::from([nt([S, Q, O])]);
    for t in [10, 20, i64::MAX] {
        assert_eq!(in_graph(&window, t), in_window, "t = {t}");
    }
    assert_eq!(in_graph(&window, 1), BTreeSet::new());
    let window = g.persistent_graph().window(2, 10);
    assert_eq!(in_graph(&window, 9), in_window);
    for t in [1, 10, 20, i64::MAX] {
        assert_eq!(in_graph(&window, t), BTreeSet::new(), "persistent t = {t}");
    }
}

/// Time graphs compose with the other views of the base, and never collide with names.
#[test]
fn time_graphs_of_other_views() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 1, [S, Q, O]);
    assert_at(&pg, 1, [O, P, O2]);
    retract_at(&pg, 3, SPO);
    assert_eq!(
        in_graph(&pg.layers(P).unwrap(), 2),
        BTreeSet::from([nt(SPO), nt([O, P, O2])])
    );
    assert_eq!(
        in_graph(&pg.layers(P).unwrap(), 3),
        BTreeSet::from([nt([O, P, O2])])
    );
    assert_eq!(
        in_graph(&pg.subgraph([S, O]), 2),
        BTreeSet::from([nt(SPO), nt([S, Q, O])])
    );
    assert_eq!(
        in_graph(&pg.exclude_nodes([O2]), 3),
        BTreeSet::from([nt([S, Q, O])])
    );

    // nodes and layers named like time graphs are other terms (`asof:5` is an absolute IRI)
    let g = PersistentGraph::new();
    g.add_edge(
        1,
        "asof:5",
        "raphtory:asof:5",
        NO_PROPS,
        Some("raphtory:asof:5"),
    )
    .unwrap();
    g.add_edge(9, "a", "b", NO_PROPS, None).unwrap();
    for name in ["asof:5", "raphtory:asof:5"] {
        let term = term_of(name);
        assert_eq!(name_of(term.as_ref()), Some(name.to_owned()));
        assert!(
            !term.to_string().starts_with(&format!("<{ASOF_NS}")),
            "{term}"
        );
    }
    assert_eq!(name_of(iri("raphtory:asof:5").as_ref().into()), None);
    assert_eq!(
        select(&g, "SELECT ?s ?p ?o { ?s ?p ?o }"),
        vec![
            vec![
                "<asof:5>".to_owned(),
                "<raphtory:raphtory%3Aasof%3A5>".to_owned(),
                "<raphtory:raphtory%3Aasof%3A5>".to_owned()
            ],
            vec![
                "<raphtory:a>".to_owned(),
                "<raphtory:_default>".to_owned(),
                "<raphtory:b>".to_owned()
            ],
        ]
    );
    // `<raphtory:asof:5>` is the graph as of 5, which does not have `a -> b` yet
    assert_eq!(
        select(&g, "SELECT ?s ?p ?o { GRAPH raphtory:asof:5 { ?s ?p ?o } }"),
        vec![vec![
            "<asof:5>".to_owned(),
            "<raphtory:raphtory%3Aasof%3A5>".to_owned(),
            "<raphtory:raphtory%3Aasof%3A5>".to_owned()
        ]]
    );
    assert!(ask(
        &g,
        "ASK { GRAPH raphtory:asof:5 { <asof:5> <raphtory:raphtory%3Aasof%3A5> ?o } }"
    ));
    let dataset = RaphtoryDataset::new(g.clone());
    assert!(matches!(
        dataset
            .internalize_term(iri("raphtory:asof:5").into())
            .unwrap(),
        RdfTerm::TimeGraph(_)
    ));
    assert!(matches!(
        dataset
            .internalize_term(iri("raphtory:raphtory%3Aasof%3A5").into())
            .unwrap(),
        RdfTerm::Node(_)
    ));
}

/// The internal terms of time graphs, and the quads and views of the dataset.
#[test]
fn time_graph_internals() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 1, [S, Q, O]);
    retract_at(&pg, 3, SPO);
    let dataset = RaphtoryDataset::new(pg.clone());
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
    assert_eq!(dataset.num_cached_views(), 0);
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
    let now = quads(None, None, Some(None));
    assert_eq!(now.len(), 1);
    assert_eq!(now[0].graph_name, None);
    // ... and the time graph is not a node, so it is never a subject
    assert!(quads(Some(&at_2), None, Some(Some(&at_2))).is_empty());
    assert!(quads(None, Some(&at_2), Some(None)).is_empty());

    // views are cached by time and layer, whatever the spelling of the time
    assert_eq!(dataset.num_cached_views(), 3);
    assert_eq!(quads(None, None, Some(Some(&also_at_2))).len(), 2);
    assert_eq!(quads(Some(&s), None, Some(Some(&at_2))).len(), 2);
    assert_eq!(dataset.num_cached_views(), 3);
    assert_eq!(quads(None, Some(&p), Some(Some(&at_2))).len(), 1);
    assert_eq!(quads(None, Some(&p), Some(Some(&also_at_2))).len(), 1);
    assert!(quads(None, Some(&p), Some(Some(&at_5))).is_empty());
    assert_eq!(dataset.num_cached_views(), 5);

    // externalize gives back the IRI as written
    assert_eq!(
        dataset.externalize_term(also_at_2.clone()).unwrap(),
        Term::from(iri("raphtory:asof:+2"))
    );

    // a dataset lists the time graphs it is given, in order and once per IRI ...
    let listed = RaphtoryDataset::new(pg.clone())
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
    use crate::rdf::{QueryResultsFormat, SparqlDataset, SparqlOptions};

    // `alice knows bob` holds from 1 to 5, `bob knows carol` from 3 on
    let pg = PersistentGraph::new();
    let knows = |s: &str, o: &str| format!("<http://ex/{s}> <http://ex/knows> <http://ex/{o}> .");
    let load = |t: i64, doc: String| pg.load_rdf(t, doc.as_bytes(), RdfFormat::NTriples, None);
    load(1, knows("alice", "bob")).unwrap();
    load(3, knows("bob", "carol")).unwrap();
    pg.retract_rdf(
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
        let SparqlResults::Solutions { rows, .. } = pg.sparql_with(query, options).unwrap() else {
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

    assert_eq!(subjects(default, &SparqlOptions::default()), s(&["bob"]));
    assert_eq!(
        subjects(default, &with(&["raphtory:asof:2"], &[])),
        s(&["alice"])
    );
    // it replaces `FROM`, and several default graphs are concatenated
    let from_4 = "SELECT ?s FROM raphtory:asof:4 { ?s ?p ?o }";
    assert_eq!(
        subjects(from_4, &SparqlOptions::default()),
        s(&["alice", "bob"])
    );
    assert_eq!(
        subjects(from_4, &with(&["raphtory:asof:2"], &[])),
        s(&["alice"])
    );
    assert_eq!(
        subjects(default, &with(&["raphtory:asof:2", "raphtory:asof:4"], &[])),
        s(&["alice", "alice", "bob"])
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

    // serialized results use it too
    let mut csv = Vec::new();
    pg.sparql_to_writer_with(
        default,
        &mut csv,
        QueryResultsFormat::Csv,
        &with(&["raphtory:asof:2"], &[]),
    )
    .unwrap();
    assert_eq!(String::from_utf8(csv).unwrap(), "s\r\nhttp://ex/alice\r\n");

    // a time graph whose time does not parse fails the query
    let error = pg
        .sparql_with(default, &with(&["raphtory:asof:soon"], &[]))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("invalid time graph <raphtory:asof:soon>"),
        "{error}"
    );
}
