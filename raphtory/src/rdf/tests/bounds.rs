//! Bounding the work of a query: `SparqlOptions::max_triple_patterns`, `timeout` and
//! `cancellation_token`.
use super::within_60s;
use crate::{
    db::api::view::IntoDynamic,
    errors::GraphError,
    prelude::*,
    rdf::{
        dataset::RaphtoryDataset,
        limits::{count_triple_patterns, run_interruptible, Interrupt},
        model::{Literal, Term},
        query::parse,
        scan::EdgeScan,
        CancellationToken, QueryResultsFormat, RdfError, SparqlOptions, SparqlResults,
    },
};
use spareval::QueryableDataset;
use std::{
    io,
    panic::{self, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

/// The number of triple patterns of `query`.
fn patterns(query: &str) -> usize {
    count_triple_patterns(&parse(query).unwrap_or_else(|e| panic!("{query}: {e}")))
}

#[test]
fn every_construct_counts_its_triple_patterns() {
    let cases: &[(&str, usize)] = &[
        ("ASK {}", 0),
        ("ASK { ?s ?p ?o . ?o ?q ?r . ?r ?t ?u }", 3),
        // collections are triple patterns once parsed
        ("ASK { ?s ?p (1 2 3) }", 7),
        // property paths: one per predicate they name
        ("ASK { ?s (raphtory:a/^raphtory:b)*|raphtory:c ?o }", 3),
        // VALUES: one per block, plus one per variable and per 100 rows
        ("ASK { VALUES (?x ?y) { (1 2) (3 UNDEF) } ?x ?p ?y }", 4),
        // BIND and the expressions of SELECT and GROUP BY: one each
        ("ASK { BIND(1 AS ?x) BIND(2 AS ?y) }", 2),
        (
            "SELECT ?x (COUNT(*) AS ?n) { ?s ?p ?o } GROUP BY (STR(?s) AS ?x)",
            3,
        ),
        // EXISTS wherever an expression is
        (
            "ASK { ?s ?p ?o OPTIONAL { ?o ?q ?r FILTER EXISTS { ?r ?t ?u } } }",
            3,
        ),
        (
            "ASK { ?s ?p ?o FILTER(?o = 1 || (EXISTS { ?o ?q ?r } && !EXISTS { ?s ?q ?r })) }",
            3,
        ),
        // sub-queries, with their modifiers
        (
            "SELECT * { ?s ?p ?o { SELECT DISTINCT ?o { ?o ?q ?r . ?r ?t ?u } ORDER BY ?o LIMIT 2 } }",
            3,
        ),
        // the CONSTRUCT template does not count
        (
            "CONSTRUCT { ?s ?p ?o . ?o ?p ?s . ?s ?p 1 } WHERE { ?s ?p ?o }",
            1,
        ),
        // the IRI is bound to a variable, as by BIND
        ("DESCRIBE raphtory:a", 1),
    ];
    for &(query, expected) in cases {
        assert_eq!(patterns(query), expected, "{query}");
    }
    // rows of VALUES
    let values = |rows: usize| {
        let rows: String = (0..rows).map(|i| format!("({i} {i}) ")).collect();
        format!("ASK {{ VALUES (?x ?y) {{ {rows} }} }}")
    };
    for (rows, expected) in [(99, 3), (100, 4)] {
        assert_eq!(patterns(&values(rows)), expected, "{rows} rows");
    }
}

fn small_graph() -> RdfGraph {
    let g = Graph::new();
    for i in 0..20 {
        g.add_edge(i, i.to_string(), (i + 1).to_string(), NO_PROPS, Some("p"))
            .unwrap();
    }
    g.into()
}

fn max_patterns(max: usize) -> SparqlOptions {
    SparqlOptions::default().with_max_triple_patterns(max)
}

fn star(n: usize) -> String {
    let patterns: String = (0..n).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
    format!("SELECT ?s {{ {patterns} }}")
}

/// A query within the limit runs; one beyond it is rejected before anything is written.
#[test]
fn queries_with_too_many_triple_patterns_are_rejected() {
    let g = small_graph();
    assert!(g.sparql_with(&star(5), &max_patterns(5)).is_ok());
    match g.sparql_with(&star(6), &max_patterns(5)) {
        Err(GraphError::Rdf(RdfError::TooManyPatterns { count, max })) => {
            assert_eq!((count, max), (6, 5))
        }
        other => panic!("{other:?}"),
    }
    let mut out = Vec::new();
    let error = g
        .sparql_to_writer_with(
            &star(6),
            &mut out,
            QueryResultsFormat::Json,
            &max_patterns(5),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::Rdf(RdfError::TooManyPatterns { count: 6, max: 5 })
    ));
    assert!(out.is_empty());
    // without a limit, any number
    assert!(g.sparql(&star(101)).is_ok());
    assert!(g.sparql_with(&star(101), &SparqlOptions::default()).is_ok());
}

/// About 20^8 solutions on `small_graph`, joined in memory.
const CUBE: &str = "?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l . \
                    ?m ?n ?o . ?p ?q ?r . ?s ?t ?u . ?v ?w ?x";

fn timeout(ms: u64) -> SparqlOptions {
    SparqlOptions::default().with_timeout(Duration::from_millis(ms))
}

/// Runs `f` (a query with a 200 ms timeout), which must fail with `RdfError::Timeout` well
/// before it could finish.
fn times_out<R: std::fmt::Debug + Send + 'static>(
    f: impl FnOnce() -> Result<R, GraphError> + Send + 'static,
) {
    let (error, elapsed) = within_60s(move || {
        let start = Instant::now();
        let result = f();
        (result, start.elapsed())
    });
    match error {
        Err(GraphError::Rdf(RdfError::Timeout { timeout })) => {
            assert_eq!(timeout, Duration::from_millis(200))
        }
        other => panic!("{other:?}"),
    }
    assert!(elapsed >= Duration::from_millis(200), "{elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(10),
        "stopped late: {elapsed:?}"
    );
}

#[test]
fn slow_queries_time_out() {
    let g = small_graph();
    // a count that returns nothing until it is done, built from solutions in memory
    let view = g.clone();
    times_out(move || {
        view.sparql_with(
            &format!("SELECT (COUNT(*) AS ?n) {{ {CUBE} }}"),
            &timeout(200),
        )
    });
    // the same with VALUES alone, which reads nothing from the graph
    let values: String = (0..40)
        .map(|i| format!("VALUES ?v{i} {{ 1 2 }} "))
        .collect();
    let view = g.clone();
    times_out(move || {
        view.sparql_with(
            &format!("SELECT (COUNT(*) AS ?n) {{ {values} }}"),
            &timeout(200),
        )
    });
    // streamed results
    let view = g.clone();
    times_out(move || {
        view.sparql_to_writer_with(
            &format!("SELECT ?a {{ {CUBE} }}"),
            io::sink(),
            QueryResultsFormat::Csv,
            &timeout(200),
        )
    });
    // a property path over a long chain, evaluated from every node
    let chain = Graph::new();
    for i in 0..2_000 {
        chain
            .add_edge(i, i.to_string(), (i + 1).to_string(), NO_PROPS, Some("p"))
            .unwrap();
    }
    let chain = RdfGraph::from(chain);
    times_out(move || {
        chain.sparql_with(
            "SELECT (COUNT(*) AS ?n) { ?a raphtory:p* ?b . ?b raphtory:p* ?c }",
            &timeout(200),
        )
    });
}

#[test]
fn fast_queries_do_not_time_out() {
    let g = small_graph();
    let options = SparqlOptions::default().with_timeout(Duration::from_secs(60));
    for query in [
        "SELECT * { ?s ?p ?o }",
        "CONSTRUCT { ?o ?p ?s } WHERE { ?s ?p ?o }",
    ] {
        assert_eq!(
            g.sparql_with(query, &options).unwrap(),
            g.sparql(query).unwrap(),
            "{query}"
        );
    }
    // a zero timeout stops any query
    assert!(matches!(
        g.sparql_with("ASK {}", &timeout(0)),
        Err(GraphError::Rdf(RdfError::Timeout { .. }))
    ));
}

/// A writer is not blocked while a query runs or is cancelled.
#[test]
fn writers_are_not_blocked_by_a_query_that_is_stopped() {
    let g = Graph::new();
    for i in 0..2_000 {
        g.add_edge(i, i.to_string(), (i + 1).to_string(), NO_PROPS, Some("p"))
            .unwrap();
    }
    let token = CancellationToken::new();
    let options = SparqlOptions::default().with_cancellation_token(token.clone());
    // reads the graph for as long as it runs: paths from every node of the chain
    let view = RdfGraph::from(g.clone());
    let query = thread::spawn(move || {
        view.sparql_with(
            "SELECT (COUNT(*) AS ?n) { ?a raphtory:p* ?b . ?b raphtory:p* ?c }",
            &options,
        )
    });
    // writes to the nodes the query reads
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (g, stop, writes) = (g.clone(), stop.clone(), writes.clone());
        thread::spawn(move || {
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let node = (i % 2_000).to_string();
                g.add_edge(10_000 + i, node.as_str(), "w", NO_PROPS, Some("q"))
                    .unwrap();
                writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        })
    };
    thread::sleep(Duration::from_millis(100));
    assert!(!query.is_finished(), "the query should still be running");
    let before = writes.load(Ordering::Relaxed);
    assert!(before > 0, "the writer should write while the query runs");
    token.cancel();
    let cancelled = Instant::now();
    let result = within_60s(move || query.join().unwrap());
    assert!(
        matches!(result, Err(GraphError::Rdf(RdfError::Cancelled))),
        "{result:?}"
    );
    assert!(cancelled.elapsed() < Duration::from_secs(10));
    // the writer goes on after the cancel
    while writes.load(Ordering::Relaxed) <= before {
        assert!(
            cancelled.elapsed() < Duration::from_secs(10),
            "no write since the cancel"
        );
        thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Relaxed);
    within_60s(move || writer.join().unwrap());
}

/// Stopping a query leaves nothing armed for later queries on the same thread.
#[test]
fn a_stopped_query_leaves_its_thread_as_it_was() {
    let g = small_graph();
    let slow = format!("SELECT (COUNT(*) AS ?n) {{ {CUBE} }}");
    assert!(g.sparql_with(&slow, &timeout(20)).is_err());
    let expected = g
        .sparql("SELECT (COUNT(*) AS ?n) { ?a ?b ?c . ?d ?e ?f }")
        .unwrap();
    match &expected {
        SparqlResults::Solutions { rows, .. } => assert_eq!(
            rows[0][0].as_ref().unwrap().to_string(),
            "\"400\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        g.sparql_with(
            "SELECT (COUNT(*) AS ?n) { ?a ?b ?c . ?d ?e ?f }",
            &SparqlOptions::default().with_timeout(Duration::from_secs(60))
        )
        .unwrap(),
        expected
    );
    // another panic in a query under limits still unwinds as a panic
    let caught = panic::catch_unwind(|| {
        run_interruptible(&Arc::new(Interrupt::new(None, None)), || {
            panic!("not a stop")
        })
    });
    let payload = caught.unwrap_err();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"not a stop"));
    // and the thread is not left armed
    assert!(g.sparql("ASK { ?s ?p ?o }").is_ok());
}

/// A scan checks its interrupt before each node, also where it produces nothing.
#[test]
fn scans_stop_when_interrupted() {
    let g = Graph::new();
    for i in 0..50 {
        let (src, dst) = (format!("n{i}"), format!("n{}", (i + 1) % 50));
        g.add_edge(i, src.as_str(), dst.as_str(), NO_PROPS, Some("p"))
            .unwrap();
        g.add_node(1_500, src.as_str(), NO_PROPS, None, None)
            .unwrap();
    }
    let window = g.window(1_000, 2_000).into_dynamic();
    let scan = |interrupt: &Arc<Interrupt>| {
        EdgeScan::all_by_nodes(window.clone(), None).interruptible(Some(interrupt.clone()))
    };
    // one check per node, none of which has an edge in the window
    let interrupt = Arc::new(Interrupt::new(Some(CancellationToken::new()), None));
    assert!(scan(&interrupt).next().is_none());
    assert!(interrupt.checks() >= 50, "{} checks", interrupt.checks());
    assert!(interrupt.error().is_none());
    // stopped before it reads the first node
    let token = CancellationToken::new();
    token.cancel();
    let interrupt = Arc::new(Interrupt::new(Some(token), None));
    assert!(scan(&interrupt).next().is_none());
    assert_eq!(interrupt.checks(), 1);
    assert!(matches!(interrupt.error(), Some(RdfError::Cancelled)));
    // and between two nodes of a scan that produces triples
    let token = CancellationToken::new();
    let interrupt = Arc::new(Interrupt::new(Some(token.clone()), None));
    let mut scan = EdgeScan::all_by_nodes(g.clone().into_dynamic(), None)
        .interruptible(Some(interrupt.clone()));
    assert!(scan.next().is_some());
    token.cancel();
    assert!(scan.next().is_none());
    // a deadline that has passed fires at the first check
    let interrupt = Interrupt::new(None, Some(Duration::ZERO));
    assert!(interrupt.error().is_none(), "no check saw it yet");
    assert!(interrupt.fired());
    assert!(matches!(
        interrupt.error(),
        Some(RdfError::Timeout { timeout }) if timeout == Duration::ZERO
    ));
    // a timeout too long for the clock never fires
    let interrupt = Interrupt::new(None, Some(Duration::MAX));
    assert!(!interrupt.fired());
}

/// The first check after the deadline sees it, however few checks came before.
#[test]
fn the_deadline_does_not_depend_on_the_checks() {
    let interrupt = Interrupt::new(None, Some(Duration::from_millis(50)));
    assert!(!interrupt.fired());
    thread::sleep(Duration::from_millis(150));
    assert!(interrupt.fired(), "the second check sees the deadline");
    assert_eq!(interrupt.checks(), 2);
    assert!(matches!(
        interrupt.error(),
        Some(RdfError::Timeout { timeout }) if timeout == Duration::from_millis(50)
    ));
    // many interrupts at once, with deadlines in any order
    let start = Instant::now();
    let interrupts: Vec<_> = [300u64, 100, 200, 100, 50]
        .iter()
        .map(|&ms| (ms, Interrupt::new(None, Some(Duration::from_millis(ms)))))
        .collect();
    for (ms, interrupt) in &interrupts {
        while !interrupt.fired() {
            assert!(start.elapsed() < Duration::from_secs(10), "{ms} ms");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(start.elapsed() >= Duration::from_millis(*ms));
    }
    // the deadline of a query is removed from the timer when it is done, or when it fires
    let interrupt = Interrupt::new(None, Some(Duration::from_secs(60)));
    let in_timer = interrupt.deadline_in_timer();
    assert!(in_timer());
    assert!(!interrupt.fired());
    drop(interrupt);
    assert!(!in_timer());
    let interrupt = Interrupt::new(None, Some(Duration::from_millis(10)));
    let in_timer = interrupt.deadline_in_timer();
    while !interrupt.fired() {
        assert!(start.elapsed() < Duration::from_secs(10));
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!in_timer());
}

/// A timeout does not cancel the caller's token.
#[test]
fn a_timeout_leaves_the_callers_token_alone() {
    let g = small_graph();
    let slow = format!("SELECT (COUNT(*) AS ?n) {{ {CUBE} }}");
    let token = CancellationToken::new();
    let options = timeout(50).with_cancellation_token(token.clone());
    assert!(matches!(
        g.sparql_with(&slow, &options),
        Err(GraphError::Rdf(RdfError::Timeout { .. }))
    ));
    assert!(!token.is_cancelled());
    assert!(g.sparql_with("ASK { ?s ?p ?o }", &options).is_ok());
}

/// After a stop, term lookups still succeed or unwind under `run_interruptible`, and scans unwind
/// there and end with an error elsewhere: spareval would read a lookup error as an unbound value.
#[test]
fn a_stop_never_alters_what_spareval_computes() {
    let g = small_graph();
    let token = CancellationToken::new();
    let interrupt = Arc::new(Interrupt::new(Some(token.clone()), None));
    let dataset = RaphtoryDataset::new(g.graph().clone()).with_interrupt(Some(interrupt.clone()));
    let one = Term::from(Literal::from(1));
    let node = crate::rdf::term_of("1");
    let internal = [
        dataset.internalize_term(one.clone()).unwrap(),
        dataset.internalize_term(node.clone()).unwrap(),
    ];
    token.cancel();
    for (internal, term) in internal.iter().zip([&one, &node]) {
        assert_eq!(&dataset.externalize_term(internal.clone()).unwrap(), term);
        assert_eq!(&dataset.internalize_term(term.clone()).unwrap(), internal);
        let stopped = run_interruptible(&interrupt, || dataset.externalize_term(internal.clone()));
        assert!(stopped.is_none());
        let stopped = run_interruptible(&interrupt, || dataset.internalize_term(term.clone()));
        assert!(stopped.is_none());
    }
    let stopped = run_interruptible(&interrupt, || {
        dataset
            .internal_quads_for_pattern(None, None, None, Some(None))
            .count()
    });
    assert!(stopped.is_none());
    let mut quads = dataset.internal_quads_for_pattern(None, None, None, Some(None));
    assert!(matches!(quads.next(), Some(Err(RdfError::Cancelled))));
    assert!(quads.next().is_none());
    assert!(matches!(interrupt.error(), Some(RdfError::Cancelled)));
}

/// Queries stopped during `ORDER BY` fail with the stop's error, never a sort panic.
#[test]
fn queries_stopped_while_they_sort_fail_with_the_error_of_the_stop() {
    let g = small_graph();
    let values: String = (0..250)
        .map(|i| format!("{} ", (i * 7_919) % 100_003))
        .collect();
    let queries = [
        (
            format!(
                "SELECT ?a ?b {{ VALUES ?a {{ {values} }} VALUES ?b {{ {values} }} }} \
                 ORDER BY DESC(?a) ?b LIMIT 1"
            ),
            12,
        ),
        (
            "SELECT ?a ?c ?e { ?a raphtory:p ?b . ?c raphtory:p ?d . ?e raphtory:p ?f } \
             ORDER BY DESC(raphtory:validFromTime(?a, raphtory:p, ?b)) \
             DESC(EXISTS { ?d raphtory:p ?x . ?x raphtory:p ?a }) ?c ?e LIMIT 2"
                .to_owned(),
            24,
        ),
    ];
    for (query, runs) in queries {
        let start = Instant::now();
        let expected = g.sparql(&query).unwrap();
        let full = start.elapsed();
        for i in 0..runs {
            // most of the time of these queries is the sort
            let limit = full.mul_f64(0.3 + 0.65 * f64::from(i) / f64::from(runs));
            let options = SparqlOptions::default().with_timeout(limit);
            let result = panic::catch_unwind(AssertUnwindSafe(|| g.sparql_with(&query, &options)));
            match result {
                Ok(Ok(results)) => assert_eq!(results, expected),
                Ok(Err(GraphError::Rdf(RdfError::Timeout { .. }))) => {}
                Ok(Err(error)) => panic!("{query}: {error}"),
                Err(_) => panic!("{query}: the query panicked when stopped after {limit:?}"),
            }
        }
    }
}
