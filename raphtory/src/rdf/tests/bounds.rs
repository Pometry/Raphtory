//! Bounding the work of a query: `SparqlOptions::max_triple_patterns`, `timeout` and
//! `cancellation_token`.
use super::within_60s;
use crate::{
    db::api::view::IntoDynamic,
    errors::GraphError,
    prelude::*,
    rdf::{
        limits::{count_triple_patterns, run_interruptible, Interrupt},
        model::{Literal, Term},
        query::parse,
        scan::EdgeScan,
        sparql_stack_size, timeout_from_secs, CancellationToken, QueryResultsFormat,
        RaphtoryDataset, RdfError, RdfFormat, RdfViewOps, SparqlOptions, SparqlResults,
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
        // basic graph patterns
        ("ASK {}", 0),
        ("ASK { ?s ?p ?o }", 1),
        ("ASK { ?s ?p ?o . ?o ?q ?r . ?r ?t ?u }", 3),
        ("ASK { ?s ?p ?o , ?o2 ; ?q ?r }", 3),
        // collections and blank node property lists are triple patterns once parsed
        ("ASK { ?s ?p (1 2 3) }", 7),
        ("ASK { ?s ?p [ ?q 1 ; ?r 2 ] }", 3),
        // property paths: one per predicate they name
        ("ASK { ?s raphtory:a ?o }", 1),
        ("ASK { ?s raphtory:a/raphtory:b/raphtory:c ?o }", 3),
        ("ASK { ?s raphtory:a|raphtory:b ?o }", 2),
        ("ASK { ?s ^raphtory:a ?o }", 1),
        ("ASK { ?s raphtory:a* ?o }", 1),
        ("ASK { ?s raphtory:a+ ?o }", 1),
        ("ASK { ?s raphtory:a? ?o }", 1),
        ("ASK { ?s !(raphtory:a|raphtory:b) ?o }", 1),
        ("ASK { ?s (raphtory:a/^raphtory:b)*|raphtory:c ?o }", 3),
        // VALUES: one per block, plus one per variable and per 100 rows
        ("ASK { VALUES ?x { 1 2 3 } }", 2),
        ("ASK { VALUES (?x ?y) { (1 2) (3 UNDEF) } ?x ?p ?y }", 4),
        ("ASK { VALUES ?x { } }", 2),
        ("ASK { VALUES () { () } }", 1),
        ("ASK { ?s ?p ?o } VALUES ?s { 1 2 }", 3),
        // BIND and the expressions of SELECT and GROUP BY: one each
        ("ASK { BIND(1 AS ?x) BIND(2 AS ?y) }", 2),
        ("SELECT (1 AS ?x) {}", 1),
        (
            "SELECT ?x (COUNT(*) AS ?n) { ?s ?p ?o } GROUP BY (STR(?s) AS ?x)",
            3,
        ),
        (
            "SELECT * { { SELECT * { BIND(1 AS ?o) BIND(raphtory:p AS ?p) } } ?s ?p ?o }",
            3,
        ),
        // groups, OPTIONAL, MINUS, UNION, GRAPH, SERVICE and LATERAL
        ("ASK { { ?s ?p ?o } { ?s ?q ?r } }", 2),
        ("ASK { ?s ?p ?o OPTIONAL { ?o ?q ?r . ?r ?t ?u } }", 3),
        (
            "ASK { ?s ?p ?o OPTIONAL { ?o ?q ?r FILTER EXISTS { ?r ?t ?u } } }",
            3,
        ),
        ("ASK { ?s ?p ?o MINUS { ?s ?q ?r } }", 2),
        (
            "ASK { { ?s ?p ?o } UNION { ?s ?q ?r } UNION { ?a ?b ?c } }",
            3,
        ),
        (
            "ASK { GRAPH ?g { ?s ?p ?o } GRAPH raphtory:asof:1 { ?s ?q ?r } }",
            2,
        ),
        ("ASK { SERVICE <http://ex/> { ?s ?p ?o } }", 1),
        ("ASK { ?s ?p ?o LATERAL { ?o ?q ?r } }", 2),
        // EXISTS wherever an expression is
        ("ASK { ?s ?p ?o FILTER EXISTS { ?o ?q ?r } }", 2),
        ("ASK { ?s ?p ?o FILTER NOT EXISTS { ?o ?q ?r } }", 2),
        (
            "ASK { ?s ?p ?o FILTER(?o = 1 || (EXISTS { ?o ?q ?r } && !EXISTS { ?s ?q ?r })) }",
            3,
        ),
        ("ASK { ?s ?p ?o BIND(EXISTS { ?o ?q ?r } AS ?e) }", 3),
        (
            "ASK { ?s ?p ?o BIND(IF(EXISTS { ?o ?q ?r }, COALESCE(EXISTS { ?r ?t ?u }), 1) AS ?e) }",
            4,
        ),
        (
            "ASK { ?s ?p ?o FILTER(?o IN (1, STR(EXISTS { ?o ?q ?r }))) }",
            2,
        ),
        (
            "SELECT ?s { ?s ?p ?o } ORDER BY DESC(EXISTS { ?o ?q ?r })",
            2,
        ),
        ("SELECT (EXISTS { ?o ?q ?r } AS ?e) { ?s ?p ?o }", 3),
        (
            "SELECT (SUM(IF(EXISTS { ?o ?q ?r }, 1, 0)) AS ?n) (COUNT(*) AS ?c) { ?s ?p ?o }",
            4,
        ),
        (
            "SELECT ?s { ?s ?p ?o } GROUP BY ?s HAVING (EXISTS { ?s ?q ?r })",
            2,
        ),
        // sub-queries, with their modifiers
        (
            "SELECT * { ?s ?p ?o { SELECT DISTINCT ?o { ?o ?q ?r . ?r ?t ?u } ORDER BY ?o LIMIT 2 } }",
            3,
        ),
        (
            "SELECT REDUCED ?s { { SELECT ?s { ?s ?p ?o } OFFSET 1 } }",
            1,
        ),
        // every query form; the CONSTRUCT template does not count
        ("SELECT * { ?s ?p ?o }", 1),
        (
            "CONSTRUCT { ?s ?p ?o . ?o ?p ?s . ?s ?p 1 } WHERE { ?s ?p ?o }",
            1,
        ),
        ("CONSTRUCT WHERE { ?s ?p ?o . ?o ?q ?r }", 2),
        ("DESCRIBE ?s { ?s ?p ?o }", 1),
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
    for (rows, expected) in [(0, 3), (99, 3), (100, 4), (199, 4), (250, 5), (1_000, 13)] {
        assert_eq!(patterns(&values(rows)), expected, "{rows} rows");
    }
}

/// What binds the variables of a star before it is joined counts too.
#[test]
fn bound_stars_count_what_binds_them() {
    let star = |n: usize| -> String { (0..n).map(|i| format!("?s ?p{i} ?o{i} . ")).collect() };
    let binds = |n: usize| -> String {
        (0..n)
            .map(|i| format!("BIND(1 AS ?o{i}) BIND(raphtory:p AS ?p{i}) "))
            .collect()
    };
    let query = format!(
        "SELECT * {{ {{ SELECT * {{ {} }} }} {} }}",
        binds(100),
        star(100)
    );
    assert_eq!(patterns(&query), 300);
    assert_eq!(
        patterns(&format!("SELECT * {{ {} {} }}", binds(100), star(100))),
        300
    );
    let columns: String = (0..99).map(|i| format!("?o{i} ?p{i} ")).collect();
    let row = "1 raphtory:p ".repeat(99);
    assert_eq!(
        patterns(&format!(
            "SELECT * {{ VALUES ({columns}) {{ ({row}) }} {} }}",
            star(99)
        )),
        1 + 198 + 99
    );
    let g = small_graph();
    assert!(matches!(
        g.sparql_with(&query, &max_patterns(100)),
        Err(GraphError::Rdf(RdfError::TooManyPatterns {
            count: 300,
            max: 100
        }))
    ));
}

/// A lookup of many values costs little to plan, and counts little.
#[test]
fn values_lookups_count_little() {
    let g = small_graph();
    let ids: String = (0..1_000).map(|i| format!("raphtory:{i} ")).collect();
    let query = format!("SELECT ?s ?o {{ VALUES ?s {{ {ids} }} ?s raphtory:p ?o }}");
    assert_eq!(patterns(&query), 1 + 1 + 10 + 1);
    match g.sparql_with(&query, &max_patterns(100)).unwrap() {
        SparqlResults::Solutions { rows, .. } => assert_eq!(rows.len(), 20),
        other => panic!("{other:?}"),
    }
}

/// A query that needs a deep stack to parse is counted on a small one.
#[test]
fn counting_a_long_query_needs_little_stack() {
    let query = format!(
        "SELECT * {{ {{?s ?p ?o}}{} }}",
        " UNION {?s ?p ?o}".repeat(20_000)
    );
    let parsed = thread::Builder::new()
        .stack_size(sparql_stack_size(query.len()))
        .spawn(move || parse(&query).unwrap())
        .unwrap()
        .join()
        .unwrap();
    let count = thread::Builder::new()
        .stack_size(64 << 10)
        .spawn(move || {
            let count = count_triple_patterns(&parsed);
            // dropping the parsed query recurses: on the stack it was made on
            (count, parsed)
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(count.0, 20_001);
    thread::Builder::new()
        .stack_size(sparql_stack_size(1 << 20))
        .spawn(move || drop(count))
        .unwrap()
        .join()
        .unwrap();
}

fn small_graph() -> Graph {
    let g = Graph::new();
    for i in 0..20 {
        g.add_edge(i, i.to_string(), (i + 1).to_string(), NO_PROPS, Some("p"))
            .unwrap();
    }
    g
}

fn max_patterns(max: usize) -> SparqlOptions {
    SparqlOptions::default().with_max_triple_patterns(max)
}

fn star(n: usize) -> String {
    let patterns: String = (0..n).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
    format!("SELECT ?s {{ {patterns} }}")
}

#[test]
fn queries_with_too_many_triple_patterns_are_rejected() {
    let g = small_graph();
    for max in [0, 1, 5, 100] {
        assert!(g.sparql_with(&star(max), &max_patterns(max)).is_ok());
        match g.sparql_with(&star(max + 1), &max_patterns(max)) {
            Err(GraphError::Rdf(RdfError::TooManyPatterns { count, max: limit })) => {
                assert_eq!((count, limit), (max + 1, max))
            }
            other => panic!("{other:?}"),
        }
    }
    let message = |count: usize, max: usize| {
        format!(
            "SPARQL query too complex: {count} triple patterns, the limit is {max} (each triple \
             pattern, property path step, BIND and (... AS ?v) of SELECT or GROUP BY counts \
             one, and each VALUES block one plus one per variable and per 100 rows)"
        )
    };
    let error = g.sparql_with(&star(3), &max_patterns(2)).unwrap_err();
    assert_eq!(error.to_string(), message(3, 2));
    // the message accounts for the expressions of SELECT, which count too
    let aggregates = "SELECT (COUNT(*) AS ?n) (MAX(?o) AS ?m) { ?s ?p ?o }";
    assert!(g.sparql_with(aggregates, &max_patterns(3)).is_ok());
    let error = g.sparql_with(aggregates, &max_patterns(1)).unwrap_err();
    assert_eq!(error.to_string(), message(3, 1));
    // without a limit, any number
    assert!(g.sparql(&star(101)).is_ok());
    assert!(g.sparql_with(&star(101), &SparqlOptions::default()).is_ok());
    // collections count
    let query = "ASK { ?s ?p (1 2 3) }";
    assert!(g.sparql_with(query, &max_patterns(7)).is_ok());
    assert!(g.sparql_with(query, &max_patterns(6)).is_err());
}

#[test]
fn too_many_triple_patterns_write_nothing() {
    let g = small_graph();
    let mut out = Vec::new();
    let error = g
        .sparql_to_writer_with(
            &star(4),
            &mut out,
            QueryResultsFormat::Json,
            &max_patterns(3),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::Rdf(RdfError::TooManyPatterns { count: 4, max: 3 })
    ));
    assert!(out.is_empty());
    let query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . ?o ?q ?r }";
    let error = g
        .sparql_to_writer_with(query, &mut out, RdfFormat::NTriples, &max_patterns(1))
        .unwrap_err();
    assert!(matches!(
        error,
        GraphError::Rdf(RdfError::TooManyPatterns { count: 2, max: 1 })
    ));
    assert!(out.is_empty());
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
    // solutions returned one by one
    let view = g.clone();
    times_out(move || view.sparql_with(&format!("SELECT ?a {{ {CUBE} }}"), &timeout(200)));
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
    let view = g.clone();
    times_out(move || {
        view.sparql_to_writer_with(
            &format!("CONSTRUCT {{ ?a ?b ?x }} WHERE {{ {CUBE} }}"),
            io::sink(),
            RdfFormat::NTriples,
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
    times_out(move || {
        chain.sparql_with(
            "SELECT (COUNT(*) AS ?n) { ?a raphtory:p* ?b . ?b raphtory:p* ?c }",
            &timeout(200),
        )
    });
}

#[test]
fn the_timeout_message_names_the_limit() {
    let g = small_graph();
    let error = g
        .sparql_with(&format!("SELECT ?a {{ {CUBE} }}"), &timeout(50))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "SPARQL query timed out: it ran longer than its time limit of 50ms"
    );
}

#[test]
fn fast_queries_do_not_time_out() {
    let g = small_graph();
    let options = SparqlOptions::default().with_timeout(Duration::from_secs(60));
    for query in [
        "SELECT * { ?s ?p ?o }",
        "SELECT (COUNT(*) AS ?n) { ?a ?b ?c . ?d ?e ?f }",
        "ASK { raphtory:1 raphtory:p raphtory:2 }",
        "CONSTRUCT { ?o ?p ?s } WHERE { ?s ?p ?o }",
        "SELECT ?x { VALUES ?x { 1 2 3 } }",
    ] {
        assert_eq!(
            g.sparql_with(query, &options).unwrap(),
            g.sparql(query).unwrap(),
            "{query}"
        );
    }
    // all the limits at once, and the temporal functions still work
    let pg = PersistentGraph::new();
    pg.add_edge(3, "a", "b", NO_PROPS, Some("p")).unwrap();
    let options = options
        .with_max_triple_patterns(10)
        .with_cancellation_token(CancellationToken::new());
    let query = "SELECT (raphtory:validFromTime(raphtory:a, raphtory:p, raphtory:b) AS ?t) {}";
    assert_eq!(
        pg.sparql_with(query, &options).unwrap(),
        pg.sparql(query).unwrap()
    );
    // a zero timeout stops any query
    assert!(matches!(
        g.sparql_with("ASK {}", &timeout(0)),
        Err(GraphError::Rdf(RdfError::Timeout { .. }))
    ));
}

/// A finite timeout too large for a `Duration` never fires and is not an error.
#[test]
fn huge_timeouts_never_fire() {
    let g = small_graph();
    for seconds in [1.8e19, 1e20, f64::MAX] {
        let limit = timeout_from_secs(seconds).unwrap();
        let options = SparqlOptions::default().with_timeout(limit);
        assert_eq!(
            g.sparql_with("SELECT * { ?s ?p ?o }", &options).unwrap(),
            g.sparql("SELECT * { ?s ?p ?o }").unwrap(),
            "{seconds}"
        );
    }
    assert_eq!(timeout_from_secs(1e20), Some(Duration::MAX));
    assert_eq!(timeout_from_secs(0.25), Some(Duration::from_millis(250)));
    for invalid in [-1.0, -1e20, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(timeout_from_secs(invalid), None, "{invalid}");
    }
}

#[test]
fn queries_can_be_cancelled() {
    let g = small_graph();
    // before it starts
    let token = CancellationToken::new();
    token.cancel();
    let options = SparqlOptions::default().with_cancellation_token(token);
    for query in ["ASK {}", "SELECT * { ?s ?p ?o }"] {
        let error = g.sparql_with(query, &options).unwrap_err();
        assert!(
            matches!(error, GraphError::Rdf(RdfError::Cancelled)),
            "{error:?}"
        );
        assert_eq!(error.to_string(), "SPARQL query cancelled");
    }
    // while it runs, from another thread
    for query in [
        format!("SELECT ?a {{ {CUBE} }}"),
        format!("SELECT (COUNT(*) AS ?n) {{ {CUBE} }}"),
    ] {
        let token = CancellationToken::new();
        let options = SparqlOptions::default().with_cancellation_token(token.clone());
        let view = g.clone();
        let query = thread::spawn(move || view.sparql_with(&query, &options));
        thread::sleep(Duration::from_millis(100));
        let cancelled = Instant::now();
        token.cancel();
        let result = within_60s(move || query.join().unwrap());
        assert!(
            matches!(result, Err(GraphError::Rdf(RdfError::Cancelled))),
            "{result:?}"
        );
        assert!(cancelled.elapsed() < Duration::from_secs(10));
    }
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
    let view = g.clone();
    let query = thread::spawn(move || {
        view.sparql_with(
            "SELECT (COUNT(*) AS ?n) { ?a raphtory:p* ?b . ?b raphtory:p* ?c }",
            &options,
        )
    });
    // writes to the nodes the query reads, timing every write
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (g, stop, writes) = (g.clone(), stop.clone(), writes.clone());
        thread::spawn(move || {
            let mut slowest = Duration::ZERO;
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let node = (i % 2_000).to_string();
                let start = Instant::now();
                g.add_edge(10_000 + i, node.as_str(), "w", NO_PROPS, Some("q"))
                    .unwrap();
                slowest = slowest.max(start.elapsed());
                writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
            slowest
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
    let slowest = within_60s(move || writer.join().unwrap());
    let after = writes.load(Ordering::Relaxed);
    assert!(
        slowest < Duration::from_millis(500),
        "a write took {slowest:?}"
    );
    // the graph works after
    g.add_edge(1_000_000, "x", "z", NO_PROPS, Some("r"))
        .unwrap();
    let count = g
        .sparql("SELECT (COUNT(*) AS ?n) { ?s raphtory:q ?o }")
        .unwrap();
    match count {
        SparqlResults::Solutions { rows, .. } => assert_eq!(
            rows[0][0].as_ref().unwrap().to_string(),
            format!(
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                after.min(2_000)
            )
        ),
        other => panic!("{other:?}"),
    }
}

/// Stopping a query leaves nothing armed for later queries on the same thread.
#[test]
fn a_stopped_query_leaves_its_thread_as_it_was() {
    let g = small_graph();
    let slow = format!("SELECT (COUNT(*) AS ?n) {{ {CUBE} }}");
    for _ in 0..3 {
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
    }
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
        EdgeScan::all_by_nodes(window.clone(), None)
            .only_valid()
            .interruptible(Some(interrupt.clone()))
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
        .only_valid()
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

/// A query with few, expensive interrupt checks stops at the first check after its deadline.
#[test]
fn expensive_steps_stop_at_the_deadline() {
    let g = small_graph();
    let query = |rewrites: usize| {
        let mut steps = vec!["BIND(\"aaaaaaaaaaaaaaaa\" AS ?a0)".to_owned()];
        steps.extend((0..18).map(|i| format!("BIND(CONCAT(?a{i}, ?a{i}) AS ?a{})", i + 1)));
        steps.extend(
            (0..rewrites).map(|j| format!("BIND(STRLEN(REPLACE(?a18, \"a\", \"b\")) AS ?n{j})")),
        );
        format!("SELECT ?n0 {{ {} }}", steps.join(" "))
    };
    // the time of a step
    let time = |rewrites: usize| {
        let start = Instant::now();
        g.sparql(&query(rewrites)).unwrap();
        start.elapsed()
    };
    let step = time(21).saturating_sub(time(1)) / 20;
    let (result, elapsed) = within_60s(move || {
        let start = Instant::now();
        let result = g.sparql_with(&query(1_000), &timeout(200));
        (result, start.elapsed())
    });
    assert!(
        matches!(result, Err(GraphError::Rdf(RdfError::Timeout { .. }))),
        "{result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(300) + step * 4,
        "stopped after {elapsed:?}, a step takes {step:?}"
    );
}

/// The time limit of a query that timed out.
fn time_limit<R>(result: &Result<R, GraphError>) -> Option<Duration> {
    match result {
        Err(GraphError::Rdf(RdfError::Timeout { timeout })) => Some(*timeout),
        _ => None,
    }
}

/// A timeout does not cancel the caller's token, so other queries sharing it go on.
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
    // two queries share a token: the one that times out first does not stop the other
    let shared = CancellationToken::new();
    let long = {
        let (g, slow) = (g.clone(), slow.clone());
        let options = SparqlOptions::default()
            .with_timeout(Duration::from_secs(1))
            .with_cancellation_token(shared.clone());
        thread::spawn(move || {
            let start = Instant::now();
            (g.sparql_with(&slow, &options), start.elapsed())
        })
    };
    let short = g.sparql_with(&slow, &timeout(50).with_cancellation_token(shared.clone()));
    assert_eq!(
        time_limit(&short),
        Some(Duration::from_millis(50)),
        "{short:?}"
    );
    let (result, elapsed) = within_60s(move || long.join().unwrap());
    assert_eq!(
        time_limit(&result),
        Some(Duration::from_secs(1)),
        "{result:?}"
    );
    assert!(elapsed >= Duration::from_secs(1));
    assert!(!shared.is_cancelled());
    // cancelling the token stops every query that has it
    let queries: Vec<_> = (0..2)
        .map(|_| {
            let (g, slow) = (g.clone(), slow.clone());
            let options = SparqlOptions::default().with_cancellation_token(shared.clone());
            thread::spawn(move || g.sparql_with(&slow, &options))
        })
        .collect();
    thread::sleep(Duration::from_millis(100));
    shared.cancel();
    for query in queries {
        let result = within_60s(move || query.join().unwrap());
        assert!(
            matches!(result, Err(GraphError::Rdf(RdfError::Cancelled))),
            "{result:?}"
        );
    }
}

/// After a stop, term lookups still succeed or unwind under `run_interruptible`, and scans unwind
/// there and end with an error elsewhere: spareval would read a lookup error as an unbound value.
#[test]
fn a_stop_never_alters_what_spareval_computes() {
    let g = small_graph();
    let token = CancellationToken::new();
    let interrupt = Arc::new(Interrupt::new(Some(token.clone()), None));
    let dataset = RaphtoryDataset::new(g.clone()).with_interrupt(Some(interrupt.clone()));
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

#[test]
fn options_are_printed_without_the_token_internals() {
    let options = SparqlOptions::default()
        .with_timeout(Duration::from_secs(1))
        .with_max_triple_patterns(5)
        .with_cancellation_token(CancellationToken::new());
    assert_eq!(
        format!("{options:?}"),
        "SparqlOptions { temporal_functions: true, timeout: Some(1s), max_triple_patterns: \
         Some(5), cancellation_token: Some(\"live\"), dataset: None }"
    );
    let options = options.with_timeout(None).with_max_triple_patterns(None);
    assert_eq!((options.timeout, options.max_triple_patterns), (None, None));
}
