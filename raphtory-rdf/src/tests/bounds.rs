//! Bounding the size of a query: `SparqlOptions::with_max_triple_patterns`.
use crate::{
    limits::count_triple_patterns, query::parse, QueryResultsFormat, RdfError, RdfGraph,
    SparqlOptions,
};
use raphtory::prelude::*;

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
    assert!(g.sparql(&star(5), &max_patterns(5)).is_ok());
    match g.sparql(&star(6), &max_patterns(5)) {
        Err(RdfError::TooManyPatterns { count, max }) => {
            assert_eq!((count, max), (6, 5))
        }
        other => panic!("{other:?}"),
    }
    let mut out = Vec::new();
    let error = g
        .sparql_to_writer(
            &star(6),
            &mut out,
            QueryResultsFormat::Json,
            &max_patterns(5),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RdfError::TooManyPatterns { count: 6, max: 5 }
    ));
    assert!(out.is_empty());
    // without a limit, any number
    assert!(g.sparql(&star(101), &SparqlOptions::default()).is_ok());
}
