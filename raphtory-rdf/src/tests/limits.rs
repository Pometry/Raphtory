//! Queries that would overflow the stack: `MAX_SPARQL_NESTING` and the query's own stack.
use crate::{
    query::on_query_stack, RdfError, RdfGraph, SparqlOptions, SparqlResults, MAX_SPARQL_NESTING,
};
use raphtory::prelude::*;
use std::thread;

fn graph() -> RdfGraph {
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.into()
}

/// Where the query nests too deep, if it does.
fn too_deep(query: &str) -> Option<(usize, usize)> {
    match graph().sparql(query, &SparqlOptions::default()) {
        Err(RdfError::SparqlTooDeep { max, line, column }) => {
            assert_eq!(max, MAX_SPARQL_NESTING);
            Some((line, column))
        }
        _ => None,
    }
}

/// Queries whose brackets nest `depth` deep in total, one for each kind of bracket and for the
/// constructs that recurse most per bracket.
fn nested(depth: usize) -> Vec<String> {
    let n = depth - 1; // the brackets of the query itself
    vec![
        // expressions
        format!(
            "ASK {{ FILTER({}1{}) }}",
            "(".repeat(n - 1),
            ")".repeat(n - 1)
        ),
        format!(
            "ASK {{ FILTER({}1{}) }}",
            "STR(".repeat(n - 1),
            ")".repeat(n - 1)
        ),
        // group patterns, OPTIONAL and sub-queries
        format!("ASK {{ {} ?s ?p ?o {} }}", "{".repeat(n), "}".repeat(n)),
        format!(
            "ASK {{ ?s ?p ?o {} }}",
            "OPTIONAL { ".repeat(n) + &"}".repeat(n)
        ),
        format!(
            "ASK {{ {}{{ ?s ?p ?o }}{} }}",
            "{ SELECT * WHERE ".repeat(n - 1),
            "}".repeat(n - 1)
        ),
        // blank node property lists, collections and paths
        format!("ASK {{ ?s ?p {}1{} }}", "[ ?p ".repeat(n), " ]".repeat(n)),
        format!("ASK {{ ?s ?p {}1{} }}", "(".repeat(n), ")".repeat(n)),
        format!("ASK {{ ?s {}a{} ?o }}", "(".repeat(n), ")".repeat(n)),
    ]
}

/// Queries nest up to `MAX_SPARQL_NESTING`, and deeper ones fail before they are parsed.
#[test]
fn queries_can_nest_up_to_the_limit() {
    for query in nested(MAX_SPARQL_NESTING) {
        assert!(
            graph().sparql(&query, &SparqlOptions::default()).is_ok(),
            "{query}"
        );
    }
    for query in nested(MAX_SPARQL_NESTING + 1) {
        assert!(too_deep(&query).is_some(), "{query}");
    }
    // far too deep for any stack: fails without parsing, on the 2 MiB stack of a test thread
    let deep = 1_000_000;
    let query = format!(
        "ASK {{ FILTER({}1{}) }}",
        "(".repeat(deep),
        ")".repeat(deep)
    );
    assert_eq!(too_deep(&query), Some((1, 140)));
    // the position is that of the first bracket too deep, in characters
    let query = format!(
        "# é\nASK {{\n  FILTER(\"é\" = {}1{}) }}",
        "(".repeat(200),
        ")".repeat(200)
    );
    assert_eq!(too_deep(&query), Some((3, 142)));
}

/// The nesting scan skips brackets in strings, comments and escaped names, and no others.
#[test]
fn every_reading_of_the_query_is_checked() {
    let hiding = |deep: &str| {
        vec![
            // `<'a>'` is the string `'a>'`, not an IRI followed by a quote
            format!("ASK {{ FILTER(1<'a>' && {deep}) }} # '"),
            format!("ASK {{ FILTER(1<'a>' && {deep}) }}"),
            // `<x'y>` is an IRI, not the start of a string
            format!("ASK {{ <http://ex/x'y> ?p ?o FILTER({deep}) }} # '"),
            // `<x#y>` is an IRI, not the start of a comment
            format!("ASK {{ <http://ex/x#y> ?p ?o FILTER({deep}) }}"),
            // escaped quotes and `#` in prefixed names and strings
            format!("PREFIX ex: <http://ex/> ASK {{ ex:a\\' ?p ?o FILTER({deep}) }} # '"),
            format!("PREFIX ex: <http://ex/> ASK {{ ex:a\\# ?p ?o FILTER({deep}) }}"),
            format!("ASK {{ FILTER(\"a\\\"b\" != {deep}) }} # \""),
            // `'''` starts a long string here, and is the empty string `''` and a quote there
            format!("ASK {{ FILTER('''a'b''' != '' && {deep}) }}"),
            format!("ASK {{ VALUES ?x {{ '''a' }} FILTER({deep}) }}"),
        ]
    };
    let shallow = |depth: usize| format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
    let deep = "(".repeat(200);
    // (query, rejected as too deep)
    let mut cases: Vec<(String, bool)> = vec![
        (format!("ASK {{ FILTER(STRLEN('{deep}') > 0) }}"), false),
        (
            format!("ASK {{ FILTER(STRLEN('''{deep}\n''') > 0) }}"),
            false,
        ),
        (
            format!("ASK {{ FILTER(STRLEN(\"\\\"{deep}\") > 0) }}"),
            false,
        ),
        (format!("ASK {{ FILTER(true) }} # {deep}"), false),
        // `\(` in a prefixed name is a character of the name
        (
            format!(
                "PREFIX ex: <http://ex/> ASK {{ FILTER(ex:a{} != ex:b) }}",
                "\\(".repeat(200)
            ),
            false,
        ),
        (format!("ASK {{ {} }}", "{} ".repeat(1000)), false),
        // brackets in IRIs count (the `<` might be an operator), but balanced ones do not add up
        (
            format!(
                "ASK {{ FILTER(<http://ex/{}> != <http://ex/>) }}",
                "(a)".repeat(200)
            ),
            false,
        ),
        (
            format!("ASK {{ FILTER(<http://ex/{deep}> != <http://ex/>) }}"),
            true,
        ),
    ];
    cases.extend(hiding(&shallow(3)).into_iter().map(|query| (query, false)));
    cases.extend(
        hiding(&shallow(MAX_SPARQL_NESTING))
            .into_iter()
            .map(|query| (query, true)),
    );
    for (query, rejected) in cases {
        if rejected {
            assert!(too_deep(&query).is_some(), "{query}");
        } else if let Err(error) = graph().sparql(&query, &SparqlOptions::default()) {
            panic!("{query}: {error}");
        }
    }
}

/// A long flat query runs on a stack sized to it, whatever the stack of the caller.
#[test]
fn long_flat_queries_run_on_a_sparql_stack() {
    // about 9,000 nested unions
    let query = format!("SELECT * {{ {{}}{} }}", "UNION{}".repeat(9_000));
    let results = thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || graph().sparql(&query, &SparqlOptions::default()))
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    match results {
        SparqlResults::Solutions { rows, .. } => assert_eq!(rows.len(), 9_001),
        other => panic!("{other:?}"),
    }
}

/// A panic on the query's stack is an error of the query.
#[test]
fn a_panic_is_an_error() {
    let result = on_query_stack("ASK {}", || -> Result<(), RdfError> { panic!("boom") });
    assert!(
        matches!(&result, Err(RdfError::Panicked(message)) if message.contains("boom")),
        "{result:?}"
    );
}
