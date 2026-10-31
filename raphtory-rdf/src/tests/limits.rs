//! Queries that would overflow the stack run on the query's own stack.
use crate::{query::on_stack, RdfError, RdfGraph, SparqlOptions, SparqlResults};
use raphtory::prelude::*;
use std::thread;

fn graph() -> RdfGraph {
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.into()
}

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

#[test]
fn deeply_nested_queries_run_on_a_sparql_stack() {
    let query = format!(
        "ASK {{ FILTER({}1{}) }}",
        "(".repeat(10_000),
        ")".repeat(10_000)
    );
    let result = thread::Builder::new()
        .stack_size(256 << 10)
        .spawn(move || graph().sparql(&query, &SparqlOptions::default()))
        .unwrap()
        .join()
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn a_panic_is_an_error() {
    let result = on_stack("the query", 0, || -> Result<(), RdfError> {
        panic!("boom")
    });
    assert!(
        matches!(&result, Err(RdfError::Panicked(message)) if message.contains("boom")),
        "{result:?}"
    );
}
