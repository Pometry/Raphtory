//! oxigraph bounds the nesting and the depth of a query, so a query runs on the caller's stack.
use super::{on_small_stack, release_or_debug_stack};
use crate::{RdfError, RdfGraph, SparqlOptions, SparqlResults};
use raphtory::prelude::*;
use std::thread;

fn graph() -> RdfGraph {
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g.into()
}

// the default stack of a spawned thread
fn sparql_on_2_mib(query: String) -> Result<SparqlResults, RdfError> {
    thread::Builder::new()
        .stack_size(release_or_debug_stack(2 << 20))
        .spawn(move || graph().sparql(&query, &SparqlOptions::default()))
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn long_flat_queries_need_little_stack() {
    // oxigraph balances the 9,000 unions
    let query = format!("SELECT * {{ {{}}{} }}", "UNION{}".repeat(9_000));
    let results = on_small_stack(move || graph().sparql(&query, &SparqlOptions::default()));
    match results.unwrap() {
        SparqlResults::Solutions { rows, .. } => assert_eq!(rows.len(), 9_001),
        other => panic!("{other:?}"),
    }
}

#[test]
fn queries_at_the_limits_run_on_a_default_stack() {
    let r = |s: &str, n: usize| s.repeat(n);
    for query in [
        // the deepest nesting and the longest chain oxigraph accepts
        format!(
            "ASK {{ {}?s ?p ?o{} }}",
            r("FILTER EXISTS { ", 254),
            r(" }", 254)
        ),
        format!("ASK {{ FILTER({}\"a\"{}) }}", r("STR(", 254), r(")", 254)),
        format!(
            "ASK {{ ?s ?p ?o {} }}",
            (0..510)
                .map(|i| format!("BIND(1 AS ?b{i}) "))
                .collect::<String>()
        ),
    ] {
        let result = sparql_on_2_mib(query);
        assert!(result.is_ok(), "{result:?}");
    }
}

#[test]
fn deeper_queries_are_errors() {
    let nested = format!(
        "ASK {{ FILTER({}1{}) }}",
        "(".repeat(10_000),
        ")".repeat(10_000)
    );
    let long = format!(
        "ASK {{ ?s ?p ?o {} }}",
        "OPTIONAL { ?s ?p ?o } ".repeat(600)
    );
    for (query, error) in [(nested, "nested"), (long, "levels deep")] {
        let result = sparql_on_2_mib(query);
        assert!(
            matches!(&result, Err(RdfError::SparqlSyntax(e)) if e.to_string().contains(error)),
            "{result:?}"
        );
    }
}

#[test]
fn built_triple_terms_stay_shallow() {
    // 20 BINDs of 200 nested triple terms each would build a term 4,000 deep
    let binds = (0..20)
        .map(|i| {
            format!(
                "BIND({}?t{i}{} AS ?t{}) ",
                "<<( :s :p ".repeat(200),
                " )>>".repeat(200),
                i + 1
            )
        })
        .collect::<String>();
    let query = format!("PREFIX : <http://e/> SELECT ?t20 {{ BIND(:o AS ?t0) {binds} }}");
    let result = sparql_on_2_mib(query);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn a_panic_is_an_error() {
    let result = RdfError::caught("the query", || -> Result<(), RdfError> { panic!("boom") });
    assert!(
        matches!(&result, Err(RdfError::Panicked(message)) if message.contains("boom")),
        "{result:?}"
    );
}
