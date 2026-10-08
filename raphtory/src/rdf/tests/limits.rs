//! Queries that would overflow the stack: `MAX_SPARQL_NESTING` and `sparql_stack_size`.
use crate::{
    errors::GraphError,
    prelude::*,
    rdf::{sparql_stack_size, RdfError, RdfViewOps, SparqlResults, MAX_SPARQL_NESTING},
};

fn graph() -> PersistentGraph {
    let g = PersistentGraph::new();
    g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
    g
}

/// Runs `query` on a thread with an 8 MiB stack, the usual size of a main thread.
fn run(query: String) -> Result<SparqlResults, GraphError> {
    std::thread::Builder::new()
        .stack_size(8 << 20)
        .spawn(move || graph().sparql(&query))
        .unwrap()
        .join()
        .unwrap()
}

/// Where the query nests too deep, if it does.
fn too_deep(query: &str) -> Option<(usize, usize)> {
    match graph().sparql(query) {
        Err(GraphError::Rdf(RdfError::SparqlTooDeep { max, line, column })) => {
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

#[test]
fn queries_can_nest_up_to_the_limit() {
    for query in nested(MAX_SPARQL_NESTING) {
        assert!(run(query.clone()).is_ok(), "{query}");
    }
}

#[test]
fn deeper_queries_fail_before_they_are_parsed() {
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
    let error = graph().sparql(&query).unwrap_err().to_string();
    assert_eq!(
        error,
        "SPARQL syntax error: brackets nest more than 128 deep at line 1, column 140"
    );
    // the position is that of the first bracket too deep, in characters
    let query = format!(
        "# é\nASK {{\n  FILTER(\"é\" = {}1{}) }}",
        "(".repeat(200),
        ")".repeat(200)
    );
    assert_eq!(too_deep(&query), Some((3, 142)));
}

#[test]
fn brackets_in_strings_iris_and_comments_do_not_nest() {
    let deep = "(".repeat(200);
    let ok = |query: String| match run(query.clone()) {
        Ok(_) => {}
        Err(error) => panic!("{query}: {error}"),
    };
    for string in [
        format!("'{deep}'"),
        format!("\"{deep}\""),
        format!("'''{deep}\n'''"),
        format!("\"\"\"{deep}\n\"\"\""),
        format!("'\\'{deep}'"),
        format!("\"\\\"{deep}\""),
        format!("'''a''{deep}'''"),
    ] {
        ok(format!("ASK {{ FILTER(STRLEN({string}) > 0) }}"));
    }
    // brackets in IRIs count (the `<` might be an operator), but balanced ones do not add up
    let balanced = "(a)".repeat(200);
    ok(format!(
        "ASK {{ FILTER(<http://ex/{balanced}> != <http://ex/>) }}"
    ));
    assert!(too_deep(&format!(
        "ASK {{ FILTER(<http://ex/{deep}> != <http://ex/>) }}"
    ))
    .is_some());
    ok(format!("ASK {{ FILTER(true) }} # {deep}"));
    ok(format!("ASK {{ # {deep}\n FILTER(true) }}"));
    // `\(` in a prefixed name is a character of the name
    let escaped = "\\(".repeat(200);
    ok(format!(
        "PREFIX ex: <http://ex/> ASK {{ FILTER(ex:a{escaped} != ex:b) }}"
    ));
    // `[]`, `()` and balanced brackets
    let flat = "{} ".repeat(1000);
    ok(format!("ASK {{ {flat} }}"));
}

/// The nesting scan finds deep nesting hidden behind ambiguous `<`, quotes and `#`.
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
    // the queries are valid
    for query in hiding(&shallow(3)) {
        assert!(run(query.clone()).is_ok(), "{query}");
    }
    for query in hiding(&shallow(MAX_SPARQL_NESTING)) {
        assert!(too_deep(&query).is_some(), "{query}");
    }
}

/// The parser does not decode `\u0028` outside strings, as the nesting scan assumes.
#[test]
fn the_parser_does_not_decode_escapes_outside_strings() {
    let error = graph()
        .sparql("SELECT * { FILTER \\u0028 true ) }")
        .unwrap_err();
    assert!(
        matches!(error, GraphError::Rdf(RdfError::SparqlSyntax(_))),
        "{error:?}"
    );
}

/// A long flat query runs on a stack of `sparql_stack_size`.
#[test]
fn long_flat_queries_run_on_a_sparql_stack() {
    // about 9,000 nested unions
    let query = format!("SELECT * {{ {{}}{} }}", "UNION{}".repeat(9_000));
    let stack = sparql_stack_size(query.len());
    assert_eq!(stack, (16 << 20) + query.len() * 8 * 1024);
    let results = std::thread::Builder::new()
        .stack_size(stack)
        .spawn(move || graph().sparql(&query))
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    match results {
        SparqlResults::Solutions { rows, .. } => assert_eq!(rows.len(), 9_001),
        other => panic!("{other:?}"),
    }
}
