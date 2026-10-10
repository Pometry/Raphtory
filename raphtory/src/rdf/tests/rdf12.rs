//! The refusal of SPARQL `SERVICE` calls, and triple terms built by SPARQL 1.2 queries (enabled
//! by the `shacl` feature).
use crate::{
    errors::GraphError,
    prelude::*,
    rdf::{RdfError, SparqlFormat},
};
use oxigraph::sparql::QueryEvaluationError;
use std::{
    io::Read,
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

/// Whether this build parses SPARQL 1.2.
fn sparql12() -> bool {
    spargebra::SparqlParser::new()
        .parse_query("SELECT * { ?s ?p <<( ?a ?b ?c )>> }")
        .is_ok()
}

fn rdf_error(error: GraphError) -> RdfError {
    match error {
        GraphError::Rdf(error) => error,
        other => panic!("not an RDF error: {other:?}"),
    }
}

/// Accepts connections on a local port and counts them.
fn listener() -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            counter.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut stream) = stream {
                let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                let _ = stream.read(&mut [0; 1024]);
            }
        }
    });
    (port, hits)
}

/// Gives a connection that a request may have started time to be accepted.
fn connections(hits: &AtomicUsize) -> usize {
    std::thread::sleep(Duration::from_millis(300));
    hits.load(Ordering::SeqCst)
}

fn small_graph() -> RdfGraph {
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, Some("p")).unwrap();
    g.into()
}

#[test]
fn service_is_refused_by_sparql() {
    let g = small_graph();
    let (port, hits) = listener();
    let query = format!("SELECT * {{ SERVICE <http://127.0.0.1:{port}/sparql> {{ ?s ?p ?o }} }}");
    let error = rdf_error(g.sparql(&query).unwrap_err());
    assert!(
        matches!(
            &error,
            RdfError::SparqlEvaluation(QueryEvaluationError::Service(_))
        ),
        "{error:?}"
    );
    assert_eq!(connections(&hits), 0);
}

/// A triple term built by a query (`TRIPLE(..)`, SPARQL 1.2) is never written as XML: its nested
/// literals are not checked for characters XML cannot hold.
#[test]
fn triple_terms_built_by_queries() {
    if !sparql12() {
        return;
    }
    let g = small_graph();
    let write = |query: &str, format: &str| {
        let mut out = Vec::new();
        g.sparql_to_writer(query, &mut out, SparqlFormat::parse(format).unwrap())
    };
    let select = "SELECT (TRIPLE(?s, ?p, \"a\\u0001b\") AS ?t) { ?s ?p ?o }";
    let error = rdf_error(write(select, "xml").unwrap_err());
    assert!(
        matches!(&error, RdfError::XmlTripleTerm(t) if t.starts_with("<<(")),
        "{error:?}"
    );
    let construct = "CONSTRUCT { ?s ?p <<( ?s ?p ?o )>> } WHERE { ?s ?p ?o }";
    let stats = write(construct, "rdf").unwrap();
    assert_eq!((stats.written, stats.skipped), (0, 1));
}
