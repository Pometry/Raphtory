//! The refusal of SPARQL `SERVICE` calls, and triple terms built by SPARQL 1.2 queries.
use crate::{QueryResultsFormat, RdfError, RdfFormat, RdfGraph, SparqlFormat, SparqlOptions};
use oxigraph::sparql::QueryEvaluationError;
use raphtory::prelude::*;

fn small_graph() -> RdfGraph {
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, Some("p")).unwrap();
    g.into()
}

#[test]
fn service_is_refused_by_sparql() {
    let g = small_graph();
    let query = "SELECT * { SERVICE <http://example.org/sparql> { ?s ?p ?o } }";
    let error = g.sparql(query, &SparqlOptions::default()).unwrap_err();
    assert!(
        matches!(
            &error,
            RdfError::SparqlEvaluation(QueryEvaluationError::UnsupportedService(_))
        ),
        "{error:?}"
    );
}

/// A triple term built by a query (`TRIPLE(..)`, SPARQL 1.2) is never written as XML.
#[test]
fn triple_terms_built_by_queries() {
    let g = small_graph();
    let write = |query: &str, format: SparqlFormat| {
        let mut out = Vec::new();
        g.sparql_to_writer(query, &mut out, format, &SparqlOptions::default())
    };
    let select = "SELECT (TRIPLE(?s, ?p, \"a\\u0001b\") AS ?t) { ?s ?p ?o }";
    let error = write(select, QueryResultsFormat::Xml.into()).unwrap_err();
    assert!(matches!(&error, RdfError::XmlTripleTerm(_)), "{error:?}");
    let construct = "CONSTRUCT { ?s ?p <<( ?s ?p ?o )>> } WHERE { ?s ?p ?o }";
    let error = write(construct, RdfFormat::RdfXml.into()).unwrap_err();
    assert!(
        matches!(
            error,
            RdfError::RdfXmlSkipped {
                skipped: 1,
                triples: 1
            }
        ),
        "{error:?}"
    );
}
