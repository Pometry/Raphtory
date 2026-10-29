//! RDF import and export and SPARQL 1.1 queries for Raphtory graphs.
//! A triple `s p o` is an edge `name(s) -> name(o)` in layer `p` ([`term_of`], [`RdfGraph`]).
//!
//! ```
//! use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults, Term};
//!
//! let rg = RdfGraph::new();
//! let doc = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
//! rg.load_rdf("2021-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! rg.retract_rdf("2023-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! assert_eq!(rg.valid().to_rdf_string(RdfFormat::NTriples).unwrap(), "");
//!
//! let query = "SELECT ?who ?until {
//!     GRAPH raphtory:asof:2022-01-01 { ?who <http://ex/worksFor> ?org }
//!     BIND(raphtory:validTo(?who, <http://ex/worksFor>, ?org) AS ?until)
//! }";
//! let options = SparqlOptions::default();
//! let SparqlResults::Solutions { rows, .. } = rg.sparql(query, &options).unwrap() else {
//!     unreachable!()
//! };
//! let row: Vec<_> = rows[0].iter().map(|v| v.as_ref().map(Term::to_string)).collect();
//! assert_eq!(
//!     row,
//!     [
//!         Some("<http://ex/alice>".to_owned()),
//!         Some("\"2023-06-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>".to_owned())
//!     ]
//! );
//! ```
mod algebra;
mod dataset;
mod error;
mod export;
mod format;
mod functions;
mod graph;
mod limits;
mod mapping;
#[cfg(feature = "python")]
pub mod python;
mod query;
mod results;
mod scan;
mod time_graph;

#[cfg(test)]
mod tests;

pub use error::RdfError;
pub use format::{QueryResultsFormat, RdfFormat};
pub use graph::{RdfGraph, RdfView};
pub use mapping::{name_of, term_name, term_of, RESERVED_NS};
pub use oxigraph::{
    model::{Literal, NamedNode, Term, TermRef, Triple, TripleRef},
    sparql::Variable,
};
pub use query::{QueryForm, QueryOutline, SparqlOptions, SparqlResults};
pub use results::SparqlFormat;
pub use time_graph::{TimeGraph, ASOF_NS};

/// Counts returned by [`RdfView::to_rdf`] and [`RdfView::sparql_to_writer`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Solutions (`SELECT`) or triples written; 0 for `ASK`.
    pub written: usize,
    /// Triples [`to_rdf`](RdfView::to_rdf) skipped because their subject is a literal.
    pub skipped: usize,
}
