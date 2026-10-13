//! RDF import and export, SPARQL queries and SHACL validation for Raphtory graphs.
//!
//! RDF terms are oxigraph's model types, re-exported here by name ([`Term`], [`Triple`],
//! [`NamedNode`], ...). Formats ([`RdfFormat`], [`QueryResultsFormat`]) and errors
//! ([`RdfError`]) are Raphtory's.
//!
//! # The model
//!
//! 1. **Triples are edges.** Each triple `s p o` is stored as an edge `name(s) -> name(o)` in
//!    the layer of the predicate IRI `p`.
//! 2. **Names.** [`term_of`] is injective; [`name_of`] is its checked inverse (`None` for terms
//!    no name maps to). IRIs are kept as written, blank nodes are named `_:label`, literal
//!    objects are nodes named by their exact N-Triples form (so identical literals share a
//!    node), and any other node name `n` is the IRI `<raphtory:pct_encode(n)>` (see
//!    [`RESERVED_NS`]). Layer names map the same way, except that layer names must be safe as
//!    file names: the layer of a predicate IRI is the IRI with `%`, `/`, `:`, `?` and `*`
//!    percent-escaped (`<http://ex/knows>` is the layer `http%3A%2F%2Fex%2Fknows`, and the layer
//!    `knows` is `raphtory:knows`), and writes reject a `raphtory:` predicate whose layer name
//!    would be blank or contain a control character or any of `/ \ : * ? " < > |`
//!    ([`RdfError::UnsafeLayerName`]).
//! 3. **Writes are timestamped.** [`RdfGraph`] holds the graph with persistent semantics:
//!    asserting a triple at `t` is `add_edge(t, ..)` and retracting it at `t` is
//!    `delete_edge(t, ..)`, which ends it. An `RdfGraph` made from an event
//!    [`Graph`](raphtory::prelude::Graph) shares its storage.
//! 4. **The view decides visibility.** SPARQL, export and SHACL see exactly the `(edge, layer)`
//!    pairs visible in an [`RdfView`] (see [`RdfGraph`] for what each view shows).
//! 5. **Only the default graph stores data.** Named graphs are rejected on import. The only
//!    named graphs a query sees are the virtual, read-only time graphs `<raphtory:asof:T>`
//!    ([`TimeGraph`]).
//!
//! # Example
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory_rdf::{RdfFormat, RdfGraph};
//!
//! let rg = RdfGraph::new();
//! let doc = r#"
//!     @prefix ex: <http://ex/> .
//!     ex:alice ex:knows ex:bob ; ex:age 42 .
//! "#;
//! rg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
//! rg.retract_rdf(
//!     5,
//!     "<http://ex/alice> <http://ex/knows> <http://ex/bob> .".as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//!
//! // Literal objects are nodes named by their N-Triples form.
//! assert!(rg
//!     .graph()
//!     .node("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
//!     .is_some());
//!
//! // As of t = 3 both triples hold.
//! let mut as_of_3 = Vec::new();
//! let stats = rg.at(3).to_rdf(&mut as_of_3, RdfFormat::NTriples).unwrap();
//! assert_eq!(stats.written, 2);
//!
//! // Now only the age is left.
//! assert_eq!(
//!     rg.valid().to_rdf_string(RdfFormat::NTriples).unwrap(),
//!     "<http://ex/alice> <http://ex/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
//! );
//! ```
//!
//! # SPARQL
//!
//! [`RdfView::sparql`] runs a SPARQL 1.1 query on the triples visible in a view, so time
//! travel is picking the view. The `raphtory:` prefix is registered, so the node `Alice` of a
//! graph that was not loaded from RDF is `raphtory:Alice` (see [`term_of`]).
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults, Term};
//!
//! let rg = RdfGraph::new();
//! let doc = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
//! rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! rg.retract_rdf(5, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//!
//! let options = SparqlOptions::default();
//! let query = "SELECT ?who { ?who <http://ex/worksFor> <http://ex/acme> }";
//! // as of t = 3
//! let SparqlResults::Solutions { rows, .. } = rg.at(3).sparql(query, &options).unwrap() else {
//!     unreachable!()
//! };
//! assert_eq!(rows.len(), 1);
//! assert_eq!(rows[0][0].as_ref().map(Term::to_string).unwrap(), "<http://ex/alice>");
//! // now
//! let SparqlResults::Solutions { rows, .. } = rg.valid().sparql(query, &options).unwrap() else {
//!     unreachable!()
//! };
//! assert!(rows.is_empty());
//!
//! // graphs that were not loaded from RDF can be queried too
//! let g = Graph::new();
//! g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
//! assert_eq!(
//!     RdfGraph::from(g)
//!         .sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }", &options)
//!         .unwrap(),
//!     SparqlResults::Boolean(true)
//! );
//! ```
//!
//! # Time travel inside a query
//!
//! The named graph `<raphtory:asof:T>` ([`TimeGraph`]) holds the triples of the view that hold at
//! `T`: `GRAPH <raphtory:asof:T> { .. }` matches its patterns against
//! `view.before(T + 1).valid()`, so one query can compare several times. `T` is epoch
//! milliseconds (`raphtory:asof:1704067200000`) or a date-time (`raphtory:asof:2024-01-01`,
//! `raphtory:asof:2024-01-01T00:00:00Z`).
//!
//! - `FROM <raphtory:asof:T>` makes a time graph the default graph. With several `FROM`
//!   clauses spareval concatenates the graphs instead of merging them, so a triple visible in
//!   several of them is matched once per graph. Use `SELECT DISTINCT` / `COUNT(DISTINCT ..)`,
//!   or a single `FROM` / `GRAPH`.
//! - Time graphs cannot be enumerated: see [`TimeGraph`] for which ones `GRAPH ?g { .. }` with
//!   an unbound `?g` visits.
//!
//! ```
//! use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults};
//!
//! let rg = RdfGraph::new();
//! let works_for =
//!     |who: &str, org: &str| format!("<http://ex/{who}> <http://ex/worksFor> <http://ex/{org}> .");
//! let load = |t: &str, doc: String| rg.load_rdf(t, doc.as_bytes(), RdfFormat::NTriples, None);
//! load("2021-06-01", works_for("alice", "acme")).unwrap();
//! load("2022-03-01", works_for("bob", "acme")).unwrap();
//! // alice moves to initech
//! rg.retract_rdf(
//!     "2023-06-01",
//!     works_for("alice", "acme").as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//! load("2023-06-01", works_for("alice", "initech")).unwrap();
//!
//! let rows = |query: &str| -> Vec<Vec<String>> {
//!     let SparqlResults::Solutions { rows, .. } =
//!         rg.sparql(query, &SparqlOptions::default()).unwrap()
//!     else {
//!         unreachable!()
//!     };
//!     rows.iter()
//!         .map(|row| row.iter().map(|v| v.as_ref().unwrap().to_string()).collect())
//!         .collect()
//! };
//!
//! // Who worked for acme at the start of 2023, and for someone else a year later?
//! let moved = rows(
//!     "SELECT ?who ?new {
//!         GRAPH raphtory:asof:2023-01-01 { ?who <http://ex/worksFor> <http://ex/acme> }
//!         GRAPH raphtory:asof:2024-01-01 {
//!             ?who <http://ex/worksFor> ?new FILTER(?new != <http://ex/acme>)
//!         }
//!     }",
//! );
//! assert_eq!(moved, [["<http://ex/alice>", "<http://ex/initech>"]]);
//! ```
//!
//! # Since when? Temporal functions
//!
//! Queries can call four functions that read the history of a triple:
//! `raphtory:validFrom(?s, ?p, ?o)` is when its latest run started and `raphtory:validTo` the
//! retraction that ended it, as `xsd:dateTime` values; `raphtory:validFromTime` and
//! `raphtory:validToTime` are the same as Raphtory times (`xsd:integer`). An optional fourth
//! argument is a time graph (see [the rules](#the-rules-of-the-temporal-functions)).
//!
//! ```
//! use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults, Term};
//!
//! let rg = RdfGraph::new();
//! let doc = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
//! rg.load_rdf("2021-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! rg.retract_rdf("2023-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//!
//! let SparqlResults::Solutions { rows, .. } = rg
//!     .sparql(
//!         "PREFIX ex: <http://ex/>
//!         SELECT ?since ?until {
//!             ?who ex:worksFor ex:acme
//!             BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme) AS ?since)
//!             BIND(raphtory:validTo(?who, ex:worksFor, ex:acme) AS ?until)
//!         }",
//!         &SparqlOptions::default(),
//!     )
//!     .unwrap()
//! else {
//!     unreachable!()
//! };
//! let dt = |v: &str| format!("\"{v}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>");
//! assert_eq!(
//!     rows[0].iter().map(|v| v.as_ref().map(Term::to_string)).collect::<Vec<_>>(),
//!     [Some(dt("2021-06-01T00:00:00Z")), Some(dt("2023-06-01T00:00:00Z"))]
//! );
//! ```
//!
//! ## The rules of the temporal functions
//!
//! `raphtory:validFrom(?s, ?p, ?o [, ?g])` and the other functions follow Raphtory's
//! persistent semantics on their *scope*: the view the query runs on or, with `?g` a time graph
//! `<raphtory:asof:T>`, that time graph (`view.before(T + 1).valid()`, see [`TimeGraph`]).
//!
//! - The result is unbound unless the triple is visible in the scope.
//! - `validFrom` is when the latest run of the triple in the scope started, or the start of the
//!   scope if the triple already held there (unbound if the triple never held in the scope).
//!   `validTo` is the retraction that ended that run, unbound if the triple still holds at the
//!   end of the scope.
//! - Asserting a triple that holds does not restart its run; retracting a triple that does not
//!   hold changes nothing. Events at the same time are ordered by event id.
//! - `validFrom` and `validTo` are `xsd:dateTime` values (UTC, milliseconds), unbound outside the
//!   years 1 to 9999; `validFromTime` and `validToTime` are `xsd:integer` Raphtory times.
//! - SPARQL passes xsd value literals in canonical form, so a literal object is matched by
//!   canonical form among the objects of `?s` in layer `?p` (several matches give an unbound
//!   result). A subject or predicate that is an xsd value literal (a number, boolean,
//!   date-time, ...), a fourth argument that is not a time graph, or another number of
//!   arguments gives an unbound result: the functions never make a query fail.
//!
//! Functions cannot see the active `GRAPH` or `FROM`: inside `GRAPH <raphtory:asof:T> { .. }` a
//! 3-argument call answers for the view, not the time graph. Pass the graph as `?g`, in a `BIND`
//! after the `GRAPH` pattern (inside it `?g` is not bound yet):
//!
//! ```sparql
//! SELECT ?who ?since FROM NAMED raphtory:asof:2023-01-01 {
//!   GRAPH ?g { ?who ex:worksFor ex:acme }
//!   BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, ?g) AS ?since)
//! }
//! ```
//!
//! # Serialized results
//!
//! [`RdfView::sparql_to_writer`] streams the results of a query in a SPARQL results format
//! (JSON, XML, CSV or TSV) for `SELECT` and `ASK`, or an RDF format for `CONSTRUCT` and
//! `DESCRIBE`. The output holds RDF terms, not Raphtory names (use [`name_of`] or
//! [`term_name`] to get a name back).
mod algebra;
mod dataset;
mod error;
mod export;
mod format;
mod functions;
mod graph;
mod import;
mod limits;
mod mapping;
#[cfg(feature = "python")]
pub mod python;
mod query;
mod results;
mod scan;
pub mod shacl;
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
pub use query::{QueryForm, QueryOutline, SparqlOptions, SparqlResults, MAX_SPARQL_NESTING};
pub use results::SparqlFormat;
pub use time_graph::{TimeGraph, ASOF_NS};

/// Counts returned by [`RdfView::to_rdf`] and [`RdfView::sparql_to_writer`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Number of solutions (`SELECT`) or triples written; 0 for `ASK`.
    pub written: usize,
    /// Number of triples [`to_rdf`](RdfView::to_rdf) skipped: the `(edge, layer)` pairs that
    /// are not valid RDF triples (a literal subject or a layer whose term is not an IRI) and, in
    /// RDF/XML, the triples it cannot write; 0 for SPARQL results.
    pub skipped: usize,
}
