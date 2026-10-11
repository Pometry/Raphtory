//! RDF import and export and SPARQL queries for Raphtory graphs.
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
//! let stats = rg.at(3).to_rdf(&mut as_of_3, RdfFormat::NTriples, &[]).unwrap();
//! assert_eq!(stats.written, 2);
//!
//! // Now only the age is left.
//! let mut now = Vec::new();
//! rg.valid().to_rdf(&mut now, RdfFormat::NTriples, &[]).unwrap();
//! assert_eq!(
//!     String::from_utf8(now).unwrap(),
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
//! The named graph `<raphtory:asof:T>` ([`TimeGraph`]) holds the triples visible as of `T`:
//! `GRAPH <raphtory:asof:T> { .. }` matches its patterns against `view.snapshot_at(T)`, so one
//! query can compare several times. `T` is epoch milliseconds (`raphtory:asof:1704067200000`)
//! or a date-time (`raphtory:asof:2024-01-01`, `raphtory:asof:2024-01-01T00:00:00Z`).
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
//! Queries can call four functions that read the history of a triple: [`VALID_FROM`]
//! (`raphtory:validFrom(?s, ?p, ?o)`) is the time since which a visible triple has held and
//! [`VALID_TO`] the time it stopped holding, as `xsd:dateTime` values; [`VALID_FROM_TIME`] and
//! [`VALID_TO_TIME`] are the same as Raphtory times (`xsd:integer`). An optional fourth argument
//! is the reference time, such as the `?g` of a time graph (see
//! [the rules](#the-rules-of-the-temporal-functions)).
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
//!             GRAPH raphtory:asof:2022-01-01 { ?who ex:worksFor ex:acme }
//!             BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, raphtory:asof:2022-01-01) AS ?since)
//!             BIND(raphtory:validTo(?who, ex:worksFor, ex:acme, raphtory:asof:2022-01-01) AS ?until)
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
//! Each function takes a triple `?s ?p ?o` and an optional reference time `?ref`, and returns
//! one end of the *validity interval* (the run) of the triple that contains the reference time:
//! `validFrom` when it started to hold and `validTo` when it was retracted. `validFrom` and
//! `validTo` return an `xsd:dateTime` (UTC, milliseconds), `validFromTime` and `validToTime` the
//! Raphtory time as an `xsd:integer`. [`SparqlOptions::with_temporal_functions`] turns them off.
//!
//! Arguments:
//!
//! - `?s ?p ?o` are the terms of the triple. SPARQL passes xsd value literals in canonical form,
//!   so a literal object is matched by canonical form among the objects of `?s` in layer `?p`
//!   (matching is by canonical value, so derived integer types such as `xsd:int` match
//!   `xsd:integer` and another timezone does not; several matching objects in the view give an
//!   unbound result). Pass the object bound by the pattern. A literal subject or predicate gives
//!   an unbound result.
//! - `?ref`, if given, is a time graph IRI `<raphtory:asof:T>` (see [`TimeGraph`]), an integer
//!   (epoch milliseconds) or an `xsd:dateTime` (UTC if it has no timezone). Without it, the
//!   reference time is the end of the view.
//!
//! Any other argument, or a call with other than 3 or 4 arguments, gives an unbound result. The
//! functions never make a query fail.
//!
//! Semantics: let `H` be the end of the view (no end for a view without a time window) and `R`
//! the persistent graph of the view without its window and filters, keeping only the events
//! before `H`.
//!
//! 1. **Scope.** The result is unbound unless the triple is visible in the view (without
//!    `?ref`) or in `view.snapshot_at(T)` (with `?ref` `T`), so hidden triples are never
//!    revealed. Same rules as `GRAPH <raphtory:asof:T>` on the view (see [`TimeGraph`]).
//! 2. **Run.** The largest interval `[from, to)` containing the reference time (capped at `H`)
//!    in which the triple is in `R.snapshot_at(t)`. `validTo` is unbound if the run is still
//!    open at `H` (so always without `?ref`). History before the window counts; events at or
//!    after `H` do not.
//! 3. **Holding.** A triple holds at `t` if its latest event at or before `t` (by time, then
//!    event id) is an assertion; a retraction and an assertion with the identical time and event
//!    id count as a retraction. So a duplicate assertion does not restart a run.
//! 4. **Return values.** `validFrom` and `validTo` are unbound outside the years 1 to 9999;
//!    `validFromTime` and `validToTime` still answer.
//!
//! Functions cannot see the active `GRAPH` or `FROM`: a 3-argument call inside
//! `GRAPH <raphtory:asof:T> { .. }` uses the present of the view, not `T`. Pass the graph as
//! `?ref` instead, in a `BIND` after the `GRAPH` pattern (inside it `?g` is not bound yet):
//!
//! ```sparql
//! SELECT ?who ?since ?until FROM NAMED raphtory:asof:2023-01-01 {
//!   GRAPH ?g { ?who ex:worksFor ex:acme }
//!   BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, ?g) AS ?since)
//!   BIND(raphtory:validTo(?who, ex:worksFor, ex:acme, ?g) AS ?until)
//! }
//! ```
//!
//! Results are computed when the function is called, with no snapshot isolation. For a
//! consistent result, wrap a view of `graph.read_only()`, e.g.
//! `RdfGraph::from(rg.graph().read_only())` (writers wait until it is dropped).
//!
//! # Serialized results
//!
//! [`RdfView::sparql_to_writer`] streams the results of a query in a SPARQL results format
//! (JSON, XML, CSV or TSV) for `SELECT` and `ASK`, or an RDF format for `CONSTRUCT` and
//! `DESCRIBE`; [`SparqlResults::write`] writes collected results the same way. The output holds
//! RDF terms, not Raphtory names (use [`name_of`] to get a name back).
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
#[cfg(feature = "shacl")]
pub mod shacl;
mod time_graph;

#[cfg(test)]
mod tests;

pub use error::RdfError;
#[cfg(feature = "python")]
pub(crate) use export::serializer_with_prefixes;
pub use format::{QueryResultsFormat, RdfFormat};
pub use functions::{VALID_FROM, VALID_FROM_TIME, VALID_TO, VALID_TO_TIME};
pub use graph::{RdfGraph, RdfView};
#[cfg(feature = "python")]
pub(crate) use mapping::literal_to_prop;
pub use mapping::{name_of, term_of, RESERVED_NS};
pub use oxigraph::{
    model::{Literal, NamedNode, Term, TermRef, Triple, TripleRef},
    sparql::Variable,
};
pub use query::{
    QueryForm, QueryOutline, SparqlDataset, SparqlOptions, SparqlResults, MAX_SPARQL_NESTING,
};
pub use results::SparqlFormat;
pub use time_graph::{TimeGraph, ASOF_NS};

/// Parses an RDF format from a file extension, a format name or a media type.
///
/// Accepts for example `"ttl"`, `".ttl"`, `"turtle"`, `"text/turtle"`, `"nt"`, `"n-triples"`,
/// `"nq"`, `"trig"`, `"rdf"`, `"xml"`, `"jsonld"` and `"n3"` (case-insensitive).
pub(crate) fn parse_rdf_format(s: &str) -> Result<RdfFormat, RdfError> {
    let t = s.trim();
    let t = t.strip_prefix('.').unwrap_or(t);
    RdfFormat::from_extension(t)
        .or_else(|| RdfFormat::from_media_type(t))
        .or_else(|| RdfFormat::from_media_type(&format!("application/{t}")))
        .ok_or_else(|| RdfError::UnknownFormat(s.to_owned()))
}

/// Counts returned by [`RdfView::to_rdf`], [`RdfView::sparql_to_writer`] and
/// [`SparqlResults::write`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Number of solutions (`SELECT`) or triples written; 0 for `ASK`.
    pub written: usize,
    /// Number of triples [`to_rdf`](RdfView::to_rdf) skipped: the `(edge, layer)` pairs that
    /// are not valid RDF triples (a literal subject or a layer whose term is not an IRI) and, in
    /// RDF/XML, the triples it cannot write; 0 for SPARQL results.
    pub skipped: usize,
}
