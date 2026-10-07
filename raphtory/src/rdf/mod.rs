//! RDF import and export and SPARQL queries for Raphtory graphs (feature `rdf`).
//!
//! # The model
//!
//! 1. **Triples are edges.** Each triple `s p o` is stored as an edge `name(s) -> name(o)` in
//!    the layer named `name(p)`.
//! 2. **One mapping for names.** [`name_of`] and [`term_of`] are one exact bijection between RDF
//!    terms and Raphtory names, used for both node names and layer names. IRIs are kept as
//!    written, blank nodes are named `_:label`, literal objects are nodes named by their exact
//!    N-Triples form (so identical literals share a node), and any other Raphtory name `n` is the
//!    IRI `<raphtory:pct_encode(n)>` (see [`RESERVED_NS`]).
//! 3. **Writes are timestamped.** Asserting a triple at `t` is `add_edge(t, ..)`; retracting it
//!    at `t` is `delete_edge(t, ..)`. This is the same on [`Graph`](crate::prelude::Graph) and
//!    [`PersistentGraph`](crate::prelude::PersistentGraph).
//! 4. **The view decides visibility.** SPARQL and export see exactly the `(edge, layer)` pairs
//!    visible in `view.valid()`. "As of `t`" means picking the view
//!    `persistent_graph.snapshot_at(t)`.
//! 5. **Only the default graph stores data.** Named graphs are rejected on import. The only
//!    named graphs a query sees are the virtual, read-only time graphs `<raphtory:asof:T>`
//!    ([`TimeGraph`]), the same view as `snapshot_at(T)`.
//!
//! # Example
//!
//! ```
//! use raphtory::{prelude::*, rdf::RdfFormat};
//!
//! let pg = PersistentGraph::new();
//! let doc = r#"
//!     @prefix ex: <http://ex/> .
//!     ex:alice ex:knows ex:bob ; ex:age 42 .
//! "#;
//! pg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
//! pg.retract_rdf(
//!     5,
//!     "<http://ex/alice> <http://ex/knows> <http://ex/bob> .".as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//!
//! // Literal objects are nodes named by their N-Triples form.
//! assert!(pg
//!     .node("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
//!     .is_some());
//!
//! // As of t = 3 both triples hold.
//! let mut as_of_3 = Vec::new();
//! let stats = pg.snapshot_at(3).to_rdf(&mut as_of_3, RdfFormat::NTriples).unwrap();
//! assert_eq!(stats.triples, 2);
//!
//! // Now only the age is left.
//! let mut now = Vec::new();
//! pg.to_rdf(&mut now, RdfFormat::NTriples).unwrap();
//! assert_eq!(
//!     String::from_utf8(now).unwrap(),
//!     "<http://ex/alice> <http://ex/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
//! );
//! ```
//!
//! # SPARQL
//!
//! [`RdfViewOps::sparql`] runs a SPARQL 1.1 query on the triples visible in a view, so time
//! travel is picking the view. The `raphtory:` prefix is registered, so the node `Alice` of a
//! graph that was not loaded from RDF is `raphtory:Alice` (see [`term_of`]).
//!
//! ```
//! use raphtory::{
//!     prelude::*,
//!     rdf::{model::Term, RdfFormat, SparqlResults, Variable},
//! };
//!
//! let pg = PersistentGraph::new();
//! let doc = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
//! pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! pg.retract_rdf(5, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//!
//! let query = "SELECT ?who { ?who <http://ex/worksFor> <http://ex/acme> }";
//! // as of t = 3
//! let SparqlResults::Solutions { rows, .. } = pg.snapshot_at(3).sparql(query).unwrap() else {
//!     unreachable!()
//! };
//! assert_eq!(rows.len(), 1);
//! assert_eq!(rows[0][0].as_ref().map(Term::to_string).unwrap(), "<http://ex/alice>");
//! // now
//! assert_eq!(
//!     pg.sparql(query).unwrap(),
//!     SparqlResults::Solutions {
//!         variables: vec![Variable::new("who").unwrap()],
//!         rows: vec![],
//!     }
//! );
//!
//! // graphs that were not loaded from RDF can be queried too
//! let g = Graph::new();
//! g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
//! assert_eq!(
//!     g.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }").unwrap(),
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
//! - Time graphs cannot be enumerated, so `GRAPH ?g { .. }` with an unbound `?g` visits only the
//!   time graphs the query names: the `FROM NAMED` ones (as in the head count below) or,
//!   without `FROM NAMED`, the ones it writes as constants, for example in
//!   `VALUES ?g { raphtory:asof:2023-01-01 raphtory:asof:2024-01-01 }`. A time graph IRI built
//!   from other values (`IRI(CONCAT("raphtory:asof:", ?t))`) is not a constant: a `GRAPH ?g`
//!   pattern only reliably matches it inside `LATERAL { .. }` after the binding (see
//!   [`TimeGraph`]).
//!
//! ```
//! use raphtory::{
//!     prelude::*,
//!     rdf::{RdfFormat, SparqlResults},
//! };
//!
//! let pg = PersistentGraph::new();
//! let works_for =
//!     |who: &str, org: &str| format!("<http://ex/{who}> <http://ex/worksFor> <http://ex/{org}> .");
//! let load = |t: &str, doc: String| pg.load_rdf(t, doc.as_bytes(), RdfFormat::NTriples, None);
//! load("2021-06-01", works_for("alice", "acme")).unwrap();
//! load("2022-03-01", works_for("bob", "acme")).unwrap();
//! // alice moves to initech
//! pg.retract_rdf(
//!     "2023-06-01",
//!     works_for("alice", "acme").as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//! load("2023-06-01", works_for("alice", "initech")).unwrap();
//!
//! let rows = |query: &str| -> Vec<Vec<String>> {
//!     let SparqlResults::Solutions { rows, .. } = pg.sparql(query).unwrap() else {
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
//!
//! // The head count of acme at the start of each year
//! let head_count = rows(
//!     "SELECT ?year (COUNT(*) AS ?n)
//!     FROM NAMED raphtory:asof:2022-01-01
//!     FROM NAMED raphtory:asof:2023-01-01
//!     FROM NAMED raphtory:asof:2024-01-01
//!     { GRAPH ?year { ?who <http://ex/worksFor> <http://ex/acme> } }
//!     GROUP BY ?year ORDER BY ?year",
//! );
//! let n = |n: u32| format!("\"{n}\"^^<http://www.w3.org/2001/XMLSchema#integer>");
//! assert_eq!(
//!     head_count,
//!     [
//!         ["<raphtory:asof:2022-01-01>".to_owned(), n(1)],
//!         ["<raphtory:asof:2023-01-01>".to_owned(), n(2)],
//!         ["<raphtory:asof:2024-01-01>".to_owned(), n(1)],
//!     ]
//! );
//! ```
//!
//! # Since when? Temporal functions
//!
//! Queries can call four functions that read the history of a triple: [`VALID_FROM`]
//! (`raphtory:validFrom(?s, ?p, ?o)`) is the time since which a visible triple has held and
//! [`VALID_TO`] the time it stopped holding, as `xsd:dateTime` values; [`VALID_FROM_TIME`] and
//! [`VALID_TO_TIME`] are the same as Raphtory times (`xsd:integer`). An optional fourth argument
//! is the reference time, such as the `?g` of a time graph. See [`with_temporal_functions`] for
//! the rules.
//!
//! ```
//! use raphtory::{
//!     prelude::*,
//!     rdf::{model::Term, RdfFormat, SparqlResults},
//! };
//!
//! let pg = PersistentGraph::new();
//! let doc = "<http://ex/alice> <http://ex/worksFor> <http://ex/acme> .";
//! pg.load_rdf("2021-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//! pg.retract_rdf("2023-06-01", doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
//!
//! let SparqlResults::Solutions { rows, .. } = pg
//!     .sparql(
//!         "PREFIX ex: <http://ex/>
//!         SELECT ?since ?until {
//!             GRAPH raphtory:asof:2022-01-01 { ?who ex:worksFor ex:acme }
//!             BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, raphtory:asof:2022-01-01) AS ?since)
//!             BIND(raphtory:validTo(?who, ex:worksFor, ex:acme, raphtory:asof:2022-01-01) AS ?until)
//!         }",
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
//! # Serialized results
//!
//! [`RdfViewOps::sparql_to_writer`] streams the results of a query in a SPARQL results format
//! (JSON, XML, CSV or TSV) for `SELECT` and `ASK`, or an RDF format for `CONSTRUCT` and
//! `DESCRIBE`; [`SparqlResults::write`] writes collected results the same way. The output holds
//! RDF terms, not Raphtory names (use [`name_of`] to get a name back).
//!
//! ```
//! use raphtory::{
//!     prelude::*,
//!     rdf::{RdfViewOps, SparqlFormat},
//! };
//!
//! let g = Graph::new();
//! g.add_edge(1, "Alice", "Bob Smith", NO_PROPS, Some("knows")).unwrap();
//! let query = "SELECT ?s ?o { ?s raphtory:knows ?o }";
//!
//! let mut tsv = Vec::new();
//! g.sparql_to_writer(query, &mut tsv, SparqlFormat::parse("tsv").unwrap())
//!     .unwrap();
//! assert_eq!(
//!     String::from_utf8(tsv).unwrap(),
//!     "?s\t?o\n<raphtory:Alice>\t<raphtory:Bob%20Smith>\n"
//! );
//!
//! // the collected results write the same bytes
//! let mut collected = Vec::new();
//! g.sparql(query)
//!     .unwrap()
//!     .write(&mut collected, SparqlFormat::parse("tsv").unwrap())
//!     .unwrap();
//! let mut streamed = Vec::new();
//! g.sparql_to_writer(query, &mut streamed, SparqlFormat::parse("tsv").unwrap())
//!     .unwrap();
//! assert_eq!(collected, streamed);
//! ```
//!
//! For more control (custom functions, cancellation, streaming results as terms), use
//! [`evaluator`] with a [`RaphtoryDataset`] (and [`with_temporal_functions`] for the temporal
//! functions).
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::{AdditionOps, DeletionOps},
};
use raphtory_api::core::utils::time::TryIntoInputTime;
use std::io::{Read, Write};

mod dataset;
mod error;
mod export;
mod functions;
mod import;
mod limits;
mod mapping;
mod query;
mod results;
pub(crate) mod scan;
#[cfg(feature = "shacl")]
pub mod shacl;
mod time_graph;

#[cfg(test)]
mod tests;

pub use dataset::{RaphtoryDataset, RdfTerm};
pub use error::RdfError;
pub use export::serializer_with_prefixes;
pub use functions::{
    with_temporal_functions, VALID_FROM, VALID_FROM_TIME, VALID_TO, VALID_TO_TIME,
};
pub use mapping::{literal_to_prop, name_of, term_of, RESERVED_NS};
pub use oxigraph::{
    io::{RdfFormat, RdfParser, RdfSerializer},
    model,
    sparql::{results::QueryResultsFormat, CancellationToken, QueryResults, Variable},
};
pub use query::{
    evaluator, sparql_stack_size, timeout_from_secs, SparqlDataset, SparqlOptions, SparqlResults,
    MAX_SPARQL_NESTING,
};
pub use results::{SparqlFormat, SparqlWriteStats};
pub use time_graph::{TimeGraph, ASOF_NS};

/// Parses an RDF format from a file extension, a format name or a media type.
///
/// Accepts for example `"ttl"`, `".ttl"`, `"turtle"`, `"text/turtle"`, `"nt"`, `"n-triples"`,
/// `"nq"`, `"trig"`, `"rdf"`, `"xml"`, `"jsonld"` and `"n3"` (case-insensitive).
pub fn parse_rdf_format(s: &str) -> Result<RdfFormat, RdfError> {
    let t = s.trim();
    let t = t.strip_prefix('.').unwrap_or(t);
    // `RdfFormat::from_media_type` panics on a media-type parameter whose value is a lone `"`
    // (it strips the quotes of `profile="…"` by slicing), so reject those first.
    let lone_quote = t.split(';').skip(1).any(|parameter| {
        parameter
            .split_once('=')
            .is_some_and(|(_, v)| v.trim() == "\"")
    });
    if lone_quote {
        return Err(RdfError::UnknownFormat(s.to_owned()));
    }
    RdfFormat::from_extension(t)
        .or_else(|| RdfFormat::from_media_type(t))
        .or_else(|| RdfFormat::from_media_type(&format!("application/{t}")))
        .ok_or_else(|| RdfError::UnknownFormat(s.to_owned()))
}

/// Counts returned by [`RdfViewOps::to_rdf`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RdfExportStats {
    /// Number of triples written.
    pub triples: usize,
    /// Number of `(edge, layer)` pairs skipped because they are not valid RDF triples (a
    /// literal subject or a layer whose term is not an IRI), or because the format cannot write
    /// them (see [`RdfViewOps::to_rdf`]).
    pub skipped: usize,
}

/// Writing RDF triples to a graph. Implemented for [`Graph`](crate::prelude::Graph) and
/// [`PersistentGraph`](crate::prelude::PersistentGraph).
///
/// Every triple becomes one edge event in the layer named by its predicate, at `time` (any time
/// `add_edge` accepts). Triples are written in document order, so for equal `t` the later write
/// wins.
///
/// Writes need a graph with string node ids (or an empty graph); a graph with u64 ids gives
/// [`RdfError::NonStringIds`] before anything is written. A term that cannot be stored (an IRI
/// under `raphtory:` that is not the canonical encoding of a name) gives
/// [`RdfError::NonCanonicalTerm`].
///
/// All writes go through the normal `add_edge`/`delete_edge` calls, so they are safe to run
/// while other threads query or write the graph.
pub trait RdfMutationOps: AdditionOps + DeletionOps {
    /// Asserts every triple of the document at `time` and returns the number of triples read.
    ///
    /// Blank nodes are renamed to fresh random labels, so two loads never collide. Named graphs
    /// are rejected ([`RdfError::Parse`]). The load streams and is not atomic: triples before
    /// an error stay in the graph.
    fn load_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<usize, GraphError> {
        let time = TryIntoInputTime::try_into_input_time(time)?;
        import::read_document(self, time, data, format, base_iri, false)
    }

    /// Retracts every triple of the document at `time` and returns the number of triples read.
    ///
    /// Blank-node labels are used as written, so a retraction document must use the stored
    /// labels (as returned by [`RdfViewOps::to_rdf`]). A retraction is always recorded, even if
    /// the triple was never asserted.
    fn retract_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<usize, GraphError> {
        let time = TryIntoInputTime::try_into_input_time(time)?;
        import::read_document(self, time, data, format, base_iri, true)
    }

    /// Asserts one triple at `time`. Blank-node labels are used as written.
    fn add_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<model::TripleRef<'a>>,
    ) -> Result<(), GraphError> {
        let time = TryIntoInputTime::try_into_input_time(time)?;
        import::write_single(self, time, triple.into(), false)
    }

    /// Retracts one triple at `time`. Blank-node labels are used as written.
    fn delete_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<model::TripleRef<'a>>,
    ) -> Result<(), GraphError> {
        let time = TryIntoInputTime::try_into_input_time(time)?;
        import::write_single(self, time, triple.into(), true)
    }
}

impl<G: AdditionOps + DeletionOps> RdfMutationOps for G {}

/// Reading RDF from any graph view: export and SPARQL queries.
///
/// Both see the same triples: one per `(edge, layer)` pair visible in `self.valid()`. On a
/// persistent graph that is every triple whose latest event in the view is an assertion, so
/// `pg` is the current state and `pg.snapshot_at(t)` is the state as of `t`. On an event graph
/// it is every triple asserted at least once in the view (retractions are ignored).
pub trait RdfViewOps: StaticGraphViewOps + IntoDynamic {
    /// Serialises the triples visible in `self.valid()`: one triple per visible
    /// `(edge, layer)` pair, `name(src) name(layer) name(dst)`.
    ///
    /// `serializer` is an [`RdfFormat`] or an [`RdfSerializer`] configured with prefixes or a
    /// base IRI ([`serializer_with_prefixes`] also checks the prefix names). Output is buffered
    /// and flushed before returning. Order is deterministic for a given storage (by source, then
    /// destination, then layer) but not lexical. Generalized triples (literal subjects, layers
    /// whose term is not an IRI) are skipped and counted.
    ///
    /// RDF/XML also skips and counts triples it cannot write: predicates that do not split into
    /// a namespace and an XML local name (such as `http://ex/42`), predicates in the `xmlns`
    /// namespace or that are RDF/XML syntax terms (`rdf:Description`, `rdf:about`, ...), and
    /// literals with characters XML does not read back unchanged (control characters other than
    /// tab and line feed, U+FFFE, U+FFFF). An `rdf:type` triple whose object cannot name an
    /// element is written after another triple of its subject, or skipped if there is none.
    /// Blank-node labels that are not XML names get an `x` prefix (`_:1` is written `x1`, and
    /// `_:x1` is written `xx1` to stay distinct).
    fn to_rdf<W: Write>(
        &self,
        writer: W,
        serializer: impl Into<RdfSerializer>,
    ) -> Result<RdfExportStats, GraphError> {
        export::write_rdf(self, writer, serializer.into())
    }

    /// Runs a SPARQL 1.1 query on the triples visible in `self.valid()` and returns the fully
    /// collected results.
    ///
    /// The query sees one default graph, the virtual time graphs `<raphtory:asof:T>`
    /// ([`TimeGraph`]: `GRAPH <raphtory:asof:T> { .. }` matches `self.snapshot_at(T)`) and the
    /// pre-registered `raphtory:` prefix; results use the terms of [`term_of`]. Time graphs
    /// cannot be enumerated, so `GRAPH ?g` with `?g` unbound visits those the query writes as
    /// constants (unless it has `FROM NAMED`). Evaluation runs on the calling thread and holds
    /// no storage lock between results, so concurrent writes may or may not be seen. Run queries
    /// you do not control on a thread with a stack of [`sparql_stack_size`] bytes.
    ///
    /// Generalized triples (a literal subject, or a layer whose term is not an IRI) are matched
    /// by patterns, dropped by `CONSTRUCT`, and silently end a `DESCRIBE` result early; on such
    /// views use `CONSTRUCT { ?s ?p ?o } WHERE { .. }` instead.
    ///
    /// The temporal functions [`VALID_FROM`], [`VALID_TO`], [`VALID_FROM_TIME`] and
    /// [`VALID_TO_TIME`] return when a visible triple started and stopped holding (see
    /// [`with_temporal_functions`]).
    ///
    /// Errors, wrapped in [`GraphError::Rdf`]: [`RdfError::SparqlSyntax`];
    /// [`RdfError::SparqlTooDeep`] if brackets nest deeper than [`MAX_SPARQL_NESTING`];
    /// [`RdfError::SparqlEvaluation`], also for unknown functions and (wrapping
    /// [`RdfError::InvalidTimeGraph`]) time graph IRIs whose time does not parse.
    ///
    /// The query has no time limit; use [`sparql_with`](Self::sparql_with) to bound or cancel it.
    fn sparql(&self, query: &str) -> Result<SparqlResults, GraphError> {
        self.sparql_with(query, &SparqlOptions::default())
    }

    /// [`sparql`](Self::sparql) with options (see [`SparqlOptions`]): to bound the time or the
    /// number of triple patterns of the query, cancel it from another thread, or turn off the
    /// temporal functions.
    ///
    /// Besides the errors of `sparql`, a query with more triple patterns than
    /// [`SparqlOptions::max_triple_patterns`] fails with [`RdfError::TooManyPatterns`] before
    /// it is planned, and a query stopped by [`SparqlOptions::timeout`] or
    /// [`SparqlOptions::cancellation_token`] fails with [`RdfError::Timeout`] or
    /// [`RdfError::Cancelled`].
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     errors::GraphError,
    ///     prelude::*,
    ///     rdf::{RdfError, RdfViewOps, SparqlOptions},
    /// };
    /// use std::time::Duration;
    ///
    /// let g = Graph::new();
    /// for i in 0..10 {
    ///     g.add_edge(i, i.to_string(), (i + 1).to_string(), NO_PROPS, None).unwrap();
    /// }
    /// let options = SparqlOptions::default().with_timeout(Duration::from_millis(100));
    /// assert!(g.sparql_with("SELECT * { ?s ?p ?o }", &options).is_ok());
    ///
    /// // 10^8 solutions: stopped after about 100 ms
    /// let slow = "SELECT ?a { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i . ?j ?k ?l . \
    ///             ?m ?n ?o . ?p ?q ?r . ?s ?t ?u . ?v ?w ?x }";
    /// let error = g.sparql_with(slow, &options).unwrap_err();
    /// assert!(matches!(error, GraphError::Rdf(RdfError::Timeout { .. })));
    /// ```
    fn sparql_with(
        &self,
        query: &str,
        options: &SparqlOptions,
    ) -> Result<SparqlResults, GraphError> {
        query::sparql(self, query, options)
    }

    /// Runs a SPARQL 1.1 query like [`sparql`](Self::sparql) and streams its results to
    /// `writer`, serialised in `format`. Returns how many solutions or triples were written.
    ///
    /// `format` is a [`QueryResultsFormat`] for `SELECT` and `ASK` queries, an [`RdfFormat`] or
    /// [`RdfSerializer`] for `CONSTRUCT` and `DESCRIBE` queries, or a [`SparqlFormat`] with
    /// both ([`SparqlFormat::parse`] reads one from a name such as `"json"`, and
    /// [`SparqlFormat::default`] is JSON and N-Triples). A format that does not fit the form of
    /// the query gives [`RdfError::WrongResultsFormat`] before the query is evaluated, so
    /// nothing is written.
    ///
    /// The output holds RDF terms ([`term_of`]), and is byte-identical to
    /// [`SparqlResults::write`] on the results of [`sparql`](Self::sparql):
    /// - `SELECT`: one solution per result, unbound variables left out. SPARQL Results XML
    ///   fails with [`RdfError::XmlUnsafeLiteral`] on a literal with a character XML cannot hold
    ///   (as in [`to_rdf`](Self::to_rdf)) and with [`RdfError::XmlTripleTerm`] on an RDF 1.2
    ///   triple term. CSV drops term kinds, datatypes and languages, as the W3C spec says.
    /// - `ASK`: a boolean; CSV and TSV write a bare `true` or `false`.
    /// - `CONSTRUCT` and `DESCRIBE`: distinct triples in first-built order (triples written so
    ///   far are kept in memory). RDF/XML groups them by subject, so it writes only once the
    ///   query has finished, and skips the triples [`to_rdf`](Self::to_rdf) would skip, counted
    ///   in [`SparqlWriteStats::skipped`].
    ///
    /// Output is buffered and flushed before returning. If evaluation fails while results are
    /// written, the error is returned, and `writer` may hold the start of the document.
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     prelude::*,
    ///     rdf::{QueryResultsFormat, RdfFormat, RdfViewOps, SparqlFormat},
    /// };
    ///
    /// let g = Graph::new();
    /// g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
    ///
    /// let mut json = Vec::new();
    /// let query = "SELECT ?who { raphtory:Alice raphtory:knows ?who }";
    /// let stats = g.sparql_to_writer(query, &mut json, QueryResultsFormat::Json).unwrap();
    /// assert_eq!(stats.written, 1);
    /// assert_eq!(
    ///     String::from_utf8(json).unwrap(),
    ///     r#"{"head":{"vars":["who"]},"results":{"bindings":[{"who":{"type":"uri","value":"raphtory:Bob"}}]}}"#
    /// );
    ///
    /// let mut nt = Vec::new();
    /// let query = "CONSTRUCT { ?b raphtory:knownBy ?a } WHERE { ?a raphtory:knows ?b }";
    /// g.sparql_to_writer(query, &mut nt, RdfFormat::NTriples).unwrap();
    /// assert_eq!(
    ///     String::from_utf8(nt).unwrap(),
    ///     "<raphtory:Bob> <raphtory:knownBy> <raphtory:Alice> .\n"
    /// );
    ///
    /// // one name for both forms: "ttl" is only an RDF format
    /// let mut out = Vec::new();
    /// let format = SparqlFormat::parse("ttl").unwrap();
    /// assert!(g.sparql_to_writer("ASK { ?s ?p ?o }", &mut out, format).is_err());
    /// assert!(out.is_empty());
    /// ```
    fn sparql_to_writer<W: Write>(
        &self,
        query: &str,
        writer: W,
        format: impl Into<SparqlFormat>,
    ) -> Result<SparqlWriteStats, GraphError> {
        self.sparql_to_writer_with(query, writer, format, &SparqlOptions::default())
    }

    /// [`sparql_to_writer`](Self::sparql_to_writer) with options (see [`SparqlOptions`]), for
    /// example to turn off the temporal functions.
    fn sparql_to_writer_with<W: Write>(
        &self,
        query: &str,
        writer: W,
        format: impl Into<SparqlFormat>,
        options: &SparqlOptions,
    ) -> Result<SparqlWriteStats, GraphError> {
        results::sparql_to_writer(self, query, writer, format.into(), options)
    }

    /// Validates the triples visible in `self.valid()` (the triples [`to_rdf`](Self::to_rdf)
    /// writes) against a SHACL shapes graph: `shapes.validate(self)` (see
    /// [`ShaclShapes::validate`](shacl::ShaclShapes::validate) and the [`shacl`] module).
    /// Feature `shacl`.
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     prelude::*,
    ///     rdf::{shacl::ShaclShapes, RdfFormat},
    /// };
    ///
    /// let g = Graph::new();
    /// g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
    /// // every node with a `knows` edge must know at most one node
    /// let shapes = r#"
    ///     @prefix sh: <http://www.w3.org/ns/shacl#> .
    ///     <http://ex/S> sh:targetSubjectsOf raphtory:knows ;
    ///         sh:property [ sh:path raphtory:knows ; sh:maxCount 1 ] .
    /// "#;
    /// let shapes = ShaclShapes::parse(
    ///     format!("@prefix raphtory: <raphtory:> . {shapes}").as_bytes(),
    ///     RdfFormat::Turtle,
    ///     None,
    /// )
    /// .unwrap();
    /// assert!(g.validate_shacl(&shapes).unwrap().conforms);
    ///
    /// g.add_edge(2, "Alice", "Carol", NO_PROPS, Some("knows")).unwrap();
    /// let report = g.validate_shacl(&shapes).unwrap();
    /// assert!(!report.conforms);
    /// assert_eq!(report.results[0].focus_node.to_string(), "<raphtory:Alice>");
    /// ```
    #[cfg(feature = "shacl")]
    fn validate_shacl(
        &self,
        shapes: &shacl::ShaclShapes,
    ) -> Result<shacl::ShaclReport, GraphError> {
        shapes.validate(self)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> RdfViewOps for G {}
