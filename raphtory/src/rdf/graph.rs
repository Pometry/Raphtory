//! [`RdfView`] and [`RdfGraph`]: the RDF APIs over a graph viewed with persistent semantics.
#[cfg(feature = "shacl")]
use crate::rdf::shacl::{ShaclReport, ShaclShapes};
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::{
            state::ops::filter::NodeTypeFilterOp,
            view::{
                internal::{GraphView, InternalFilter, MaterializedGraph},
                DynamicGraph, IntoDynamic, StaticGraphViewOps,
            },
        },
        graph::views::{
            filter::node_filtered_graph::NodeFilteredGraph, node_subgraph::NodeSubgraph,
            valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
    prelude::*,
    rdf::{
        export, import, model, query, results, CancellationToken, RdfError, RdfFormat,
        SparqlFormat, SparqlOptions, SparqlResults, WriteStats,
    },
};
#[cfg(feature = "io")]
use raphtory_api::core::storage::graph_folder::GraphPaths;
#[cfg(feature = "shacl")]
use raphtory_api::core::{storage::timeindex::EventTime, utils::time::TryIntoTime};
use raphtory_api::{core::utils::time::TryIntoInputTime, GraphType};
use std::{
    io::{Read, Write},
    time::Duration,
};

/// A graph viewed as RDF, with persistent semantics: the place to load, retract and query
/// triples.
///
/// It holds a [`PersistentGraph`]. Wrapping an event [`Graph`] (`RdfGraph::from(g)`) shares its
/// storage, so nothing is copied and writes through either handle are seen by both; a
/// retraction is a deletion, which the persistent view sees and event views of the same graph
/// ignore.
///
/// Every Raphtory view method (`window`, `at`, `layers`, `filter`, `valid`, ...) returns an
/// [`RdfView`], which queries exactly the triples of the view: under `valid()` the current
/// state, under `at(t)` the state at `t`, under a window every triple that held in it, and on
/// the whole graph every triple ever asserted. A retraction is an event of its triple even if
/// the triple did not hold before it, so the whole graph, and a window holding such a
/// retraction, also show a triple that was only ever retracted or was retracted again; `valid()`
/// and `at(t)` do not.
///
/// Every triple `s p o` is one edge `name(s) -> name(o)` in the layer `layer_name_of(p)`
/// (see the [module documentation](crate::rdf)). Writes need a graph with string node ids (or
/// an empty graph); a graph with u64 ids gives [`RdfError::NonStringIds`] before anything is
/// written. A term that cannot be stored (an IRI under `raphtory:` that is not the canonical
/// encoding of a name) gives [`RdfError::NonCanonicalTerm`], and a predicate under `raphtory:`
/// whose layer name would be blank or contain a control character or any of
/// `/ \ : * ? " < > |` gives [`RdfError::UnsafeLayerName`]. Writes go through the normal
/// `add_edge`/`delete_edge` calls, so they are safe to run while other threads query or write
/// the graph.
pub type RdfGraph = RdfView<PersistentGraph>;

/// A persistent graph view seen as RDF: export, SPARQL queries and SHACL validation of its
/// triples. [`RdfGraph`] is the whole graph, and the view methods return views of it.
///
/// All of them see the same triples: one per `(edge, layer)` pair visible in the view. On the
/// whole graph that is every triple with an event, retractions included (see [`RdfGraph`]);
/// `valid()` keeps the triples valid at the end of the view (on the whole graph, the current
/// state) and `at(t)` is the state at `t`.
///
/// The time graphs `<raphtory:asof:T>` of a query are `snapshot_at(T)` of the view, so every
/// filter of the view applies to them too, `valid()` included: on `rg.valid()` a time graph
/// holds only the triples that held at `T` and still hold now. To compare times in one query,
/// run it on a view without `valid()`.
#[derive(Clone, Debug)]
pub struct RdfView<G = DynamicGraph>(pub(crate) G);

impl RdfGraph {
    /// A new empty graph in memory.
    pub fn new() -> Self {
        Self(PersistentGraph::new())
    }

    /// A new empty graph stored at `path`.
    #[cfg(feature = "io")]
    pub fn new_at_path(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        Ok(Self(PersistentGraph::new_at_path(path)?))
    }

    /// Opens the graph stored at `path`, event or persistent, whether saved to disk or encoded.
    /// An event graph is wrapped as with `RdfGraph::from(g)`.
    #[cfg(feature = "io")]
    pub fn load(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        let graph = if path.read_metadata()?.is_diskgraph {
            MaterializedGraph::load(path)?
        } else {
            MaterializedGraph::decode(path)?
        };
        Ok(graph.into())
    }

    /// Asserts every triple of the document at `time` and returns the number of triples read.
    ///
    /// Every triple becomes one edge event at `time` (any time `add_edge` accepts), in document
    /// order, so for equal `t` the later write wins. Blank nodes are renamed to fresh random
    /// labels, so two loads never collide. Named graphs are rejected ([`RdfError::Parse`]). The
    /// load streams and is not atomic: triples before an error stay in the graph.
    pub fn load_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<usize, GraphError> {
        let time = time.try_into_input_time()?;
        import::read_document(&self.0, time, data, format, base_iri, false)
    }

    /// Retracts every triple of the document at `time` and returns the number of triples read.
    ///
    /// Blank-node labels are used as written, so a retraction document must use the stored
    /// labels (as returned by [`to_rdf`](RdfView::to_rdf)). A retraction is always recorded,
    /// even if the triple does not hold, and views that show every triple with an event in them
    /// then show it (see [`RdfGraph`]).
    pub fn retract_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<usize, GraphError> {
        let time = time.try_into_input_time()?;
        import::read_document(&self.0, time, data, format, base_iri, true)
    }

    /// Asserts one triple at `time`. Blank-node labels are used as written.
    pub fn add_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<model::TripleRef<'a>>,
    ) -> Result<(), GraphError> {
        let time = time.try_into_input_time()?;
        import::write_single(&self.0, time, triple.into(), false)
    }

    /// Retracts one triple at `time`. Blank-node labels are used as written.
    pub fn retract_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<model::TripleRef<'a>>,
    ) -> Result<(), GraphError> {
        let time = time.try_into_input_time()?;
        import::write_single(&self.0, time, triple.into(), true)
    }
}

impl Default for RdfGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Graph> for RdfGraph {
    fn from(graph: Graph) -> Self {
        Self(graph.persistent_graph())
    }
}

impl From<PersistentGraph> for RdfGraph {
    fn from(graph: PersistentGraph) -> Self {
        Self(graph)
    }
}

impl From<MaterializedGraph> for RdfGraph {
    fn from(graph: MaterializedGraph) -> Self {
        match graph {
            MaterializedGraph::EventGraph(graph) => graph.into(),
            MaterializedGraph::PersistentGraph(graph) => graph.into(),
        }
    }
}

impl<G: GraphView> RdfView<G> {
    /// The RDF view of a persistent graph view, such as a view of `graph.persistent_graph()`.
    /// Fails with [`RdfError::NotPersistent`] on a view of an event graph.
    pub fn from_view(view: G) -> Result<Self, GraphError> {
        match view.graph_type() {
            GraphType::PersistentGraph => Ok(Self(view)),
            GraphType::EventGraph => Err(RdfError::NotPersistent.into()),
        }
    }

    /// The view as a Raphtory graph view.
    pub fn graph(&self) -> &G {
        &self.0
    }
}

impl<G: StaticGraphViewOps> RdfView<G> {
    /// The triples valid at the end of this view ([`GraphViewOps::valid`]).
    pub fn valid(&self) -> RdfView<ValidGraph<G>> {
        RdfView(self.0.valid())
    }

    /// The view restricted to `nodes` ([`GraphViewOps::subgraph`]).
    pub fn subgraph<I: IntoIterator<Item = V>, V: AsNodeRef>(
        &self,
        nodes: I,
    ) -> RdfView<NodeSubgraph<G>> {
        RdfView(self.0.subgraph(nodes))
    }

    /// The view restricted to nodes of `node_types` ([`GraphViewOps::subgraph_node_types`]).
    pub fn subgraph_node_types<I: IntoIterator<Item = V>, V: AsRef<str>>(
        &self,
        node_types: I,
    ) -> RdfView<NodeFilteredGraph<G, NodeTypeFilterOp>> {
        RdfView(self.0.subgraph_node_types(node_types))
    }

    /// The view without `nodes` ([`GraphViewOps::exclude_nodes`]).
    pub fn exclude_nodes<I: IntoIterator<Item = V>, V: AsNodeRef>(
        &self,
        nodes: I,
    ) -> RdfView<NodeSubgraph<G>> {
        RdfView(self.0.exclude_nodes(nodes))
    }
}

impl<'graph, G: GraphView + 'graph> InternalFilter<'graph> for RdfView<G> {
    type Graph = G;
    type Filtered<FilteredGraph: GraphView + 'graph> = RdfView<FilteredGraph>;

    fn base_graph(&self) -> &Self::Graph {
        &self.0
    }

    fn apply_filter<FilteredGraph: GraphView + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph> {
        RdfView(filtered_graph)
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> RdfView<G> {
    /// The view with its graph type erased ([`IntoDynamic::into_dynamic`]).
    pub fn into_dynamic(self) -> RdfView {
        RdfView(self.0.into_dynamic())
    }

    /// Serialises the triples of the view in `format`: one triple per visible `(edge, layer)`
    /// pair, `term_of(src) layer_term(layer) term_of(dst)`.
    ///
    /// `prefixes` are the prefix names and IRIs that formats which support them (Turtle, TriG,
    /// RDF/XML) abbreviate IRIs with; a name the format cannot write gives
    /// [`RdfError::InvalidPrefix`]. Output is buffered and flushed before returning. Order is
    /// deterministic for a given storage (by source, then destination, then layer) but not
    /// lexical. Generalized triples (literal subjects, layers whose term is not an IRI) are
    /// skipped and counted.
    ///
    /// RDF/XML also skips and counts triples it cannot write: predicates that do not split into
    /// a namespace and an XML local name (such as `http://ex/42`), predicates in the `xmlns`
    /// namespace or that are RDF/XML syntax terms (`rdf:Description`, `rdf:about`, ...), and
    /// literals with characters XML does not read back unchanged (control characters other than
    /// tab and line feed, U+FFFE, U+FFFF). An `rdf:type` triple whose object cannot name an
    /// element is written after another triple of its subject, or skipped if there is none.
    /// Blank-node labels that are not XML names get an `x` prefix (`_:1` is written `x1`, and
    /// `_:x1` is written `xx1` to stay distinct).
    ///
    /// # Example
    /// ```
    /// use raphtory::{prelude::*, rdf::RdfFormat};
    ///
    /// let rg = RdfGraph::new();
    /// let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .";
    /// rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
    /// let mut out = Vec::new();
    /// rg.to_rdf(&mut out, RdfFormat::Turtle, &[("ex", "http://ex/")]).unwrap();
    /// assert_eq!(
    ///     String::from_utf8(out).unwrap(),
    ///     "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\n"
    /// );
    /// ```
    pub fn to_rdf<W: Write>(
        &self,
        writer: W,
        format: RdfFormat,
        prefixes: &[(&str, &str)],
    ) -> Result<WriteStats, GraphError> {
        export::write_rdf(&self.0, writer, format, prefixes, None, None)
    }

    /// [`to_rdf`](Self::to_rdf), stopped with [`RdfError::ExportTimeout`] once it has run for
    /// `timeout`, or with [`RdfError::ExportCancelled`] once `cancellation_token` is cancelled
    /// (the export never cancels it). `writer` may then hold the start of the document.
    pub fn to_rdf_with<W: Write>(
        &self,
        writer: W,
        format: RdfFormat,
        prefixes: &[(&str, &str)],
        timeout: Option<Duration>,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<WriteStats, GraphError> {
        export::write_rdf(
            &self.0,
            writer,
            format,
            prefixes,
            timeout,
            cancellation_token,
        )
    }

    /// Runs a SPARQL 1.1 query on the triples of the view and returns the fully collected
    /// results.
    ///
    /// The query sees one default graph, the virtual time graphs `<raphtory:asof:T>`
    /// ([`TimeGraph`](crate::rdf::TimeGraph): `GRAPH <raphtory:asof:T> { .. }` matches
    /// `self.snapshot_at(T)`) and the pre-registered `raphtory:` prefix; results use the terms
    /// of [`term_of`](crate::rdf::term_of) and [`layer_term`](crate::rdf::layer_term). Time
    /// graphs cannot be enumerated, so `GRAPH ?g` with `?g` unbound visits those the query
    /// writes as constants (unless it has `FROM NAMED`). Evaluation runs on its own thread, with
    /// a stack sized to the query, while the caller waits; it holds no storage lock between
    /// results, so concurrent writes may or may not be seen.
    ///
    /// Generalized triples (a literal subject, or a layer whose term is not an IRI) are matched
    /// by patterns, dropped by `CONSTRUCT`, and silently end a `DESCRIBE` result early; on such
    /// views use `CONSTRUCT { ?s ?p ?o } WHERE { .. }` instead.
    ///
    /// The temporal functions [`VALID_FROM`](crate::rdf::VALID_FROM),
    /// [`VALID_TO`](crate::rdf::VALID_TO), [`VALID_FROM_TIME`](crate::rdf::VALID_FROM_TIME) and
    /// [`VALID_TO_TIME`](crate::rdf::VALID_TO_TIME) return when a visible triple started and
    /// stopped holding (see [the rules](crate::rdf#the-rules-of-the-temporal-functions)).
    ///
    /// Errors, wrapped in [`GraphError::Rdf`]: [`RdfError::SparqlSyntax`];
    /// [`RdfError::SparqlTooDeep`] if brackets nest deeper than
    /// [`MAX_SPARQL_NESTING`](crate::rdf::MAX_SPARQL_NESTING); [`RdfError::SparqlEvaluation`],
    /// also for unknown functions and (wrapping [`RdfError::InvalidTimeGraph`]) time graph IRIs
    /// whose time does not parse.
    ///
    /// The query has no time limit; use [`sparql_with`](Self::sparql_with) to bound or cancel it.
    pub fn sparql(&self, query: &str) -> Result<SparqlResults, GraphError> {
        self.sparql_with(query, &SparqlOptions::default())
    }

    /// [`sparql`](Self::sparql) with options (see [`SparqlOptions`]): to bound the time or the
    /// number of triple patterns of the query, cancel it from another thread, or turn off the
    /// temporal functions.
    ///
    /// Besides the errors of `sparql`, a query with more triple patterns than
    /// [`SparqlOptions::max_triple_patterns`] fails with [`RdfError::TooManyPatterns`] before
    /// it is planned, and a query stopped by [`SparqlOptions::timeout`] or by its cancellation
    /// token ([`SparqlOptions::with_cancellation_token`]) fails with [`RdfError::Timeout`] or
    /// [`RdfError::Cancelled`].
    pub fn sparql_with(
        &self,
        query: &str,
        options: &SparqlOptions,
    ) -> Result<SparqlResults, GraphError> {
        query::sparql(&self.0, query, options)
    }

    /// Runs a SPARQL 1.1 query like [`sparql`](Self::sparql) and streams its results to
    /// `writer`, serialised in `format`. Returns how many solutions or triples were written.
    ///
    /// `format` is a [`QueryResultsFormat`](crate::rdf::QueryResultsFormat) for `SELECT` and
    /// `ASK` queries, an [`RdfFormat`] for `CONSTRUCT` and `DESCRIBE` queries, or a
    /// [`SparqlFormat`] with both ([`SparqlFormat::parse`] reads one from a name such as
    /// `"json"`, and [`SparqlFormat::default`] is JSON and N-Triples). A format that does not
    /// fit the form of the query gives [`RdfError::WrongResultsFormat`] before the query is
    /// evaluated, so nothing is written.
    ///
    /// The output holds RDF terms ([`term_of`](crate::rdf::term_of)), and is byte-identical to
    /// [`SparqlResults::write`] on the results of [`sparql`](Self::sparql):
    /// - `SELECT`: one solution per result, unbound variables left out. SPARQL Results XML
    ///   fails with [`RdfError::XmlUnsafeLiteral`] on a literal with a character XML cannot hold
    ///   (as in [`to_rdf`](Self::to_rdf)) and with [`RdfError::XmlTripleTerm`] on an RDF 1.2
    ///   triple term. CSV drops term kinds, datatypes and languages, as the W3C spec says.
    /// - `ASK`: a boolean; CSV and TSV write a bare `true` or `false`.
    /// - `CONSTRUCT` and `DESCRIBE`: distinct triples in first-built order (triples written so
    ///   far are kept in memory). RDF/XML groups them by subject, so it writes only once the
    ///   query has finished, and skips the triples [`to_rdf`](Self::to_rdf) would skip, counted
    ///   in [`WriteStats::skipped`].
    ///
    /// Output is buffered and flushed before returning. If evaluation fails while results are
    /// written, the error is returned, and `writer` may hold the start of the document.
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     prelude::*,
    ///     rdf::{QueryResultsFormat, RdfFormat},
    /// };
    ///
    /// let rg = RdfGraph::new();
    /// let doc = "<http://ex/alice> <http://ex/knows> <http://ex/bob> .";
    /// rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
    ///
    /// let mut json = Vec::new();
    /// let query = "SELECT ?who { <http://ex/alice> <http://ex/knows> ?who }";
    /// let stats = rg.sparql_to_writer(query, &mut json, QueryResultsFormat::Json).unwrap();
    /// assert_eq!(stats.written, 1);
    /// assert_eq!(
    ///     String::from_utf8(json).unwrap(),
    ///     r#"{"head":{"vars":["who"]},"results":{"bindings":[{"who":{"type":"uri","value":"http://ex/bob"}}]}}"#
    /// );
    /// ```
    pub fn sparql_to_writer<W: Write + Send>(
        &self,
        query: &str,
        writer: W,
        format: impl Into<SparqlFormat>,
    ) -> Result<WriteStats, GraphError> {
        self.sparql_to_writer_with(query, writer, format, &SparqlOptions::default())
    }

    /// [`sparql_to_writer`](Self::sparql_to_writer) with options (see [`SparqlOptions`]), for
    /// example to turn off the temporal functions.
    pub fn sparql_to_writer_with<W: Write + Send>(
        &self,
        query: &str,
        writer: W,
        format: impl Into<SparqlFormat>,
        options: &SparqlOptions,
    ) -> Result<WriteStats, GraphError> {
        results::sparql_to_writer(&self.0, query, writer, format.into(), options)
    }

    /// Validates the triples of the view (the triples [`to_rdf`](Self::to_rdf) writes) against
    /// a SHACL shapes graph (see the [`shacl`](crate::rdf::shacl) module). Feature `shacl`.
    ///
    /// Runs on a shared pool of large-stack threads while the caller waits; a validator panic
    /// becomes [`RdfError::Shacl`]. It has no time limit; use
    /// [`validate_shacl_with`](Self::validate_shacl_with) to bound or cancel it.
    #[cfg(feature = "shacl")]
    pub fn validate_shacl(&self, shapes: &ShaclShapes) -> Result<ShaclReport, GraphError> {
        shapes.validate(&self.0, None, None)
    }

    /// [`validate_shacl`](Self::validate_shacl), stopped with [`RdfError::ShaclTimeout`] once
    /// it has run for `timeout`, or with [`RdfError::ShaclCancelled`] once `cancellation_token`
    /// is cancelled (the validation never cancels it). The validation checks whether to stop
    /// between the triples it reads. Feature `shacl`.
    #[cfg(feature = "shacl")]
    pub fn validate_shacl_with(
        &self,
        shapes: &ShaclShapes,
        timeout: Option<Duration>,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<ShaclReport, GraphError> {
        shapes.validate(&self.0, timeout, cancellation_token)
    }

    /// Validates `self.snapshot_at(t)` for every time `t` of `times`, in order, and returns each
    /// time with its report. Feature `shacl`.
    ///
    /// On a view without `valid()` that is the state as of each time, so this tells when the
    /// data started or stopped conforming. On a view with `valid()` each snapshot holds only
    /// the triples that still hold at the end of the view (see
    /// [`TimeGraph`](crate::rdf::TimeGraph)).
    #[cfg(feature = "shacl")]
    pub fn validate_shacl_at<T: TryIntoTime>(
        &self,
        shapes: &ShaclShapes,
        times: impl IntoIterator<Item = T>,
    ) -> Result<Vec<(EventTime, ShaclReport)>, GraphError> {
        times
            .into_iter()
            .map(|time| {
                let t = time.try_into_time()?;
                Ok((t, shapes.validate(&self.0.snapshot_at(t), None, None)?))
            })
            .collect()
    }
}
