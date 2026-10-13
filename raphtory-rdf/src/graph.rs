//! [`RdfView`] and [`RdfGraph`]: the RDF APIs over a graph viewed with persistent semantics.
use crate::{
    export, import, query, results,
    shacl::{ShaclOptions, ShaclReport, ShaclShapes},
    RdfError, RdfFormat, SparqlFormat, SparqlOptions, SparqlResults, TripleRef, WriteStats,
};
use raphtory::{
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
            cached_view::CachedView, filter::node_filtered_graph::NodeFilteredGraph,
            node_subgraph::NodeSubgraph, valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::{core::utils::time::TryIntoInputTime, GraphType};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

/// A graph viewed as RDF, with persistent semantics: the place to load, retract and query
/// triples.
///
/// It holds a [`PersistentGraph`]. Wrapping an event [`Graph`] (`RdfGraph::from(g)`) shares its
/// storage, so nothing is copied and writes through either handle are seen by both. On an event
/// Graph a retraction is a deletion event: it ends nothing, so event views still show the triple,
/// and a triple that was only retracted appears as an edge (`g.valid()` drops it).
///
/// Every Raphtory view method (`window`, `at`, `layers`, `filter`, `valid`, ...) returns an
/// [`RdfView`], which queries exactly the triples of the view: under `valid()` the triples valid
/// at the end of the view (on the whole graph, the current state), under `at(t)` the state at
/// `t`, under a window every triple that held in it, and on the whole graph every triple ever
/// asserted. A retraction is an event of its triple even if the triple did not hold before it,
/// so the whole graph, and a window holding such a retraction, also show a triple that was only
/// ever retracted or was retracted again; `valid()` and `at(t)` do not.
///
/// Every triple `s p o` is one edge `name(s) -> name(o)` in the layer of the predicate IRI `p`
/// (see the [module documentation](crate)). Writes need a graph with string node ids. A term
/// that cannot be stored (an IRI under `raphtory:` that is not the canonical encoding of a
/// name) gives [`RdfError::NonCanonicalTerm`], and a predicate whose layer name is not safe as
/// a file name gives [`RdfError::UnsafeLayerName`]. Writes go through the normal
/// `add_edge`/`delete_edge` calls, so they are safe to run while other threads query or write
/// the graph.
pub type RdfGraph = RdfView<PersistentGraph>;

/// A persistent graph view seen as RDF: export, SPARQL queries and SHACL validation of its
/// triples. [`RdfGraph`] is the whole graph, and the view methods return views of it.
///
/// All of them see the same triples: one per `(edge, layer)` pair visible in the view (see
/// [`RdfGraph`]); for the time graphs of a query, see [`TimeGraph`](crate::TimeGraph).
#[derive(Clone, Debug)]
pub struct RdfView<G = DynamicGraph>(pub(crate) G);

impl RdfGraph {
    /// A new empty graph in memory.
    pub fn new() -> Self {
        Self(PersistentGraph::new())
    }

    /// Asserts every triple of the document at `time` and returns the number of triples read.
    ///
    /// The document is read in `format`, or Turtle (which also reads N-Triples) if it is `None`.
    /// Every triple becomes one edge event at `time` (any time `add_edge` accepts), in document
    /// order, so for equal `t` the later write wins. Blank nodes are renamed to fresh random
    /// labels, so two loads never collide. Named graphs are rejected ([`RdfError::Parse`]). The
    /// load streams and is not atomic: triples before an error stay in the graph.
    pub fn load_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let time = time.try_into_input_time()?;
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        import::read_document(&self.0, time, data, format, base_iri, false)
    }

    /// Asserts every triple of the file at `path`, as [`load_rdf`](Self::load_rdf) does. The
    /// file is read in `format`, or the format its extension names
    /// ([`RdfFormat::from_path`]).
    pub fn load_rdf_path<T: TryIntoInputTime>(
        &self,
        time: T,
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let (file, format) = import::open_document(path.as_ref(), format.into())?;
        self.load_rdf(time, file, format, base_iri)
    }

    /// Retracts every triple of the document at `time` and returns the number of triples read.
    ///
    /// The document is read as in [`load_rdf`](Self::load_rdf). Blank-node labels are used as
    /// written, so a retraction document must use the stored labels (as returned by
    /// [`to_rdf`](RdfView::to_rdf)). A retraction is always recorded, even if the triple does
    /// not hold (see [`RdfGraph`] for which views then show it). The retraction streams and is
    /// not atomic: triples before an error stay retracted.
    pub fn retract_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let time = time.try_into_input_time()?;
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        import::read_document(&self.0, time, data, format, base_iri, true)
    }

    /// Retracts every triple of the file at `path`, read as in
    /// [`load_rdf_path`](Self::load_rdf_path).
    pub fn retract_rdf_path<T: TryIntoInputTime>(
        &self,
        time: T,
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let (file, format) = import::open_document(path.as_ref(), format.into())?;
        self.retract_rdf(time, file, format, base_iri)
    }

    /// Asserts one triple at `time`. Blank-node labels are used as written.
    pub fn add_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<TripleRef<'a>>,
    ) -> Result<(), RdfError> {
        let time = time.try_into_input_time()?;
        import::write_triple(&self.0, time, triple.into(), false)
    }

    /// Retracts one triple at `time`. Blank-node labels are used as written.
    pub fn retract_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<TripleRef<'a>>,
    ) -> Result<(), RdfError> {
        let time = time.try_into_input_time()?;
        import::write_triple(&self.0, time, triple.into(), true)
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
    pub fn from_view(view: G) -> Result<Self, RdfError> {
        match view.graph_type() {
            GraphType::PersistentGraph => Ok(Self(view)),
            GraphType::EventGraph => Err(RdfError::NotPersistent),
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

    /// The view with its visible nodes and edges precomputed ([`GraphViewOps::cache_view`]).
    pub fn cache_view(&self) -> RdfView<CachedView<G>> {
        RdfView(self.0.cache_view())
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
    /// Serialises the triples of the view in `format` (N-Triples if it is `None`): one triple
    /// per visible `(edge, layer)` pair, from the term of its source
    /// ([`term_of`](crate::term_of)) through the predicate IRI of its layer to the term of its
    /// destination.
    ///
    /// IRIs are written in full. Output is buffered and flushed before returning. Order is
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
    /// use raphtory_rdf::{RdfFormat, RdfGraph};
    ///
    /// let rg = RdfGraph::new();
    /// let doc = "<http://ex/a> <http://ex/p> <http://ex/b> .";
    /// rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
    /// let mut out = Vec::new();
    /// rg.to_rdf(&mut out, RdfFormat::Turtle).unwrap();
    /// assert_eq!(
    ///     String::from_utf8(out).unwrap(),
    ///     "<http://ex/a> <http://ex/p> <http://ex/b> .\n"
    /// );
    /// ```
    pub fn to_rdf<W: Write>(
        &self,
        writer: W,
        format: impl Into<Option<RdfFormat>>,
    ) -> Result<WriteStats, RdfError> {
        let format = format.into().unwrap_or(RdfFormat::NTriples);
        export::write_rdf(self.0.clone().into_dynamic(), writer, format)
    }

    /// The triples of the view as a document, as [`to_rdf`](Self::to_rdf) writes them.
    pub fn to_rdf_string(&self, format: impl Into<Option<RdfFormat>>) -> Result<String, RdfError> {
        let mut document = Vec::new();
        self.to_rdf(&mut document, format)?;
        Ok(String::from_utf8(document)?)
    }

    /// Writes the triples of the view to the file at `path`, created or truncated, as
    /// [`to_rdf`](Self::to_rdf) does, in `format`, else the format its extension names
    /// ([`RdfFormat::from_path`]), else (without an extension) N-Triples.
    pub fn to_rdf_path(
        &self,
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
    ) -> Result<WriteStats, RdfError> {
        let path = path.as_ref();
        let format = match format.into() {
            Some(format) => format,
            None if path.extension().is_none_or(|e| e.is_empty()) => RdfFormat::NTriples,
            None => RdfFormat::from_path(path)?,
        };
        let file = File::create(path).map_err(|error| {
            GraphError::IOErrorMsg(format!("cannot create '{}': {error}", path.display()))
        })?;
        self.to_rdf(file, format)
    }

    /// Runs a SPARQL 1.1 query on the triples of the view and returns the fully collected
    /// results.
    ///
    /// The query sees one default graph, the virtual time graphs `<raphtory:asof:T>` (see
    /// [`TimeGraph`](crate::TimeGraph)) and the pre-registered `raphtory:` prefix; results
    /// use the terms of [`term_of`](crate::term_of) for nodes and the predicate IRIs of the
    /// layers. Evaluation runs on its own thread, with a stack sized to the query, while the
    /// caller waits.
    ///
    /// Generalized triples (a literal subject, or a layer whose term is not an IRI) are matched
    /// by patterns, dropped by `CONSTRUCT`, and silently end a `DESCRIBE` result early; on such
    /// views use `CONSTRUCT { ?s ?p ?o } WHERE { .. }` instead.
    ///
    /// The temporal functions `raphtory:validFrom`, `raphtory:validTo`,
    /// `raphtory:validFromTime` and `raphtory:validToTime` return when a visible triple started and
    /// stopped holding (see [the rules](crate#the-rules-of-the-temporal-functions)).
    ///
    /// `options` can bound the length and the number of triple patterns of the query (see
    /// [`SparqlOptions`]); `&SparqlOptions::default()` does not.
    ///
    /// Errors: [`RdfError::QueryTooLong`], before parsing, for a query longer than
    /// [`SparqlOptions::with_max_query_length`] allows; [`RdfError::SparqlSyntax`];
    /// [`RdfError::SparqlTooDeep`] if brackets nest deeper than
    /// [`MAX_SPARQL_NESTING`](crate::MAX_SPARQL_NESTING); [`RdfError::TooManyPatterns`],
    /// before planning, for a query with more triple patterns than
    /// [`SparqlOptions::with_max_triple_patterns`] allows; [`RdfError::SparqlEvaluation`], also
    /// for unknown functions and (wrapping [`RdfError::InvalidTimeGraph`]) time graph IRIs whose
    /// time does not parse.
    pub fn sparql(&self, query: &str, options: &SparqlOptions) -> Result<SparqlResults, RdfError> {
        query::sparql(&self.0.clone().into_dynamic(), query, options)
    }

    /// Runs a SPARQL 1.1 query like [`sparql`](Self::sparql) and streams its results to
    /// `writer`, serialised in `format`, with the `options` of `sparql`. Returns how many
    /// solutions or triples were written. Results larger than
    /// [`SparqlOptions::with_max_result_bytes`] allows fail with [`RdfError::ResultsTooLarge`].
    ///
    /// `format` is a [`QueryResultsFormat`](crate::QueryResultsFormat) for `SELECT` and
    /// `ASK` queries, an [`RdfFormat`] for `CONSTRUCT` and `DESCRIBE` queries, or a
    /// [`SparqlFormat`] with both ([`SparqlFormat::parse`] reads one from a name such as
    /// `"json"`, and [`SparqlFormat::default`] is JSON and N-Triples). A format that does not
    /// fit the form of the query gives [`RdfError::WrongResultsFormat`] before the query is
    /// evaluated, so nothing is written.
    ///
    /// The output holds RDF terms ([`term_of`](crate::term_of)):
    /// - `SELECT`: one solution per result, unbound variables left out. SPARQL Results XML
    ///   fails with [`RdfError::XmlUnsafeLiteral`] on a literal with a character XML cannot hold
    ///   (as in [`to_rdf`](Self::to_rdf)) and with [`RdfError::XmlTripleTerm`] on an RDF 1.2
    ///   triple term. CSV drops term kinds, datatypes and languages, as the W3C spec says.
    /// - `ASK`: a boolean; CSV and TSV write a bare `true` or `false`.
    /// - `CONSTRUCT` and `DESCRIBE`: distinct triples in first-built order (triples written so
    ///   far are kept in memory). RDF/XML groups them by subject, so it writes only once the
    ///   query has finished, and fails with [`RdfError::RdfXmlSkipped`], writing nothing, if it
    ///   cannot hold a triple (one [`to_rdf`](Self::to_rdf) would skip).
    ///
    /// Output is buffered and flushed before returning. If evaluation fails while results are
    /// written, the error is returned, and `writer` may hold the start of the document.
    ///
    /// # Example
    /// ```
    /// use raphtory_rdf::{QueryResultsFormat, RdfFormat, RdfGraph, SparqlOptions};
    ///
    /// let rg = RdfGraph::new();
    /// let doc = "<http://ex/alice> <http://ex/knows> <http://ex/bob> .";
    /// rg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
    ///
    /// let mut json = Vec::new();
    /// let query = "SELECT ?who { <http://ex/alice> <http://ex/knows> ?who }";
    /// let stats = rg
    ///     .sparql_to_writer(query, &mut json, QueryResultsFormat::Json, &SparqlOptions::default())
    ///     .unwrap();
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
        options: &SparqlOptions,
    ) -> Result<WriteStats, RdfError> {
        results::sparql_to_writer(
            &self.0.clone().into_dynamic(),
            query,
            writer,
            format.into(),
            options,
        )
    }

    /// The results of a SPARQL query as a document, as
    /// [`sparql_to_writer`](Self::sparql_to_writer) writes them.
    pub fn sparql_to_string(
        &self,
        query: &str,
        format: impl Into<SparqlFormat>,
        options: &SparqlOptions,
    ) -> Result<String, RdfError> {
        let mut document = Vec::new();
        self.sparql_to_writer(query, &mut document, format, options)?;
        Ok(String::from_utf8(document)?)
    }

    /// Validates the triples of the view (the triples [`to_rdf`](Self::to_rdf) writes) against
    /// a SHACL shapes graph (see the [`shacl`](crate::shacl) module), with the `options` of
    /// [`ShaclOptions`].
    ///
    /// Runs on a shared pool of large-stack threads while the caller waits; a validator panic
    /// becomes [`RdfError::Panicked`].
    pub fn validate_shacl(
        &self,
        shapes: &ShaclShapes,
        options: &ShaclOptions,
    ) -> Result<ShaclReport, RdfError> {
        shapes.validate(self.0.clone().into_dynamic(), options)
    }
}
