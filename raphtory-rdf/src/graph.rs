//! [`RdfView`] and [`RdfGraph`]: the RDF APIs over a persistent graph view.
//! Writing a triple is one `add_edge` or `delete_edge` event.
use crate::{
    export::write_rdf,
    mapping::{is_rdf11_term, layer_name_of, name_of},
    query, results,
    shacl::{ShaclOptions, ShaclReport, ShaclShapes},
    RdfError, RdfFormat, SparqlFormat, SparqlOptions, SparqlResults, TermRef, TripleRef,
    WriteStats,
};
use oxigraph::io::RdfParser;
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
use raphtory_api::{
    core::utils::time::{InputTime, TryIntoInputTime},
    GraphType,
};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

/// A whole graph viewed as RDF with persistent semantics: the place to load and retract triples.
/// `RdfGraph::from(graph)` shares an event graph's storage; its event views still show retractions.
pub type RdfGraph = RdfView<PersistentGraph>;

/// A persistent graph view seen as RDF: one triple per `(edge, layer)` pair visible in it.
#[derive(Clone, Debug)]
pub struct RdfView<G = DynamicGraph>(pub(crate) G);

impl RdfGraph {
    /// A new empty graph in memory.
    pub fn new() -> Self {
        Self(PersistentGraph::new())
    }

    /// Asserts every triple of the document at `time` (Turtle if `format` is `None`).
    /// Blank nodes get fresh labels and named graphs are rejected; not atomic.
    pub fn load_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let time = time.try_into_input_time()?;
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        read_document(&self.0, time, data, format, base_iri, false)
    }

    /// [`load_rdf`](Self::load_rdf) of a file, in `format` or the one its extension names.
    pub fn load_rdf_path<T: TryIntoInputTime>(
        &self,
        time: T,
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let (file, format) = open_document(path.as_ref(), format.into())?;
        self.load_rdf(time, file, format, base_iri)
    }

    /// Retracts every triple of the document at `time`.
    /// Blank-node labels are used as written, so they must be the stored ones; not atomic.
    pub fn retract_rdf<T: TryIntoInputTime>(
        &self,
        time: T,
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let time = time.try_into_input_time()?;
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        read_document(&self.0, time, data, format, base_iri, true)
    }

    /// [`retract_rdf`](Self::retract_rdf) of a file, as [`load_rdf_path`](Self::load_rdf_path).
    pub fn retract_rdf_path<T: TryIntoInputTime>(
        &self,
        time: T,
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<usize, RdfError> {
        let (file, format) = open_document(path.as_ref(), format.into())?;
        self.retract_rdf(time, file, format, base_iri)
    }

    /// Asserts one triple at `time`. Blank-node labels are used as written.
    pub fn add_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<TripleRef<'a>>,
    ) -> Result<(), RdfError> {
        let time = time.try_into_input_time()?;
        write_triple(&self.0, time, triple.into(), false)
    }

    /// Retracts one triple at `time`. Blank-node labels are used as written.
    pub fn retract_triple<'a, T: TryIntoInputTime>(
        &self,
        time: T,
        triple: impl Into<TripleRef<'a>>,
    ) -> Result<(), RdfError> {
        let time = time.try_into_input_time()?;
        write_triple(&self.0, time, triple.into(), true)
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
    /// The RDF view of a persistent graph view; fails on an event graph view.
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
    /// Writes the triples of the view in `format` (N-Triples if `None`).
    /// Triples with a literal subject are skipped and counted.
    pub fn to_rdf<W: Write>(
        &self,
        writer: W,
        format: impl Into<Option<RdfFormat>>,
    ) -> Result<WriteStats, RdfError> {
        let format = format.into().unwrap_or(RdfFormat::NTriples);
        write_rdf(self.0.clone().into_dynamic(), writer, format)
    }

    /// [`to_rdf`](Self::to_rdf) into a string.
    pub fn to_rdf_string(&self, format: impl Into<Option<RdfFormat>>) -> Result<String, RdfError> {
        let mut document = Vec::new();
        self.to_rdf(&mut document, format)?;
        Ok(String::from_utf8(document)?)
    }

    /// [`to_rdf`](Self::to_rdf) into a file, in `format`, else its extension's, else N-Triples.
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

    /// Runs a SPARQL 1.1 query on the triples of the view and collects the results.
    /// `DESCRIBE` can stop early at a triple with a literal subject; `CONSTRUCT` drops those.
    pub fn sparql(&self, query: &str, options: &SparqlOptions) -> Result<SparqlResults, RdfError> {
        query::sparql(&self.0.clone().into_dynamic(), query, options)
    }

    /// Runs a SPARQL 1.1 query and streams its results to `writer` in `format`.
    /// On an evaluation error `writer` may hold the start of the document.
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

    /// [`sparql_to_writer`](Self::sparql_to_writer) into a string.
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

    /// Validates the triples [`to_rdf`](Self::to_rdf) writes against SHACL shapes.
    pub fn validate_shacl(
        &self,
        shapes: &ShaclShapes,
        options: &ShaclOptions,
    ) -> Result<ShaclReport, RdfError> {
        shapes.validate(self.0.clone().into_dynamic(), options)
    }
}

fn unstorable(t: TermRef<'_>) -> RdfError {
    if is_rdf11_term(t) {
        RdfError::NonCanonicalTerm(t.to_string())
    } else {
        RdfError::Rdf12Term(t.to_string())
    }
}

fn name(t: TermRef<'_>) -> Result<String, RdfError> {
    name_of(t).ok_or_else(|| unstorable(t))
}

fn layer(t: TermRef<'_>) -> Result<String, RdfError> {
    layer_name_of(t).ok_or_else(|| unstorable(t))
}

fn write_triple<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'_>,
    retract: bool,
) -> Result<(), RdfError> {
    let s = name(triple.subject.into())?;
    let p = layer(triple.predicate.into())?;
    let o = name(triple.object)?;
    if retract {
        g.delete_edge(time, &s, &o, Some(&p))?;
    } else {
        g.add_edge(time, &s, &o, NO_PROPS, Some(&p))?;
    }
    Ok(())
}

fn read_document<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    data: impl Read,
    format: RdfFormat,
    base_iri: Option<&str>,
    retract: bool,
) -> Result<usize, RdfError> {
    let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
    if !retract {
        parser = parser.rename_blank_nodes();
    }
    if let Some(base_iri) = base_iri {
        parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
    }
    let mut count = 0;
    // the parsers buffer their input themselves
    for quad in parser.for_reader(data) {
        let quad = quad.map_err(RdfError::from)?;
        let triple = quad.as_ref().into();
        write_triple(g, time, triple, retract)?;
        count += 1;
    }
    Ok(count)
}

pub(crate) fn open_document(
    path: &Path,
    format: Option<RdfFormat>,
) -> Result<(File, RdfFormat), RdfError> {
    let format = format.map_or_else(|| RdfFormat::from_path(path), Ok)?;
    // `File::open` opens a directory on Unix, and reading it fails later with a vaguer error
    if path.is_dir() {
        return Err(GraphError::IOErrorMsg(format!(
            "cannot open '{}': it is a directory",
            path.display()
        ))
        .into());
    }
    let file = File::open(path).map_err(|error| {
        GraphError::IOErrorMsg(format!("cannot open '{}': {error}", path.display()))
    })?;
    Ok((file, format))
}
