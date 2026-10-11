//! The `rdfGraph` field of the query root and the `RdfGraph` object (feature `rdf`): a stored
//! graph viewed as RDF, with persistent semantics.
use crate::{
    config::concurrency_config::ConcurrencyConfig,
    data::{Data, GqlGraphType},
    model::{
        graph::{
            filtering::{GqlFilter, GraphViewCollection},
            graph::GqlGraph,
            node_id::GqlNodeId,
            timeindex::{GqlEventTime, GqlTimeInput},
            windowset::GqlGraphWindowSet,
            GqlAlignmentUnit, WindowDuration,
        },
        QueryRoot,
    },
    paths::PathValidationError,
    sparql::{run_limited, run_sparql, write_capped, SparqlError},
};
use async_graphql::Context;
use dynamic_graphql::{
    Enum, ExpandObject, ExpandObjectFields, ResolvedObject, ResolvedObjectFields, Result,
};
use raphtory::errors::GraphError;
#[cfg(feature = "shacl")]
use raphtory_rdf::shacl::ShaclShapes;
use raphtory_rdf::{QueryResultsFormat, RdfError, RdfFormat, RdfView, SparqlFormat};
use std::sync::{Arc, OnceLock};

/// Adds the `rdfGraph` field to the query root.
#[derive(ExpandObject)]
pub struct RdfGraphQuery<'a>(&'a QueryRoot);

#[ExpandObjectFields]
impl<'a> RdfGraphQuery<'a> {
    /// Returns a stored graph, event or persistent, viewed as RDF with persistent semantics:
    /// one triple per edge and layer (source, layer, destination), where a retraction is a
    /// deletion. Null if the caller cannot read the graph.
    async fn rdf_graph(
        ctx: &Context<'_>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
    ) -> Result<Option<GqlRdfGraph>> {
        let data = ctx.data_unchecked::<Data>();
        let Some((folder, graph, permission)) = data
            .get_graph_and_read_permission(ctx, path, Some(GqlGraphType::Persistent))
            .await?
        else {
            return Ok(None);
        };
        let rdf_graph = GqlRdfGraph {
            graph: GqlGraph::new(folder, graph),
            // The temporal functions read unfiltered history, which a row filter must hide.
            temporal_functions: !permission.has_row_filter(),
        };
        if let Some(lookup) = ctx.data_opt::<Arc<RdfGraphLookup>>() {
            lookup.set(&rdf_graph);
        }
        Ok(Some(rdf_graph))
    }
}

/// Where `rdfGraph` puts the graph it reads, when the request carries one: the SPARQL endpoint
/// reads the graph through a GraphQL request, because permissions are resolved from the GraphQL
/// context (`AuthorizationPolicy` takes a `Context`), then runs the query itself.
#[derive(Default)]
pub(crate) struct RdfGraphLookup(OnceLock<GqlRdfGraph>);

impl RdfGraphLookup {
    fn set(&self, graph: &GqlRdfGraph) {
        self.0.get_or_init(|| graph.clone());
    }

    pub(crate) fn get(&self) -> Option<GqlRdfGraph> {
        self.0.get().cloned()
    }
}

/// A graph viewed as RDF, with persistent semantics: one triple per visible edge and layer
/// (source, layer, destination). A node name that is not an IRI, a blank node or a literal is an
/// IRI under `raphtory:`, a prefix every query can use, and the layer of a predicate IRI is the
/// IRI with `%`, `/`, `:`, `?` and `*` written `%25`, `%2F`, `%3A`, `%3F` and `%2A` (the
/// predicate `<http://ex/knows>` is the layer `http%3A%2F%2Fex%2Fknows`). Every view field
/// returns an RDF view, and the RDF fields see exactly the triples of the view: under `valid`
/// the current state, under `at(time: T)` the state as of T, and on the whole graph every triple
/// ever asserted or retracted.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "RdfGraph")]
pub struct GqlRdfGraph {
    graph: GqlGraph,
    temporal_functions: bool,
}

impl GqlRdfGraph {
    fn with(&self, graph: GqlGraph) -> Self {
        Self {
            graph,
            temporal_functions: self.temporal_functions,
        }
    }

    fn windows(&self, windows: GqlGraphWindowSet) -> GqlRdfGraphWindowSet {
        GqlRdfGraphWindowSet {
            windows,
            rdf_graph: self.clone(),
        }
    }

    pub(crate) fn view(&self) -> Result<RdfView, RdfError> {
        RdfView::from_view(self.graph.graph().clone())
    }

    /// Whether queries may call the temporal functions: not when the caller's read is
    /// row-filtered.
    pub(crate) fn temporal_functions(&self) -> bool {
        self.temporal_functions
    }
}

#[ResolvedObjectFields]
impl GqlRdfGraph {
    /// View restricted to the default layer.
    async fn default_layer(&self) -> Self {
        self.with(self.graph.default_layer().await)
    }

    /// View restricted to the named layers.
    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        self.with(self.graph.layers(names).await)
    }

    /// View with the named layers hidden.
    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        self.with(self.graph.exclude_layers(names).await)
    }

    /// View restricted to a single layer.
    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.with(self.graph.layer(name).await)
    }

    /// View with one layer hidden.
    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.with(self.graph.exclude_layer(name).await)
    }

    /// View restricted to a set of nodes and the edges between them.
    async fn subgraph(&self, #[graphql(desc = "Node ids to keep.")] nodes: Vec<GqlNodeId>) -> Self {
        self.with(self.graph.subgraph(nodes).await)
    }

    /// View of the triples valid at the end of this view: on the whole graph, the current
    /// state.
    async fn valid(&self) -> Self {
        self.with(self.graph.valid().await)
    }

    /// View restricted to nodes with the given node types.
    async fn subgraph_node_types(
        &self,
        #[graphql(desc = "Node types to include.")] node_types: Vec<String>,
    ) -> Self {
        self.with(self.graph.subgraph_node_types(node_types).await)
    }

    /// View with a set of nodes removed, along with their edges.
    async fn exclude_nodes(
        &self,
        #[graphql(desc = "Node ids to exclude.")] nodes: Vec<GqlNodeId>,
    ) -> Self {
        self.with(self.graph.exclude_nodes(nodes).await)
    }

    /// Windows of a fixed size that move forward by a step, as `Graph.rolling`; each window is
    /// an `RdfGraph`.
    async fn rolling(
        &self,
        #[graphql(desc = "Width of each window, as in `Graph.rolling`.")] window: WindowDuration,
        #[graphql(desc = "Gap between the starts of consecutive windows; defaults to `window`.")]
        step: Option<WindowDuration>,
        #[graphql(desc = "Unit to align the window boundaries to, as in `Graph.rolling`.")]
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlRdfGraphWindowSet, GraphError> {
        Ok(self.windows(self.graph.rolling(window, step, alignment_unit).await?))
    }

    /// Windows that grow by a step, as `Graph.expanding`; each window is an `RdfGraph`.
    async fn expanding(
        &self,
        #[graphql(desc = "How much the window grows by on each step.")] step: WindowDuration,
        #[graphql(desc = "Unit to align the window boundaries to, as in `Graph.expanding`.")]
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlRdfGraphWindowSet, GraphError> {
        Ok(self.windows(self.graph.expanding(step, alignment_unit).await?))
    }

    /// View of the triples that held at some point between start and end.
    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.with(self.graph.window(start, end).await)
    }

    /// View of the state at a time.
    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.with(self.graph.at(time).await)
    }

    /// View of the state at the latest time.
    async fn latest(&self) -> Self {
        self.with(self.graph.latest().await)
    }

    /// View of the triples valid at a time.
    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which triples must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.with(self.graph.snapshot_at(time).await)
    }

    /// View of the triples valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        self.with(self.graph.snapshot_latest().await)
    }

    /// View of the triples that held at some time before `time` (exclusive), including those
    /// retracted before it; `at` gives the state at a time.
    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.with(self.graph.before(time).await)
    }

    /// View of the triples that held at some time after `time` (exclusive).
    async fn after(&self, #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput) -> Self {
        self.with(self.graph.after(time).await)
    }

    /// Sets the start of the window to the larger of the given time and the current start.
    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start; has no effect if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.with(self.graph.shrink_start(start).await)
    }

    /// Sets the end of the window to the smaller of the given time and the current end.
    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end; has no effect if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.with(self.graph.shrink_end(end).await)
    }

    /// View narrowed by a node or edge filter expression, as `Graph.filter`.
    async fn filter(
        &self,
        #[graphql(desc = "Filter expression; if omitted, applies the identity filter.")]
        expr: Option<GqlFilter>,
    ) -> Result<Self, GraphError> {
        Ok(self.with(self.graph.filter(expr).await?))
    }

    /// Applies a list of view operations in order, as `Graph.applyViews`.
    async fn apply_views(
        &self,
        #[graphql(desc = "Ordered list of view operations.")] views: Vec<GraphViewCollection>,
    ) -> Result<Self, GraphError> {
        Ok(self.with(self.graph.apply_views(views).await?))
    }

    /// Returns the graph name.
    async fn name(&self) -> Result<String, PathValidationError> {
        self.graph.name().await
    }

    /// Returns the path of the graph.
    async fn path(&self) -> String {
        self.graph.path().await
    }

    /// Returns the time entry of the earliest activity in the view.
    async fn earliest_time(&self) -> Result<GqlEventTime> {
        self.graph.earliest_time().await
    }

    /// Returns the time entry of the latest activity in the view.
    async fn latest_time(&self) -> Result<GqlEventTime> {
        self.graph.latest_time().await
    }

    /// Returns the start time of the window. Errors if there is no window.
    async fn start(&self) -> Result<GqlEventTime> {
        self.graph.start().await
    }

    /// Returns the end time of the window. Errors if there is no window.
    async fn end(&self) -> Result<GqlEventTime> {
        self.graph.end().await
    }

    /// Returns the size of the window covered by this view (`end - start`), or None if the view
    /// is unbounded.
    async fn window_size(&self) -> Option<i64> {
        self.graph.window_size().await
    }

    /// Returns the names of the layers with an edge event visible in this view.
    async fn unique_layers(&self) -> Vec<String> {
        self.graph.unique_layers().await
    }

    /// Check if a layer with the given name is present in the graph.
    async fn has_layer(&self, #[graphql(desc = "Layer name to look for.")] name: String) -> bool {
        self.graph.has_layer(name).await
    }

    /// The same view as a property graph.
    async fn graph(&self) -> GqlGraph {
        self.graph.clone()
    }

    /// Runs a SPARQL 1.1 query (SELECT, ASK, CONSTRUCT or DESCRIBE) on the triples of this view
    /// and returns the results as one serialized document. `GRAPH <raphtory:asof:T>` matches
    /// `snapshotAt(time: T)` of the view. The temporal functions (`raphtory:validFrom`,
    /// `validTo`, `validFromTime`, `validToTime`) are unavailable when the caller's access to
    /// the graph is row-filtered. Disabled when the server sets `disable_lists`. Queries longer
    /// than the server's `max_sparql_query_length`, with more than `max_sparql_triple_patterns`
    /// triple patterns or with brackets nested too deep are rejected before they run, and
    /// results larger than `max_sparql_result_bytes` fail. RDF/XML fails when it cannot hold
    /// the results, such as a triple whose predicate does not end in an XML name.
    async fn sparql(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The SPARQL 1.1 query.")] query: String,
        #[graphql(
            desc = "The format of the results: JSON (the default), XML, CSV or TSV for SELECT and ASK; an RDF format for CONSTRUCT and DESCRIBE (N_TRIPLES by default), where JSON is JSON-LD and XML is RDF/XML. A format that does not fit the query is an error."
        )]
        format: Option<GqlSparqlFormat>,
    ) -> Result<String> {
        let format = format.map_or_else(SparqlFormat::default, SparqlFormat::from);
        let results = run_sparql(
            ctx.data_opt::<ConcurrencyConfig>(),
            self,
            query,
            None,
            move |view, query, options, config| {
                into_string(write_capped(config, |out| {
                    view.sparql_to_writer(query, out, format, options)
                })?)
            },
        )
        .await?;
        Ok(results)
    }

    /// The triples of this view as one RDF document. Disabled when the server sets
    /// `disable_lists`; documents larger than `max_sparql_result_bytes` fail.
    async fn to_rdf(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The RDF format of the document; N_TRIPLES by default.")] format: Option<
            GqlRdfFormat,
        >,
    ) -> Result<String> {
        let view = self.view()?;
        let format = format.map_or(RdfFormat::NTriples, RdfFormat::from);
        let document = run_limited(ctx.data_opt::<ConcurrencyConfig>(), move |config| {
            into_string(write_capped(config, |out| view.to_rdf(out, format, &[]))?)
        })
        .await?;
        Ok(document)
    }
}

/// The windows of `rolling` or `expanding` on an `RdfGraph`, each an `RdfGraph`.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "RdfGraphWindowSet")]
pub struct GqlRdfGraphWindowSet {
    windows: GqlGraphWindowSet,
    /// The graph the windows are taken from.
    rdf_graph: GqlRdfGraph,
}

impl GqlRdfGraphWindowSet {
    fn rdf_graphs(&self, graphs: Vec<GqlGraph>) -> Vec<GqlRdfGraph> {
        graphs
            .into_iter()
            .map(|graph| self.rdf_graph.with(graph))
            .collect()
    }
}

#[ResolvedObjectFields]
impl GqlRdfGraphWindowSet {
    /// Number of windows in this set.
    async fn count(&self) -> usize {
        self.windows.count().await
    }

    /// One page of windows, as `GraphWindowSet.page`.
    async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of items to return on this page.")] limit: usize,
        #[graphql(desc = "Extra items to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> Result<Vec<GqlRdfGraph>> {
        let graphs = self.windows.page(ctx, limit, offset, page_index).await?;
        Ok(self.rdf_graphs(graphs))
    }

    /// Every window, as `GraphWindowSet.list`. Rejected when the server sets `disable_lists`.
    async fn list(&self, ctx: &Context<'_>) -> Result<Vec<GqlRdfGraph>> {
        let graphs = self.windows.list(ctx).await?;
        Ok(self.rdf_graphs(graphs))
    }
}

/// Adds the `validateShacl` field to `RdfGraph`.
#[cfg(feature = "shacl")]
#[derive(ExpandObject)]
pub struct RdfGraphShacl<'a>(&'a GqlRdfGraph);

#[cfg(feature = "shacl")]
#[ExpandObjectFields]
impl<'a> RdfGraphShacl<'a> {
    /// Validates the triples of this view against a SHACL shapes graph and returns the W3C
    /// `sh:ValidationReport`, with RDF terms. Recursive shapes (a shape that refers to itself)
    /// are rejected. Disabled when the server sets `disable_lists`.
    async fn validate_shacl(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The shapes graph, as an RDF document.")] shapes: String,
        #[graphql(desc = "The RDF format of the shapes; TURTLE by default.")] format: Option<
            GqlRdfFormat,
        >,
        #[graphql(desc = "The RDF format of the report; TURTLE by default.")] report_format: Option<
            GqlRdfFormat,
        >,
    ) -> Result<String> {
        let view = self.0.view()?;
        let format = format.map_or(RdfFormat::Turtle, RdfFormat::from);
        let report_format = report_format.map_or(RdfFormat::Turtle, RdfFormat::from);
        let report = run_limited(ctx.data_opt::<ConcurrencyConfig>(), move |_| {
            let shapes = ShaclShapes::parse(shapes.as_bytes(), format, None)?;
            if shapes.is_recursive() {
                return Err(SparqlError::RecursiveShapes);
            }
            let mut report = Vec::new();
            view.validate_shacl(&shapes)?
                .write(&mut report, report_format, &[])?;
            into_string(report)
        })
        .await?;
        Ok(report)
    }
}

fn into_string(document: Vec<u8>) -> Result<String, SparqlError> {
    Ok(String::from_utf8(document).map_err(RdfError::from)?)
}

/// An RDF format.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[graphql(name = "RdfFormat")]
pub enum GqlRdfFormat {
    /// Turtle.
    Turtle,
    /// N-Triples.
    NTriples,
    /// N-Quads.
    NQuads,
    /// TriG.
    Trig,
    /// RDF/XML.
    RdfXml,
    /// JSON-LD.
    JsonLd,
}

impl From<GqlRdfFormat> for RdfFormat {
    fn from(format: GqlRdfFormat) -> Self {
        match format {
            GqlRdfFormat::Turtle => RdfFormat::Turtle,
            GqlRdfFormat::NTriples => RdfFormat::NTriples,
            GqlRdfFormat::NQuads => RdfFormat::NQuads,
            GqlRdfFormat::Trig => RdfFormat::TriG,
            GqlRdfFormat::RdfXml => RdfFormat::RdfXml,
            GqlRdfFormat::JsonLd => RdfFormat::JsonLd,
        }
    }
}

/// A format of SPARQL results: a SPARQL results format for SELECT and ASK, an RDF format for
/// CONSTRUCT and DESCRIBE.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[graphql(name = "SparqlFormat")]
pub enum GqlSparqlFormat {
    /// SPARQL 1.1 Query Results JSON; JSON-LD for CONSTRUCT and DESCRIBE.
    Json,
    /// SPARQL Query Results XML; RDF/XML for CONSTRUCT and DESCRIBE.
    Xml,
    /// SPARQL 1.1 Query Results CSV.
    Csv,
    /// SPARQL 1.1 Query Results TSV.
    Tsv,
    /// Turtle.
    Turtle,
    /// N-Triples.
    NTriples,
    /// N-Quads.
    NQuads,
    /// TriG.
    Trig,
    /// RDF/XML.
    RdfXml,
    /// JSON-LD.
    JsonLd,
}

impl From<GqlSparqlFormat> for SparqlFormat {
    fn from(format: GqlSparqlFormat) -> Self {
        let rdf = |format: GqlRdfFormat| RdfFormat::from(format).into();
        // as `SparqlFormat::parse` reads the names `json` and `xml`
        let both = |results, graph: GqlRdfFormat| SparqlFormat {
            results: Some(results),
            graph: Some(graph.into()),
        };
        match format {
            GqlSparqlFormat::Json => both(QueryResultsFormat::Json, GqlRdfFormat::JsonLd),
            GqlSparqlFormat::Xml => both(QueryResultsFormat::Xml, GqlRdfFormat::RdfXml),
            GqlSparqlFormat::Csv => QueryResultsFormat::Csv.into(),
            GqlSparqlFormat::Tsv => QueryResultsFormat::Tsv.into(),
            GqlSparqlFormat::Turtle => rdf(GqlRdfFormat::Turtle),
            GqlSparqlFormat::NTriples => rdf(GqlRdfFormat::NTriples),
            GqlSparqlFormat::NQuads => rdf(GqlRdfFormat::NQuads),
            GqlSparqlFormat::Trig => rdf(GqlRdfFormat::Trig),
            GqlSparqlFormat::RdfXml => rdf(GqlRdfFormat::RdfXml),
            GqlSparqlFormat::JsonLd => rdf(GqlRdfFormat::JsonLd),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "shacl")]
    use crate::test_support::rdf::ex;
    use crate::{
        auth_policy::{
            auth_policy_tests::FakePolicy, AuthorizationPolicy, GraphPermission,
            NamespacePermission,
        },
        config::concurrency_config::ConcurrencyConfig,
        model::App,
        sparql::SparqlError,
        test_support::{
            rdf::{
                graph, pairs, people, rows, without_carol, AS_OF_3, COUNT, KNOWN_BY, KNOWN_BY_NOW,
                KNOWS, NOW, SINCE,
            },
            setup_with_graphs, setup_with_policy,
        },
    };
    use async_graphql::{dynamic::Schema, ServerError};
    use dynamic_graphql::{Request, Variables};
    #[cfg(feature = "shacl")]
    use oxigraph::model::NamedNodeRef;
    use oxigraph::{io::RdfFormat, model::Literal, sparql::QueryEvaluationError};
    use raphtory::db::api::view::MaterializedGraph;
    use raphtory_rdf::RdfError;
    use serde_json::{json, Value as Json};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// `pg` holds [`people`], `eg` the same events as an event graph.
    fn graphs() -> Vec<(&'static str, MaterializedGraph)> {
        vec![
            ("pg", people().into()),
            ("eg", people().event_graph().into()),
        ]
    }

    async fn setup(work_dir: &TempDir) -> Schema {
        setup_with_graphs(&graphs(), work_dir.path()).await.schema
    }

    /// A schema whose policy grants `permission` on `pg`.
    async fn setup_as(work_dir: &TempDir, permission: GraphPermission) -> Schema {
        let policy: Arc<dyn AuthorizationPolicy> = Arc::new(
            FakePolicy::default()
                .with_namespace("", NamespacePermission::Read)
                .with_graph("pg", permission),
        );
        setup_with_policy(&graphs(), work_dir.path(), policy)
            .await
            .schema
    }

    /// A schema whose server has the concurrency configuration `config`.
    async fn setup_with_config(work_dir: &TempDir, config: ConcurrencyConfig) -> Schema {
        let data = setup_with_graphs(&graphs(), work_dir.path()).await.data;
        App::create_schema()
            .data(data)
            .data(config)
            .finish()
            .unwrap()
    }

    /// The data of `document` with `variables`, or its first error.
    async fn execute(
        schema: &Schema,
        document: &str,
        variables: Json,
    ) -> Result<Json, ServerError> {
        let request = Request::new(document).variables(Variables::from_json(variables));
        let response = schema.execute(request).await;
        match response.errors.first() {
            Some(error) => Err(error.clone()),
            None => Ok(response.data.into_json().unwrap()),
        }
    }

    /// The results of `query` on `rdfGraph(path: "pg") { valid { .. } }`.
    async fn sparql(schema: &Schema, query: &str) -> Result<String, ServerError> {
        let document =
            r#"query Q($q: String!) { rdfGraph(path: "pg") { valid { sparql(query: $q) } } }"#;
        let data = execute(schema, document, json!({ "q": query })).await?;
        Ok(data["rdfGraph"]["valid"]["sparql"]
            .as_str()
            .unwrap()
            .to_owned())
    }

    #[tokio::test]
    async fn view_chains_end_in_sparql() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let document = r#"query Q($knows: String!, $since: String!, $knownBy: String!) {
            rdfGraph(path: "pg") {
                all: sparql(query: $knows)
                valid { sparql(query: $knows) }
                at(time: 3) { sparql(query: $knows) }
                window(start: 3, end: 6) { valid { sparql(query: $knows) } }
                since: valid { sparql(query: $since) }
                knownBy: valid { sparql(query: $knownBy, format: TURTLE) }
                export: valid { toRdf }
                rolling(window: { epoch: 10 }) { list { valid { sparql(query: $knows) } } }
            }
            events: rdfGraph(path: "eg") { valid { sparql(query: $knows) } }
        }"#;
        let variables = json!({ "knows": KNOWS, "since": SINCE, "knownBy": KNOWN_BY });
        let data = execute(&schema, document, variables).await.unwrap();
        let fields = &data["rdfGraph"];
        let rows_of = |value: &Json| rows(value.as_str().unwrap());
        let graph_of = |value: &Json, format| graph(value.as_str().unwrap(), format);

        let all = [("alice", "bob"), ("bob", "carol"), ("dave", "alice")];
        assert_eq!(rows_of(&fields["all"]), pairs(&all));
        assert_eq!(rows_of(&fields["valid"]["sparql"]), pairs(NOW));
        assert_eq!(rows_of(&fields["at"]["sparql"]), pairs(AS_OF_3));
        assert_eq!(
            rows_of(&fields["window"]["valid"]["sparql"]),
            pairs(&[("bob", "carol")])
        );
        assert_eq!(
            rows_of(&fields["since"]["sparql"]),
            [[Some(Literal::from(1).into())]]
        );
        assert_eq!(
            graph_of(&fields["knownBy"]["sparql"], RdfFormat::Turtle),
            graph(KNOWN_BY_NOW, RdfFormat::NTriples)
        );
        let export = r#"
            <http://ex/alice> <http://ex/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
            <http://ex/bob> <http://ex/knows> <http://ex/carol> .
            <http://ex/carol> <http://ex/name> "Carol" .
            <http://ex/dave> <http://ex/knows> <http://ex/alice> .
        "#;
        assert_eq!(
            graph_of(&fields["export"]["toRdf"], RdfFormat::NTriples),
            graph(export, RdfFormat::NTriples)
        );
        let windows = fields["rolling"]["list"].as_array().unwrap();
        assert_eq!(
            rows_of(&windows.last().unwrap()["valid"]["sparql"]),
            pairs(NOW)
        );
        // an event graph is served as persistent
        assert_eq!(rows_of(&data["events"]["valid"]["sparql"]), pairs(NOW));
    }

    #[cfg(feature = "shacl")]
    #[tokio::test]
    async fn views_validate_against_shacl_shapes() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:knows ;
                sh:property [ sh:path ex:age ; sh:minCount 1 ] .";
        let document = r#"query Q($shapes: String!) {
            rdfGraph(path: "pg") { at(time: 3) { validateShacl(shapes: $shapes, reportFormat: N_TRIPLES) } }
        }"#;
        let data = execute(&schema, document, json!({ "shapes": shapes }))
            .await
            .unwrap();
        let report = graph(
            data["rdfGraph"]["at"]["validateShacl"].as_str().unwrap(),
            RdfFormat::NTriples,
        );
        let focus_node = NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#focusNode");
        // as of 3, bob knows carol but has no age
        assert_eq!(
            report
                .triples_for_predicate(focus_node)
                .map(|triple| triple.object.into_owned())
                .collect::<Vec<_>>(),
            [ex("bob").into()]
        );
    }

    #[cfg(feature = "shacl")]
    #[tokio::test]
    async fn recursive_shapes_are_refused() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ; sh:targetNode ex:alice ;
                sh:property [ sh:path ex:knows ; sh:node ex:S ] .";
        let document = r#"query Q($shapes: String!) {
            rdfGraph(path: "pg") { validateShacl(shapes: $shapes) }
        }"#;
        let error = execute(&schema, document, json!({ "shapes": shapes }))
            .await
            .unwrap_err();
        assert!(matches!(
            error.source::<SparqlError>(),
            Some(SparqlError::RecursiveShapes)
        ));
    }

    #[tokio::test]
    async fn a_row_filter_turns_off_the_temporal_functions() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, GraphPermission::Read { filter: None }).await;
        assert_eq!(
            rows(&sparql(&schema, SINCE).await.unwrap()),
            [[Some(Literal::from(1).into())]]
        );

        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, without_carol()).await;
        assert_eq!(
            rows(&sparql(&schema, KNOWS).await.unwrap()),
            pairs(&[("dave", "alice")])
        );
        let error = sparql(&schema, SINCE).await.unwrap_err();
        assert!(matches!(
            error.source::<SparqlError>(),
            Some(SparqlError::Query(RdfError::SparqlEvaluation(
                QueryEvaluationError::UnsupportedCustomFunction(_)
            )))
        ));
    }

    /// A graph the caller may only introspect, one it has no grant on and one that does not
    /// exist look the same.
    #[tokio::test]
    async fn a_denied_read_looks_like_a_missing_graph() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, GraphPermission::Introspect).await;
        let document = r#"{
            introspect: rdfGraph(path: "pg") { path }
            noGrant: rdfGraph(path: "eg") { path }
            missing: rdfGraph(path: "missing") { path }
        }"#;
        let data = execute(&schema, document, json!({})).await.unwrap();
        assert_eq!(
            data,
            json!({ "introspect": null, "noGrant": null, "missing": null })
        );
    }

    #[tokio::test]
    async fn the_limits_of_the_server_apply() {
        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            max_sparql_query_length: Some(COUNT.len()),
            // `(COUNT(*) AS ?n)` counts one
            max_sparql_triple_patterns: Some(2),
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        assert_eq!(
            rows(&sparql(&schema, COUNT).await.unwrap()),
            [[Some(Literal::from(4).into())]]
        );
        let error = sparql(&schema, &format!("{COUNT} ")).await.unwrap_err();
        assert!(matches!(
            error.source::<SparqlError>(),
            Some(SparqlError::TooLong { length, max })
                if *length == COUNT.len() + 1 && *max == COUNT.len()
        ));
        let error = sparql(&schema, "ASK{?a ?b ?c.?c ?d ?e.?e ?f ?g}")
            .await
            .unwrap_err();
        assert!(matches!(
            error.source::<SparqlError>(),
            Some(SparqlError::Query(RdfError::TooManyPatterns { .. }))
        ));

        let work_dir = TempDir::new().unwrap();
        let config = ConcurrencyConfig {
            disable_lists: true,
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        let disabled = |error: ServerError| {
            matches!(error.source::<SparqlError>(), Some(SparqlError::Disabled))
        };
        assert!(disabled(sparql(&schema, "ASK {}").await.unwrap_err()));
        let export = execute(&schema, r#"{ rdfGraph(path: "pg") { toRdf } }"#, json!({})).await;
        assert!(disabled(export.unwrap_err()));
        #[cfg(feature = "shacl")]
        {
            let document = r#"{ rdfGraph(path: "pg") { validateShacl(shapes: "") } }"#;
            let validation = execute(&schema, document, json!({})).await;
            assert!(disabled(validation.unwrap_err()));
        }
    }
}
