//! The `rdfGraph` field of the query root and the `RdfGraph` object: a stored graph viewed as
//! RDF, with persistent semantics.
use crate::{
    config::concurrency_config::ConcurrencyConfig,
    data::{Data, GqlGraphType},
    model::{
        graph::{
            filtering::{GqlFilter, GraphViewCollection},
            graph::GqlGraph,
            node_id::GqlNodeId,
            timeindex::GqlTimeInput,
            windowset::GqlGraphWindowSet,
            GqlAlignmentUnit, WindowDuration,
        },
        QueryRoot,
    },
    sparql::run_limited,
};
use async_graphql::Context;
use dynamic_graphql::{
    Enum, ExpandObject, ExpandObjectFields, ResolvedObject, ResolvedObjectFields, Result,
};
use raphtory::errors::GraphError;
use raphtory_rdf::{
    shacl::{ShaclOptions, ShaclShapes},
    QueryForm, RdfError, RdfFormat, RdfView, SparqlFormat,
};

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
        let graph = data
            .get_graph_with_read_permission(ctx, path, Some(GqlGraphType::Persistent))
            .await?;
        Ok(graph.map(|(folder, graph)| GqlGraph::new(folder, graph).into()))
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
}

impl From<GqlGraph> for GqlRdfGraph {
    fn from(graph: GqlGraph) -> Self {
        Self { graph }
    }
}

impl GqlRdfGraph {
    fn view(&self) -> Result<RdfView, RdfError> {
        RdfView::from_view(self.graph.graph.clone())
    }
}

#[ResolvedObjectFields]
impl GqlRdfGraph {
    /// View restricted to the default layer.
    async fn default_layer(&self) -> Self {
        self.graph.default_layer().await.into()
    }

    /// View restricted to the named layers.
    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        self.graph.layers(names).await.into()
    }

    /// View with the named layers hidden.
    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        self.graph.exclude_layers(names).await.into()
    }

    /// View restricted to a single layer.
    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.graph.layer(name).await.into()
    }

    /// View with one layer hidden.
    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.graph.exclude_layer(name).await.into()
    }

    /// View restricted to a set of nodes and the edges between them.
    async fn subgraph(&self, #[graphql(desc = "Node ids to keep.")] nodes: Vec<GqlNodeId>) -> Self {
        self.graph.subgraph(nodes).await.into()
    }

    /// View of the triples valid at the end of this view: on the whole graph, the current
    /// state.
    async fn valid(&self) -> Self {
        self.graph.valid().await.into()
    }

    /// View restricted to nodes with the given node types.
    async fn subgraph_node_types(
        &self,
        #[graphql(desc = "Node types to include.")] node_types: Vec<String>,
    ) -> Self {
        self.graph.subgraph_node_types(node_types).await.into()
    }

    /// View with a set of nodes removed, along with their edges.
    async fn exclude_nodes(
        &self,
        #[graphql(desc = "Node ids to exclude.")] nodes: Vec<GqlNodeId>,
    ) -> Self {
        self.graph.exclude_nodes(nodes).await.into()
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
        let windows = self.graph.rolling(window, step, alignment_unit).await?;
        Ok(GqlRdfGraphWindowSet { windows })
    }

    /// Windows that grow by a step, as `Graph.expanding`; each window is an `RdfGraph`.
    async fn expanding(
        &self,
        #[graphql(desc = "How much the window grows by on each step.")] step: WindowDuration,
        #[graphql(desc = "Unit to align the window boundaries to, as in `Graph.expanding`.")]
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlRdfGraphWindowSet, GraphError> {
        let windows = self.graph.expanding(step, alignment_unit).await?;
        Ok(GqlRdfGraphWindowSet { windows })
    }

    /// View of the triples that held at some point between start and end.
    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.graph.window(start, end).await.into()
    }

    /// View of the state at a time.
    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.graph.at(time).await.into()
    }

    /// View of the state at the latest time.
    async fn latest(&self) -> Self {
        self.graph.latest().await.into()
    }

    /// View of the triples valid at a time.
    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which triples must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.graph.snapshot_at(time).await.into()
    }

    /// View of the triples valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        self.graph.snapshot_latest().await.into()
    }

    /// View of the triples that held at some time before `time` (exclusive), including those
    /// retracted before it; `at` gives the state at a time.
    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.graph.before(time).await.into()
    }

    /// View of the triples that held at some time after `time` (exclusive).
    async fn after(&self, #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput) -> Self {
        self.graph.after(time).await.into()
    }

    /// Sets the start of the window to the larger of the given time and the current start.
    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start; has no effect if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.graph.shrink_start(start).await.into()
    }

    /// Sets the end of the window to the smaller of the given time and the current end.
    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end; has no effect if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.graph.shrink_end(end).await.into()
    }

    /// View narrowed by a node or edge filter expression, as `Graph.filter`.
    async fn filter(
        &self,
        #[graphql(desc = "Filter expression; if omitted, applies the identity filter.")]
        expr: Option<GqlFilter>,
    ) -> Result<Self, GraphError> {
        Ok(self.graph.filter(expr).await?.into())
    }

    /// Applies a list of view operations in order, as `Graph.applyViews`.
    async fn apply_views(
        &self,
        #[graphql(desc = "Ordered list of view operations.")] views: Vec<GraphViewCollection>,
    ) -> Result<Self, GraphError> {
        Ok(self.graph.apply_views(views).await?.into())
    }

    /// The same view as a property graph.
    async fn graph(&self) -> GqlGraph {
        self.graph.clone()
    }

    /// Runs a SPARQL 1.1 query (SELECT, ASK, CONSTRUCT or DESCRIBE) on the triples of this view
    /// and returns the results as one serialized document. `GRAPH <raphtory:asof:T>` matches
    /// `before(time: T + 1) { valid }` of the view. Queries longer than the server's
    /// `max_sparql_query_length`, with more than `max_sparql_triple_patterns` triple patterns or
    /// with brackets nested too deep are rejected before they run, and results larger than
    /// `max_sparql_result_bytes` fail. RDF/XML fails when it cannot hold the results, such as a
    /// triple whose predicate does not end in an XML name. Null if the query fails unexpectedly.
    async fn sparql(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The SPARQL 1.1 query.")] query: String,
        #[graphql(
            desc = "The format of the results: JSON (the default), XML, CSV or TSV for SELECT and ASK; an RDF format for CONSTRUCT and DESCRIBE (N_TRIPLES by default), where JSON is JSON-LD and XML is RDF/XML. A format that does not fit the query is an error."
        )]
        format: Option<GqlSparqlFormat>,
    ) -> Result<Option<String>> {
        let view = self.view()?;
        let format = format.map_or(Ok(SparqlFormat::default()), GqlSparqlFormat::sparql_format)?;
        run_limited(ctx.data_opt::<ConcurrencyConfig>(), move |options| {
            view.sparql_to_string(&query, format, options)
        })
        .await
    }

    /// Validates the triples of this view against a SHACL shapes graph and returns the W3C
    /// `sh:ValidationReport`, with RDF terms. Recursive shapes (a shape that refers to itself)
    /// are rejected. Null if the validation fails unexpectedly.
    async fn validate_shacl(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The shapes graph, as an RDF document.")] shapes: String,
        #[graphql(
            desc = "The RDF format of the shapes, TURTLE by default; JSON is JSON-LD, XML is RDF/XML, and CSV and TSV are errors."
        )]
        format: Option<GqlSparqlFormat>,
        #[graphql(
            desc = "The RDF format of the report, TURTLE by default; JSON is JSON-LD, XML is RDF/XML, and CSV and TSV are errors."
        )]
        report_format: Option<GqlSparqlFormat>,
    ) -> Result<Option<String>> {
        let view = self.view()?;
        let format = format.map(GqlSparqlFormat::rdf_format).transpose()?;
        let report_format =
            report_format.map_or(Ok(RdfFormat::Turtle), GqlSparqlFormat::rdf_format)?;
        run_limited(ctx.data_opt::<ConcurrencyConfig>(), move |_| {
            let shapes = ShaclShapes::parse(shapes.as_bytes(), format, None)?;
            let options = ShaclOptions::default().with_reject_recursive_shapes(true);
            view.validate_shacl(&shapes, &options)?
                .to_rdf_string(report_format)
        })
        .await
    }
}

/// The windows of `rolling` or `expanding` on an `RdfGraph`, each an `RdfGraph`.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "RdfGraphWindowSet")]
pub struct GqlRdfGraphWindowSet {
    windows: GqlGraphWindowSet,
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
        Ok(graphs.into_iter().map(GqlRdfGraph::from).collect())
    }

    /// Every window, as `GraphWindowSet.list`. Rejected when the server sets `disable_lists`.
    async fn list(&self, ctx: &Context<'_>) -> Result<Vec<GqlRdfGraph>> {
        let graphs = self.windows.list(ctx).await?;
        Ok(graphs.into_iter().map(GqlRdfGraph::from).collect())
    }
}

/// A format of SPARQL results: a SPARQL results format for SELECT and ASK, an RDF format for
/// CONSTRUCT and DESCRIBE. As the format of a SHACL shapes graph or report, an RDF format.
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
    /// N3.
    N3,
}

impl GqlSparqlFormat {
    const ALL: [Self; 11] = [
        Self::Json,
        Self::Xml,
        Self::Csv,
        Self::Tsv,
        Self::Turtle,
        Self::NTriples,
        Self::NQuads,
        Self::Trig,
        Self::RdfXml,
        Self::JsonLd,
        Self::N3,
    ];

    /// The name of the format, as [`SparqlFormat::parse`] and [`RdfFormat::parse`] read it.
    fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Xml => "xml",
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Turtle => "ttl",
            Self::NTriples => "nt",
            Self::NQuads => "nq",
            Self::Trig => "trig",
            Self::RdfXml => "rdf",
            Self::JsonLd => "jsonld",
            Self::N3 => "n3",
        }
    }

    fn sparql_format(self) -> Result<SparqlFormat, RdfError> {
        SparqlFormat::parse(self.name())
    }

    fn rdf_format(self) -> Result<RdfFormat, RdfError> {
        RdfFormat::parse(self.name())
    }

    /// The value that names `format` for the results of a query of `form`.
    pub(crate) fn naming(form: QueryForm, format: SparqlFormat) -> Option<Self> {
        Self::ALL.into_iter().find(|name| {
            name.sparql_format().is_ok_and(|named| {
                if form.is_graph() {
                    named.graph == format.graph
                } else {
                    named.results == format.results
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::GqlSparqlFormat;
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
                ex, graph, pairs, people, rows, without_carol, AS_OF_3, COUNT, KNOWN_BY,
                KNOWN_BY_NOW, KNOWS, NOW,
            },
            setup_with_graphs, setup_with_policy,
        },
    };
    use async_graphql::{dynamic::Schema, ServerError};
    use dynamic_graphql::{Request, Variables};
    use oxigraph::{
        io::RdfFormat as OxRdfFormat,
        model::{Literal, NamedNodeRef},
    };
    use raphtory::db::api::view::MaterializedGraph;
    use raphtory_rdf::{
        shacl::ShaclLimit, QueryForm, QueryResultsFormat, RdfError, RdfFormat, SparqlFormat,
    };
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

    fn rdf_error(error: &ServerError) -> Option<&RdfError> {
        match error.source::<SparqlError>() {
            Some(SparqlError::Query(error)) => Some(error),
            _ => None,
        }
    }

    /// Every format the endpoint can choose has a value that names it.
    #[test]
    fn every_format_has_a_value() {
        let named = |form, format: SparqlFormat| {
            GqlSparqlFormat::naming(form, format)
                .unwrap()
                .sparql_format()
                .unwrap()
        };
        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Csv,
            QueryResultsFormat::Tsv,
        ] {
            assert_eq!(
                named(QueryForm::Select, format.into()).results,
                Some(format)
            );
        }
        for format in [
            RdfFormat::Turtle,
            RdfFormat::NTriples,
            RdfFormat::NQuads,
            RdfFormat::TriG,
            RdfFormat::RdfXml,
            RdfFormat::JsonLd,
            RdfFormat::N3,
        ] {
            assert_eq!(
                named(QueryForm::Construct, format.into()).graph,
                Some(format)
            );
        }
    }

    #[tokio::test]
    async fn view_chains_end_in_sparql() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup(&work_dir).await;
        let document = r#"query Q($knows: String!, $knownBy: String!) {
            rdfGraph(path: "pg") {
                all: sparql(query: $knows)
                valid { sparql(query: $knows) }
                at(time: 3) { sparql(query: $knows) }
                window(start: 3, end: 6) { valid { sparql(query: $knows) } }
                knownBy: valid { sparql(query: $knownBy, format: TURTLE) }
                rolling(window: { epoch: 10 }) { list { valid { sparql(query: $knows) } } }
            }
            events: rdfGraph(path: "eg") { valid { sparql(query: $knows) } }
        }"#;
        let variables = json!({ "knows": KNOWS, "knownBy": KNOWN_BY });
        let data = execute(&schema, document, variables).await.unwrap();
        let fields = &data["rdfGraph"];
        let rows_of = |value: &Json| rows(value.as_str().unwrap());

        let all = [("alice", "bob"), ("bob", "carol"), ("dave", "alice")];
        assert_eq!(rows_of(&fields["all"]), pairs(&all));
        assert_eq!(rows_of(&fields["valid"]["sparql"]), pairs(NOW));
        assert_eq!(rows_of(&fields["at"]["sparql"]), pairs(AS_OF_3));
        assert_eq!(
            rows_of(&fields["window"]["valid"]["sparql"]),
            pairs(&[("bob", "carol")])
        );
        assert_eq!(
            graph(
                fields["knownBy"]["sparql"].as_str().unwrap(),
                OxRdfFormat::Turtle
            ),
            graph(KNOWN_BY_NOW, OxRdfFormat::NTriples)
        );
        let windows = fields["rolling"]["list"].as_array().unwrap();
        assert_eq!(
            rows_of(&windows.last().unwrap()["valid"]["sparql"]),
            pairs(NOW)
        );
        // an event graph is served as persistent
        assert_eq!(rows_of(&data["events"]["valid"]["sparql"]), pairs(NOW));
    }

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
            OxRdfFormat::NTriples,
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

        let recursive = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ; sh:targetNode ex:alice ;
                sh:property [ sh:path ex:knows ; sh:node ex:S ] .";
        let document = r#"query Q($shapes: String!) {
            rdfGraph(path: "pg") { validateShacl(shapes: $shapes) }
        }"#;
        let error = execute(&schema, document, json!({ "shapes": recursive }))
            .await
            .unwrap_err();
        assert!(matches!(
            rdf_error(&error),
            Some(RdfError::ShaclLimit(ShaclLimit::RecursiveShapes))
        ));
    }

    /// The caller's access filter applies, and a graph the caller may only introspect, one it
    /// has no grant on and one that does not exist look the same.
    #[tokio::test]
    async fn reads_follow_the_permissions() {
        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, without_carol()).await;
        assert_eq!(
            rows(&sparql(&schema, KNOWS).await.unwrap()),
            pairs(&[("dave", "alice")])
        );

        let work_dir = TempDir::new().unwrap();
        let schema = setup_as(&work_dir, GraphPermission::Introspect).await;
        let document = r#"{
            introspect: rdfGraph(path: "pg") { graph { path } }
            noGrant: rdfGraph(path: "eg") { graph { path } }
            missing: rdfGraph(path: "missing") { graph { path } }
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
            ..Default::default()
        };
        let schema = setup_with_config(&work_dir, config).await;
        assert_eq!(
            rows(&sparql(&schema, COUNT).await.unwrap()),
            [[Some(Literal::from(4).into())]]
        );
        let error = sparql(&schema, &format!("{COUNT} ")).await.unwrap_err();
        assert!(matches!(
            rdf_error(&error),
            Some(RdfError::QueryTooLong { .. })
        ));
    }
}
