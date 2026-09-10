use crate::{
    auth::ContextValidation,
    data::Data,
    model::{
        graph::{timeindex::GqlTimeInput, vector_selection::GqlVectorSelection},
        resolve, QueryRoot, Template,
    },
    paths::ExistingGraphFolder,
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{
    ExpandObject, ExpandObjectFields, InputObject, OneOfInput, ResolvedObject, ResolvedObjectFields,
};
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::GraphResult,
    vectors::{
        cache::CachedEmbeddingModel,
        storage::OpenAIEmbeddings,
        template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
        vectorised_graph::VectorisedGraph,
    },
};
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::IntoTime};

#[derive(InputObject, Debug, Clone, Default)]
pub struct OpenAIConfig {
    model: String,
    api_base: Option<String>,
    api_key_env: Option<String>,
    org_id: Option<String>,
    project_id: Option<String>,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EmbeddingModel {
    /// OpenAI embedding models or compatible providers
    OpenAI(OpenAIConfig),
}

impl EmbeddingModel {
    async fn cache<'a>(self, ctx: &Context<'a>) -> GraphResult<CachedEmbeddingModel> {
        let data = ctx.data_unchecked::<Data>();
        match self {
            Self::OpenAI(OpenAIConfig {
                model,
                api_base,
                api_key_env,
                org_id,
                project_id,
            }) => {
                let embeddings = OpenAIEmbeddings {
                    model,
                    api_base,
                    api_key_env,
                    org_id,
                    project_id,
                    dim: None,
                };
                let vector_cache = data.vector_cache.resolve().await?;
                vector_cache.openai(embeddings.into()).await
            }
        }
    }
}

#[derive(ExpandObject)]
pub struct VectorQuery<'a>(&'a QueryRoot); // expand Query object type

#[ExpandObjectFields]
impl<'b> VectorQuery<'b> {
    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph
    async fn vectorise_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
        #[graphql(desc = "Optional embedding model; defaults to OpenAI's standard model.")]
        model: Option<EmbeddingModel>,
        #[graphql(
            desc = "Optional node-document template (which fields go into each node's text representation); defaults to the built-in template."
        )]
        nodes: Option<Template>,
        #[graphql(desc = "Optional edge-document template; defaults to the built-in template.")]
        edges: Option<Template>,
    ) -> async_graphql::Result<bool> {
        {
            ctx.require_jwt_write_access()?;
            let data = ctx.data_unchecked::<Data>();
            let template = DocumentTemplate {
                node_template: resolve(nodes, DEFAULT_NODE_TEMPLATE),
                edge_template: resolve(edges, DEFAULT_EDGE_TEMPLATE),
            };
            let cached_model = model
                .unwrap_or(EmbeddingModel::OpenAI(Default::default()))
                .cache(ctx)
                .await?;
            let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, &path)?;
            data.vectorise_folder(&folder, &template, cached_model)
                .await?;
            Ok(true)
        }
    }

    /// Index only the entities that are missing from an existing vector index, leaving what is
    /// already indexed untouched. Cheap enough to run routinely, and never destructive.
    ///
    /// Fails if the graph has no index yet, or if the template or model differs from the one the
    /// index was built with — `vectoriseGraph` is what covers those, by rebuilding.
    ///
    /// Returns:: bool
    async fn vectorise_missing<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
        #[graphql(desc = "Embedding model; must match the one the index was built with.")]
        model: Option<EmbeddingModel>,
        #[graphql(desc = "Node-document template; must match the one the index was built with.")]
        nodes: Option<Template>,
        #[graphql(desc = "Edge-document template; must match the one the index was built with.")]
        edges: Option<Template>,
    ) -> async_graphql::Result<bool> {
        ctx.require_jwt_write_access()?;
        let data = ctx.data_unchecked::<Data>();
        let template = DocumentTemplate {
            node_template: resolve(nodes, DEFAULT_NODE_TEMPLATE),
            edge_template: resolve(edges, DEFAULT_EDGE_TEMPLATE),
        };
        let cached_model = model
            .unwrap_or(EmbeddingModel::OpenAI(Default::default()))
            .cache(ctx)
            .await?;
        let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, &path)?;
        data.vectorise_missing_in_folder(&folder, &template, cached_model)
            .await?;
        Ok(true)
    }

    /// Create vectorised graph in the format used for queries
    ///
    /// Returns:: GqlVectorisedGraph
    async fn vectorised_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
    ) -> async_graphql::Result<Option<GqlVectorisedGraph>> {
        let data = ctx.data_unchecked::<Data>();
        data.get_vectors_with_read_permission(ctx, path).await
    }
}

#[derive(InputObject)]
pub struct VectorisedGraphWindow {
    /// Inclusive lower bound of the search window.
    start: GqlTimeInput,
    /// Exclusive upper bound of the search window.
    end: GqlTimeInput,
}

pub(super) trait IntoWindowTuple {
    fn into_window_tuple(self) -> Option<(i64, i64)>;
}

impl IntoWindowTuple for Option<VectorisedGraphWindow> {
    fn into_window_tuple(self) -> Option<(i64, i64)> {
        self.map(|window| (window.start.into_time().t(), window.end.into_time().t()))
    }
}

/// A graph with embedded vector representations for its nodes and edges.
/// Exposes similarity search over documents, nodes, and edges, plus
/// selection building (`emptySelection`) and index maintenance
/// (`optimizeIndex`).
#[derive(ResolvedObject)]
#[graphql(name = "VectorisedGraph")]
pub struct GqlVectorisedGraph(VectorisedGraph<MaterializedGraph>);

impl From<VectorisedGraph<MaterializedGraph>> for GqlVectorisedGraph {
    fn from(value: VectorisedGraph<MaterializedGraph>) -> Self {
        Self(value.clone())
    }
}

#[ResolvedObjectFields]
impl GqlVectorisedGraph {
    /// Rebuild (or incrementally update) the on-disk vector indexes for nodes
    /// and edges so subsequent similarity searches hit the fresh embeddings.
    /// Safe to call repeatedly; returns true on success.
    pub async fn optimize_index(&self) -> GraphResult<bool> {
        self.0.optimize_index().await?;
        Ok(true)
    }

    /// Returns an empty selection of documents.
    pub async fn empty_selection(&self) -> GqlVectorSelection {
        self.0.empty_selection().into()
    }

    /// Find the highest-scoring nodes *and* edges (mixed) by similarity to a
    /// natural-language query. The query is embedded server-side and matched
    /// against indexed entity vectors.
    pub async fn entities_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of results to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to entities active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query =
            blocking_compute(move || cloned.entities_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }

    /// Find the highest-scoring nodes by similarity to a natural-language
    /// query. The query is embedded server-side and matched against indexed
    /// node vectors.
    pub async fn nodes_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of nodes to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to nodes active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query = blocking_compute(move || cloned.nodes_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }

    /// Find the highest-scoring edges by similarity to a natural-language
    /// query. The query is embedded server-side and matched against indexed
    /// edge vectors.
    pub async fn edges_by_similarity(
        &self,
        #[graphql(desc = "Natural-language search string; embedded by the server.")] query: String,
        #[graphql(desc = "Maximum number of edges to return.")] limit: usize,
        #[graphql(
            desc = "Optional `{start, end}` to restrict matches to edges active in that interval."
        )]
        window: Option<VectorisedGraphWindow>,
    ) -> GraphResult<GqlVectorSelection> {
        let vector = self.0.embed_text(query).await?;
        let w = window.into_window_tuple();
        let cloned = self.0.clone();
        let query = blocking_compute(move || cloned.edges_by_similarity(&vector, limit, w)).await;
        Ok(query.execute().await?.into())
    }
}
