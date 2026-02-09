use super::{
    entity_db::EntityDb,
    entity_ref::EntityRef,
    utils::{apply_window, find_top_k},
    vectorised_graph::VectorisedGraph,
    Document, DocumentEntity, Embedding,
};
use crate::{
    core::entities::nodes::node_ref::AsNodeRef,
    db::{
        api::view::{DynamicGraph, StaticGraphViewOps},
        graph::{edge::EdgeView, node::NodeView},
    },
    errors::GraphResult,
    prelude::{EdgeViewOps, NodeViewOps, *},
    vectors::{vector_collection::VectorCollection, VectorsQuery},
};
use either::Either;
use futures_util::future::join_all;
use itertools::Itertools;
use std::{collections::HashSet, future::Future};

#[derive(Clone, Copy)]
enum ExpansionPath {
    Nodes,
    Edges,
    Both,
}

impl ExpansionPath {
    fn includes_nodes(&self) -> bool {
        matches!(self, Self::Nodes | Self::Both)
    }

    fn includes_edges(&self) -> bool {
        matches!(self, Self::Edges | Self::Both)
    }
}

#[derive(Debug, Clone)]
struct Selected(Vec<(EntityRef, f32)>);

impl From<Vec<(EntityRef, f32)>> for Selected {
    fn from(value: Vec<(EntityRef, f32)>) -> Self {
        Self(value)
    }
}

impl Selected {
    fn extend(&mut self, extension: impl IntoIterator<Item = (EntityRef, f32)>) {
        self.extend_with_limit(extension, usize::MAX);
    }

    fn extend_with_limit(
        &mut self,
        extension: impl IntoIterator<Item = (EntityRef, f32)>,
        limit: usize,
    ) {
        let selection_set: HashSet<EntityRef> =
            HashSet::from_iter(self.0.iter().map(|(doc, _)| *doc));
        let new_docs = extension
            .into_iter()
            .unique_by(|(entity, _)| *entity)
            .filter(|(entity, _)| !selection_set.contains(entity))
            .take(limit);
        self.0.extend(new_docs);
    }

    fn iter(&self) -> impl Iterator<Item = &(EntityRef, f32)> {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub type DynamicVectorSelection = VectorSelection<DynamicGraph>;

pub type QueryPair = (
    VectorsQuery<GraphResult<Vec<(EntityRef, f32)>>>,
    VectorsQuery<GraphResult<Vec<(EntityRef, f32)>>>,
);

pub trait BlockingExecutor<Fut>:
    Fn(Box<dyn FnOnce() -> QueryPair + Send + 'static>) -> Fut + Send + Sync + Clone + 'static
where
    Fut: std::future::Future<Output = QueryPair> + Send,
{
}

impl<Fut, T> BlockingExecutor<Fut> for T
where
    Fut: std::future::Future<Output = QueryPair> + Send,
    T: Fn(Box<dyn FnOnce() -> QueryPair + Send + 'static>) -> Fut + Send + Sync + Clone + 'static,
{
}

pub async fn noop_executor(f: Box<dyn FnOnce() -> QueryPair + Send + 'static>) -> QueryPair {
    f()
}

#[derive(Clone)]
pub struct VectorSelection<G: StaticGraphViewOps> {
    pub(crate) graph: VectorisedGraph<G>,
    selected: Selected, // FIXME: this is a bit error prone, might contain duplicates
}

impl<G: StaticGraphViewOps> VectorSelection<G> {
    pub(crate) fn empty(graph: VectorisedGraph<G>) -> Self {
        Self {
            graph,
            selected: vec![].into(),
        }
    }

    pub(super) fn new(graph: VectorisedGraph<G>, docs: Vec<(EntityRef, f32)>) -> Self {
        Self {
            graph,
            selected: docs.into(),
        }
    }

    /// Returns the vectorised graph instance behind this selection
    pub fn get_vectorised_graph(&self) -> &VectorisedGraph<G> {
        &self.graph
    }

    /// Return the nodes present in the current selection
    pub fn nodes(&self) -> Vec<NodeView<'static, G>> {
        let g = &self.graph.source_graph;
        self.selected
            .iter()
            .filter_map(|(id, _)| id.as_node_view(g))
            .collect()
    }

    /// Return the edges present in the current selection
    pub fn edges(&self) -> Vec<EdgeView<G>> {
        let g = &self.graph.source_graph;
        self.selected
            .iter()
            .filter_map(|(id, _)| id.as_edge_view(g))
            .collect()
    }

    /// Return the documents present in the current selection
    pub async fn get_documents(&self) -> GraphResult<Vec<Document<G>>> {
        Ok(self
            .get_documents_with_distances()
            .await?
            .into_iter()
            .map(|(doc, _)| doc)
            .collect())
    }

    /// Return the documents alongside their distances present in the current selection
    pub async fn get_documents_with_distances(&self) -> GraphResult<Vec<(Document<G>, f32)>> {
        let futures = self.selected.iter().map(|(entity, distance)| async {
            self.regenerate_doc(*entity)
                .await
                .map(|doc| (doc, *distance))
        });
        join_all(futures).await.into_iter().collect()
    }

    /// Add all `nodes` to the current selection
    ///
    /// Documents added by this call are assumed to have a distance of 0.
    /// If any node id doesn't exist it will be ignored
    ///
    /// # Arguments
    ///   * nodes - a list of the node ids or nodes to add
    pub fn add_nodes<V: AsNodeRef>(&mut self, nodes: Vec<V>) {
        let new_docs = nodes
            .into_iter()
            .filter_map(|id| Some(self.graph.source_graph.node(id)?.into()));
        self.selected.extend(new_docs.map(|doc| (doc, 0.0)));
    }

    /// Add all `edges` to the current selection
    ///
    /// Documents added by this call are assumed to have a distance of 0.
    /// If any edge doesn't exist it will be ignored
    ///
    /// # Arguments
    ///   * edges - a list of the edge ids or edges to add
    pub fn add_edges<V: AsNodeRef>(&mut self, edges: Vec<(V, V)>) {
        let new_docs = edges
            .into_iter()
            .filter_map(|(src, dst)| Some(self.graph.source_graph.edge(src, dst)?.into()));
        // self.extend_selection_with_refs(new_docs);
        self.selected.extend(new_docs.map(|doc| (doc, 0.0)));
    }

    /// Append all the documents in `selection` to the current selection
    ///
    /// # Arguments
    ///   * selection - a selection to be added
    ///
    /// # Returns
    ///   The selection with the new documents
    pub fn append(&mut self, selection: &Self) -> &Self {
        self.selected.extend(selection.selected.iter().cloned());
        self
    }

    /// Add all the documents `hops` hops away to the selection
    ///
    /// Two documents A and B are considered to be 1 hop away of each other if they are on the same
    /// entity or if they are on the same node/edge pair. Provided that, two nodes A and C are n
    /// hops away of  each other if there is a document B such that A is n - 1 hops away of B and B
    /// is 1 hop away of C.
    ///
    /// # Arguments
    ///   * hops - the number of hops to carry out the expansion
    ///   * window - the window where documents need to belong to in order to be considered
    pub fn expand(&mut self, hops: usize, window: Option<(i64, i64)>) {
        let nodes = self.get_nodes_in_context(window, false);
        let edges = self.get_edges_in_context(window, false);
        let docs = nodes.into_iter().chain(edges).map(|entity| (entity, 0.0));
        self.selected.extend(docs);
        if hops > 1 {
            self.expand(hops - 1, window);
        }
    }

    /// Add to the selection the `limit` adjacent entities closest to `query`
    ///
    /// The expansion algorithm is a loop with two steps on each iteration:
    ///
    /// 1. All the entities 1 hop away of some of the entities included on the selection (and
    ///    not already selected) are marked as candidates.
    /// 2. Those candidates are added to the selection in ascending distance from `query`.
    ///
    /// This loops goes on until the number of new entities reaches a total of `limit`
    /// entities or until no more documents are available
    ///
    /// Args:
    ///   query (str | list): the text or the embedding to calculate the distance from
    ///   limit (int): the number of documents to add
    ///   window (Tuple[int | str, int | str], optional): the window where documents need to belong to in order to be considered
    ///
    /// Returns:
    ///     None:
    pub async fn expand_entities_by_similarity<Fut, Exec>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        executor: Exec,
    ) -> GraphResult<()>
    where
        Fut: Future<Output = QueryPair> + Send,
        Exec: BlockingExecutor<Fut>,
    {
        self.expand_by_similarity(query, limit, window, ExpansionPath::Both, executor)
            .await
    }

    /// Add to the selection the `limit` adjacent nodes closest to `query`
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers nodes.
    ///
    /// # Arguments
    ///   * query - the embedding to calculate the distance from
    ///   * limit - the maximum number of new nodes to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub async fn expand_nodes_by_similarity<Fut, Exec>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        executor: Exec,
    ) -> GraphResult<()>
    where
        Fut: Future<Output = QueryPair> + Send,
        Exec: BlockingExecutor<Fut>,
    {
        self.expand_by_similarity(query, limit, window, ExpansionPath::Nodes, executor)
            .await
    }

    /// Add to the selection the `limit` adjacent edges closest to `query`
    ///
    /// This function has the same behavior as expand_entities_by_similarity but it only considers edges.
    ///
    /// # Arguments
    ///   * query - the embedding to calculate the distance from
    ///   * limit - the maximum number of new edges to add
    ///   * window - the window where documents need to belong to in order to be considered
    pub async fn expand_edges_by_similarity<Fut, Exec>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        executor: Exec,
    ) -> GraphResult<()>
    where
        Fut: Future<Output = QueryPair> + Send,
        Exec: BlockingExecutor<Fut>,
    {
        self.expand_by_similarity(query, limit, window, ExpansionPath::Edges, executor)
            .await
    }

    // it's very important that we keep raphtory access separate from async blocks querying to lancedb
    // so that we can wrap the former with things like blocking_compute in raphtory-graphql
    async fn expand_by_similarity<Fut, Exec>(
        &mut self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
        path: ExpansionPath,
        executor: Exec,
    ) -> GraphResult<()>
    where
        Fut: Future<Output = QueryPair> + Send,
        Exec: BlockingExecutor<Fut>,
    {
        let initial_size = self.selected.len();
        let cloned_query = query.clone();
        let cloned_self = self.clone();

        let (node_query, edge_query) = executor(Box::new(move || {
            let g = &cloned_self.graph.source_graph;
            let view = apply_window(g, window);

            let node_query = if path.includes_nodes() {
                let jump = matches!(path, ExpansionPath::Nodes);
                let filter = cloned_self.get_nodes_in_context(window, jump);
                cloned_self
                    .graph
                    .node_db
                    .top_k(&cloned_query, limit, view.clone(), Some(filter))
            } else {
                VectorsQuery::resolved(Ok(vec![]))
            };

            let edge_query = if path.includes_edges() {
                let jump = matches!(path, ExpansionPath::Edges);
                let filter = cloned_self.get_edges_in_context(window, jump);
                cloned_self
                    .graph
                    .edge_db
                    .top_k(&cloned_query, limit, view, Some(filter))
            } else {
                VectorsQuery::resolved(Ok(vec![]))
            };

            (node_query, edge_query)
        }))
        .await;

        let nodes = node_query.execute().await?;
        let edges = edge_query.execute().await?;

        let docs = find_top_k(nodes.into_iter().chain(edges), limit).collect::<Vec<_>>();
        self.selected.extend_with_limit(docs, limit);

        let increment = self.selected.len() - initial_size;
        println!("limit {increment}, limit: {limit}, stop? {}", increment > 0 && increment < limit);
        if increment > 0 && increment < limit {
            Box::pin(self.expand_by_similarity(query, limit, window, path, executor)).await?
        }
        Ok(())
    }

    fn get_nodes_in_context(&self, window: Option<(i64, i64)>, jump: bool) -> HashSet<EntityRef> {
        match window {
            Some((start, end)) => self
                .get_nodes_in_context_for_view(&self.graph.source_graph.window(start, end), jump),
            None => self.get_nodes_in_context_for_view(&self.graph.source_graph, jump),
        }
    }

    fn get_edges_in_context(&self, window: Option<(i64, i64)>, jump: bool) -> HashSet<EntityRef> {
        match window {
            Some((start, end)) => self
                .get_edges_in_context_for_view(&self.graph.source_graph.window(start, end), jump),
            None => self.get_edges_in_context_for_view(&self.graph.source_graph, jump),
        }
    }

    fn get_nodes_in_context_for_view<W: StaticGraphViewOps>(
        &self,
        v: &W,
        jump: bool,
    ) -> HashSet<EntityRef> {
        let iter = self.selected.iter();
        iter.flat_map(|(e, _)| e.get_adjacent_nodes(v, jump))
            .collect()
    }

    fn get_edges_in_context_for_view<W: StaticGraphViewOps>(
        &self,
        v: &W,
        jump: bool,
    ) -> HashSet<EntityRef> {
        let iter = self.selected.iter();
        iter.flat_map(|(e, _)| e.get_adjacent_edges(v, jump))
            .collect()
    }

    async fn regenerate_doc(&self, entity: EntityRef) -> GraphResult<Document<G>> {
        match entity.resolve_entity(&self.graph.source_graph).unwrap() {
            Either::Left(node) => Ok(Document {
                entity: DocumentEntity::Node(node.clone()),
                content: self.graph.template.node(node).unwrap(),
                embedding: self.graph.node_db.get_id(entity.id()).await?.unwrap(),
            }),
            Either::Right(edge) => Ok(Document {
                entity: DocumentEntity::Edge(edge.clone()),
                content: self.graph.template.edge(edge).unwrap(),
                embedding: self.graph.edge_db.get_id(entity.id()).await?.unwrap(),
            }),
        }
    }
}

// TODO: we could make get_neighbour_nodes rely on get_neighbour_edges and viceversa, reusing some code
impl EntityRef {
    fn get_adjacent_nodes<G: StaticGraphViewOps>(
        &self,
        view: &G,
        jump: bool,
    ) -> impl Iterator<Item = EntityRef> {
        let nodes: Box<dyn Iterator<Item = NodeView<'static, _>>> =
            if let Some(node) = self.as_node_view(view) {
                if jump {
                    let docs = node.neighbours().into_iter();
                    Box::new(docs)
                } else {
                    Box::new(std::iter::empty())
                }
            } else if let Some(edge) = self.as_edge_view(view) {
                Box::new([edge.src(), edge.dst()].into_iter())
            } else {
                Box::new(std::iter::empty())
            };
        nodes.map(|node| node.into())
    }

    fn get_adjacent_edges<G: StaticGraphViewOps>(
        &self,
        view: &G,
        jump: bool,
    ) -> impl Iterator<Item = EntityRef> {
        let edges: Box<dyn Iterator<Item = EdgeView<_>>> =
            if let Some(node) = self.as_node_view(view) {
                let docs = node.edges().into_iter();
                Box::new(docs)
            } else if let Some(edge) = self.as_edge_view(view) {
                if jump {
                    let src_edges = edge.src().edges().into_iter();
                    let dst_edges = edge.dst().edges().into_iter();
                    Box::new(src_edges.chain(dst_edges))
                } else {
                    Box::new(std::iter::empty())
                }
            } else {
                Box::new(std::iter::empty())
            };
        edges.map(|edge| edge.into())
    }
}
