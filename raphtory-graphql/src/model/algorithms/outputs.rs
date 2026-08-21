use crate::{
    model::graph::{edge::GqlEdge, node::GqlNode, node_id::GqlNodeId},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::{
    algorithms::{bipartite::max_weight_matching::Matching, pathing::scored_paths::ScoredPath},
    db::{api::view::DynamicGraph, graph::node::NodeView},
};

/// The motif counts for a single delta. Wraps the counts in an object because
/// the schema builder does not support nested lists of scalars.
#[derive(SimpleObject)]
#[graphql(name = "MotifCounts")]
pub(crate) struct GqlMotifCounts {
    /// The delta these counts were computed for.
    pub(crate) delta: i64,
    /// The 40 motif counts, positionally ordered (see the core docs).
    pub(crate) counts: Vec<usize>,
}

/// A matching of a graph: a set of edges no two of which share a node.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Matching")]
pub(crate) struct GqlMatching {
    pub(crate) matching: Matching<DynamicGraph>,
}

impl From<Matching<DynamicGraph>> for GqlMatching {
    fn from(matching: Matching<DynamicGraph>) -> Self {
        Self { matching }
    }
}

#[ResolvedObjectFields]
impl GqlMatching {
    /// Returns the number of edges in the matching.
    async fn count(&self) -> usize {
        self.matching.len()
    }

    /// The edges in the matching.
    async fn edges(&self) -> crate::model::graph::edges::GqlEdges {
        crate::model::graph::edges::GqlEdges::new(self.matching.edges())
    }

    /// The node matched to `dst`, null if it is unmatched.
    async fn src(
        &self,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> Option<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.src(dst).map(|n| n.cloned().into())).await
    }

    /// The node matched to `src`, null if it is unmatched.
    async fn dst(&self, #[graphql(desc = "Source node id.")] src: GqlNodeId) -> Option<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.dst(src).map(|n| n.cloned().into())).await
    }

    /// The matched edge for `src`, null if it is unmatched.
    async fn edge_for_src(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
    ) -> Option<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .matching
                .edge_for_src(src)
                .map(|e| e.cloned().into())
        })
        .await
    }

    /// The matched edge for `dst`, null if it is unmatched.
    async fn edge_for_dst(
        &self,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> Option<GqlEdge> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .matching
                .edge_for_dst(dst)
                .map(|e| e.cloned().into())
        })
        .await
    }

    /// Whether the `src` to `dst` edge is part of the matching.
    async fn contains(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.contains(src, dst)).await
    }
}

/// A path to a destination node, together with its total score.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "ScoredPath")]
pub(crate) struct GqlScoredPath {
    graph: DynamicGraph,
    path: ScoredPath,
}

impl GqlScoredPath {
    pub(crate) fn new(graph: &DynamicGraph, path: ScoredPath) -> Self {
        Self {
            graph: graph.clone(),
            path,
        }
    }
}

#[ResolvedObjectFields]
impl GqlScoredPath {
    /// Sum of every node score and edge score along the path.
    async fn score(&self) -> f64 {
        self.path.score
    }

    /// The nodes on the path, starting at the start node and ending at the destination.
    async fn nodes(&self) -> Vec<GqlNode> {
        self.path
            .nodes
            .iter()
            .map(|node| {
                let node: NodeView<'static, DynamicGraph> =
                    NodeView::new_internal(self.graph.clone(), *node);
                node.into()
            })
            .collect()
    }

    /// The layer traversed at each hop: `layers[i]` connects `nodes[i]` to `nodes[i + 1]`.
    async fn layers(&self) -> Vec<String> {
        self.path
            .layers
            .iter()
            .map(|layer| layer.to_string())
            .collect()
    }

    /// The number of hops on the path.
    async fn hops(&self) -> usize {
        self.path.layers.len()
    }
}
