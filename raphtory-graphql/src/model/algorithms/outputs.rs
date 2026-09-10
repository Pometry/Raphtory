use crate::{
    model::graph::{edge::GqlEdge, node::GqlNode, node_id::GqlNodeId},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::{algorithms::bipartite::max_weight_matching::Matching, db::api::view::DynamicGraph};

/// The motif counts for a single delta. Wraps the counts in an object because
/// the schema builder does not support nested lists of scalars.
#[derive(SimpleObject)]
#[graphql(name = "MotifCounts")]
pub struct GqlMotifCounts {
    /// The delta these counts were computed for.
    pub(crate) delta: i64,
    /// The 40 motif counts, positionally ordered (see the core docs).
    pub(crate) counts: Vec<usize>,
}

/// A matching of a graph: a set of edges no two of which share a node.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Matching")]
pub struct GqlMatching {
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
    pub async fn count(&self) -> usize {
        self.matching.len()
    }

    /// The edges in the matching.
    pub async fn edges(&self) -> crate::model::graph::edges::GqlEdges {
        crate::model::graph::edges::GqlEdges::new(self.matching.edges())
    }

    /// The node matched to `dst`, null if it is unmatched.
    pub async fn src(
        &self,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> Option<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.src(dst).map(|n| n.cloned().into())).await
    }

    /// The node matched to `src`, null if it is unmatched.
    pub async fn dst(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
    ) -> Option<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.dst(src).map(|n| n.cloned().into())).await
    }

    /// The matched edge for `src`, null if it is unmatched.
    pub async fn edge_for_src(
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
    pub async fn edge_for_dst(
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
    pub async fn contains(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.matching.contains(src, dst)).await
    }
}
