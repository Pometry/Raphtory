use crate::{
    model::schema::{edge_schema::EdgeSchema, get_node_type, MAX_DETAILED_SCHEMA_ENTITIES},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::views::layer_graph::LayeredGraph},
    prelude::*,
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use std::collections::HashMap;

/// Describes a single edge layer — its name and the per `(srcType, dstType)`
/// edge schemas observed within it.
#[derive(ResolvedObject)]
pub(crate) struct LayerSchema<G: StaticGraphViewOps> {
    graph: LayeredGraph<G>,
}

impl<G: StaticGraphViewOps> From<LayeredGraph<G>> for LayerSchema<G> {
    fn from(value: LayeredGraph<G>) -> Self {
        Self { graph: value }
    }
}

#[ResolvedObjectFields]
impl<G: StaticGraphViewOps> LayerSchema<G> {
    /// Returns the name of the layer with this schema
    async fn name(&self) -> String {
        let mut layers = self.graph.unique_layers();
        let layer = layers.next().expect("Layered graph has a layer");
        debug_assert!(
            layers.next().is_none(),
            "Layered graph outputted more than one layer name"
        );
        layer.into()
    }
    /// Returns the list of edge schemas for this edge layer
    async fn edges(&self) -> Vec<EdgeSchema<LayeredGraph<G>>> {
        let graph = self.graph.clone();
        blocking_compute(move || {
            // Single scan over the layer's edges, bucketing them by (src_node_type, dst_node_type)
            let mut buckets: HashMap<(String, String), Vec<EdgeRef>> = HashMap::new();
            let mut total = 0usize;
            for edge in graph.edges().into_iter() {
                // FIXME: Do we stop if we have over 1000 edges or no?
                // total += 1;
                // if total > MAX_DETAILED_SCHEMA_ENTITIES {
                //     // Too many edges to build a detailed schema; abort
                //     return vec![];
                // }
                let src_type = get_node_type(edge.src());
                let dst_type = get_node_type(edge.dst());
                buckets
                    .entry((src_type, dst_type))
                    .or_default()
                    .push(edge.edge);
            }
            buckets
                .into_iter()
                .map(|((src_type, dst_type), edges)| {
                    EdgeSchema::new(graph.clone(), src_type, dst_type, edges)
                })
                .collect()
        })
        .await
    }
}
