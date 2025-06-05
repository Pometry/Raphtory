use crate::model::schema::{edge_schema::EdgeSchema, get_node_type};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::views::layer_graph::LayeredGraph},
    prelude::*,
};

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
        self.graph
            .edges()
            .into_iter()
            .map(|edge| {
                let src_type = get_node_type(edge.src());
                let dst_type = get_node_type(edge.dst());
                (src_type, dst_type)
            })
            .unique()
            .map(|(src_type, dst_type)| EdgeSchema::new(self.graph.clone(), src_type, dst_type))
            .collect_vec()
    }
}
