use crate::model::schema::{edge_echema::EdgeSchema, get_vertex_type};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::graph::views::layer_graph::LayeredGraph,
    prelude::{EdgeViewOps, GraphViewOps},
};

#[derive(ResolvedObject)]
pub(crate) struct LayerSchema<G: GraphViewOps> {
    graph: LayeredGraph<G>,
}

impl<G: GraphViewOps> From<LayeredGraph<G>> for LayerSchema<G> {
    fn from(value: LayeredGraph<G>) -> Self {
        Self { graph: value }
    }
}

#[ResolvedObjectFields]
impl<G: GraphViewOps> LayerSchema<G> {
    /// Returns the name of the layer with this schema
    async fn name(&self) -> String {
        match &self.graph.get_unique_layers()[..] {
            [layer] => layer.clone(),
            _ => panic!("Layered graph outputted more than one layer name"),
        }
    }
    /// Returns the list of edge schemas for this edge layer
    async fn edges(&self) -> Vec<EdgeSchema<LayeredGraph<G>>> {
        self.graph
            .edges()
            .into_iter()
            .map(|edge| {
                let src_type = get_vertex_type(edge.src());
                let dst_type = get_vertex_type(edge.dst());
                (src_type, dst_type)
            })
            .unique()
            .map(|(src_type, dst_type)| EdgeSchema::new(self.graph.clone(), src_type, dst_type))
            .collect_vec()
    }
}
