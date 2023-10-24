use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{graph_entity::GraphEntity, DocumentInput},
};
use itertools::Itertools;
use std::convert::identity;

pub trait DocumentTemplate: Send + Sync {
    fn node<G: GraphViewOps>(vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;
    fn edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;
}

pub struct DefaultTemplate;

impl DocumentTemplate for DefaultTemplate {
    fn node<G: GraphViewOps>(vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        Box::new(std::iter::once(content.into()))
    }

    fn edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let src = edge.src().name();
        let dst = edge.dst().name();
        // TODO: property list

        let layer_lines = edge.layer_names().map(|layer| {
            let times = edge
                .layer(layer.clone())
                .unwrap()
                .history()
                .iter()
                .join(", ");
            match layer.as_ref() {
                "_default" => format!("{src} interacted with {dst} at times: {times}"),
                layer => format!("{src} {layer} {dst} at times: {times}"),
            }
        });
        let content: String =
            itertools::Itertools::intersperse(layer_lines, "\n".to_owned()).collect();
        Box::new(std::iter::once(content.into()))
    }
}
