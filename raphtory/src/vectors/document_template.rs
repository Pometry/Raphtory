use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{graph_entity::GraphEntity, splitting::split_text_by_line_breaks, DocumentInput},
};
use itertools::Itertools;
use std::{convert::identity, iter::Once, vec::IntoIter};

pub trait DocumentTemplate: Send + Sync {
    fn node<G: GraphViewOps>(
        &self,
        vertex: &VertexView<G>,
    ) -> Box<dyn Iterator<Item = DocumentInput>>;
    fn edge<G: GraphViewOps>(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;
}

pub struct DefaultTemplate;

const DEFAULT_MAX_SIZE: usize = 1000;

impl DocumentTemplate for DefaultTemplate {
    fn node<G: GraphViewOps>(
        &self,
        vertex: &VertexView<G>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        let text_chunks = split_text_by_line_breaks(content, DEFAULT_MAX_SIZE);
        Box::new(text_chunks.into_iter().map(|text| text.into()))
    }

    fn edge<G: GraphViewOps>(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
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
        let text_chunks = split_text_by_line_breaks(content, DEFAULT_MAX_SIZE);
        Box::new(text_chunks.into_iter().map(|text| text.into()))
    }
}
