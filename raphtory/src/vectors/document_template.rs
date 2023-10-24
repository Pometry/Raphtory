use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{graph_entity::GraphEntity, splitting::split_text_by_line_breaks, DocumentInput},
};
use itertools::Itertools;
use std::{convert::identity, iter::Once, vec::IntoIter};

pub trait DocumentTemplate: Send + Sync {
    type Output: Iterator<Item = Self::DocumentOutput>;
    type DocumentOutput: Into<DocumentInput>;

    fn node<G: GraphViewOps>(vertex: &VertexView<G>) -> Self::Output;
    fn edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Self::Output;
}

pub struct DefaultTemplate;

const DEFAULT_MAX_SIZE: usize = 1000;

impl DocumentTemplate for DefaultTemplate {
    type Output = IntoIter<Self::DocumentOutput>;
    type DocumentOutput = String;
    fn node<G: GraphViewOps>(vertex: &VertexView<G>) -> Self::Output {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        split_text_by_line_breaks(content, DEFAULT_MAX_SIZE).into_iter()
    }

    fn edge<G: GraphViewOps>(edge: &EdgeView<G>) -> Self::Output {
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
        split_text_by_line_breaks(content, DEFAULT_MAX_SIZE).into_iter()
    }
}
