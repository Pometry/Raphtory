use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, LayerOps, VertexViewOps},
    vectors::{graph_entity::GraphEntity, splitting::split_text_by_line_breaks, DocumentInput},
};
use itertools::Itertools;
use std::{convert::identity, sync::Arc};

pub trait DocumentTemplate<G: GraphViewOps>: Send + Sync {
    fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;
    fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;
}

impl<G: GraphViewOps> DocumentTemplate<G> for Arc<dyn DocumentTemplate<G>> {
    fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        self.as_ref().node(vertex)
    }
    fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        self.as_ref().edge(edge)
    }
}

pub struct DefaultTemplate;

const DEFAULT_MAX_SIZE: usize = 1000;

impl<G: GraphViewOps> DocumentTemplate<G> for DefaultTemplate {
    fn node(&self, vertex: &VertexView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let name = vertex.name();
        let property_list = vertex.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        let text_chunks = split_text_by_line_breaks(content, DEFAULT_MAX_SIZE);
        Box::new(text_chunks.into_iter().map(|text| text.into()))
    }

    fn edge(&self, edge: &EdgeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
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
