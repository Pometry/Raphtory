use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::{EdgeViewOps, LayerOps, NodeViewOps},
    vectors::{graph_entity::GraphEntity, splitting::split_text_by_line_breaks, DocumentInput},
};
use itertools::Itertools;
use std::{convert::identity, sync::Arc};

/// Trait to be implemented for custom document templates
pub trait DocumentTemplate<G: StaticGraphViewOps>: Send + Sync {
    /// A function that translate the graph into an iterator of documents
    fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>>;

    /// A function that translate a node into an iterator of documents
    fn node(&self, node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>>;

    /// A function that translate an edge into an iterator of documents
    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>>;
}

impl<G: StaticGraphViewOps> DocumentTemplate<G> for Arc<dyn DocumentTemplate<G>> {
    fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
        self.as_ref().graph(graph)
    }
    fn node(&self, node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        self.as_ref().node(node)
    }
    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        self.as_ref().edge(edge)
    }
}

pub struct DefaultTemplate;

const DEFAULT_MAX_SIZE: usize = 1000;

impl<G: StaticGraphViewOps> DocumentTemplate<G> for DefaultTemplate {
    fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::empty())
    }

    fn node(&self, node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let name = node.name();
        let property_list = node.generate_property_list(&identity, vec![], vec![]);
        let content = format!("The entity {name} has the following details:\n{property_list}");
        let text_chunks = split_text_by_line_breaks(content, DEFAULT_MAX_SIZE);
        Box::new(text_chunks.into_iter().map(|text| text.into()))
    }

    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
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
