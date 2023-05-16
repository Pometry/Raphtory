use std::sync::Arc;

use raphtory::{
    db::{graph::Graph, view_api::GraphViewOps},
    graph_loader::source::csv_loader::CsvLoader, core::Prop,
};
use serde::Deserialize;

pub(crate) type TGraph = Graph;

pub(crate) struct Metadata<G: GraphViewOps> {
    parent_graph: Arc<G>,
}
#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

impl Metadata<TGraph> {

    pub(crate) fn graph(&self) -> &TGraph {
        &self.parent_graph
    }

    pub(crate) fn lotr() -> Self {
        let graph = TGraph::new(4);

        CsvLoader::new("/tmp/lotr.csv")
            .load_into_graph(&graph, |lotr: Lotr, g: &Graph| {
                g.add_vertex(
                    lotr.time,
                    lotr.src_id.clone(),
                    &vec![("type".to_string(), Prop::Str("Character".to_string()))],
                )
                .expect("Failed to add vertex");

                g.add_vertex(
                    lotr.time,
                    lotr.dst_id.clone(),
                    &vec![("type".to_string(), Prop::Str("Character".to_string()))],
                )
                .expect("Failed to add vertex");

                g.add_edge(
                    lotr.time,
                    lotr.src_id.clone(),
                    lotr.dst_id.clone(),
                    &vec![(
                        "type".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                    None,
                )
                .expect("Failed to add edge");
            })
            .expect("Failed to load csv into graph");

        Self {
            parent_graph: Arc::new(graph),
        }
    }
}

impl Default for Metadata<TGraph> {
    fn default() -> Self {
        Self {
            parent_graph: Arc::new(TGraph::new(4)),
        }
    }
}
