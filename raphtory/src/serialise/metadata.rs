use crate::{
    db::api::view::internal::GraphView,
    prelude::{GraphViewOps, PropertiesOps},
    serialise::GraphFolder,
};
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::arc_str::ArcStr},
    GraphType,
};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Debug)]
pub struct GraphMetadata {
    pub node_count: usize,
    pub edge_count: usize,
    pub metadata: Vec<(ArcStr, Prop)>,
    pub graph_type: GraphType,
}

impl GraphMetadata {
    pub fn from_graph<G: GraphView>(graph: G) -> Self {
        let node_count = graph.count_nodes();
        let edge_count = graph.count_edges();
        let metadata = graph.metadata().as_vec();
        let graph_type = graph.graph_type();
        Self {
            node_count,
            edge_count,
            metadata,
            graph_type,
        }
    }
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.metadata, graph.properties().as_vec());
    assert_eq!(metadata.graph_type, graph.graph_type());
}
