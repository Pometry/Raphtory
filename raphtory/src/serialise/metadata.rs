use crate::core::Prop;
use crate::prelude::GraphViewOps;
use crate::serialise::GraphFolder;
use raphtory_api::core::storage::arc_str::ArcStr;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct GraphMetadata {
    pub node_count: usize,
    pub edge_count: usize,
    pub properties: Vec<(ArcStr, Prop)>,
}

pub fn assert_metadata_correct<'graph>(folder: &GraphFolder, graph: &impl GraphViewOps<'graph>) {
    let metadata = folder.read_metadata().unwrap();
    assert_eq!(metadata.node_count, graph.count_nodes());
    assert_eq!(metadata.edge_count, graph.count_edges());
    assert_eq!(metadata.properties, graph.properties().as_vec());
}
