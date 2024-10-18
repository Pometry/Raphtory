use graph::{edges::edge_ref::EdgeStorageRef, nodes::node_ref::NodeStorageRef};
use raphtory_api::core::entities::LayerIds;

use crate::FilterState;

use super::list_ops::{EdgeList, NodeList};

pub mod graph;
pub mod locked;
pub mod storage;

pub trait StorageGraphViewOps:Send + Sync{
    fn layer_ids(&self) -> &LayerIds;
    fn unfiltered_num_layers(&self) -> usize;

    fn node_list_trusted(&self) -> bool;
    fn node_list(&self) -> NodeList;
    fn edge_list(&self) -> EdgeList;
    fn filter_state(&self) -> FilterState;

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool;
    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool;
}
