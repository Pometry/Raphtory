use raphtory_memstorage::db::api::{
    list_ops::{EdgeList, NodeList},
    storage::graph::GraphStorage,
};

use crate::db::api::view::internal::ListOps;

impl ListOps for GraphStorage {
    fn node_list(&self) -> NodeList {
        NodeList::All {
            num_nodes: self.internal_num_nodes(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        EdgeList::All {
            num_edges: self.internal_num_edges(),
        }
    }
}
