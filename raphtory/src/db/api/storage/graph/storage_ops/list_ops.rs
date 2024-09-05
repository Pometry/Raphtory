use crate::db::api::view::internal::{EdgeList, ListOps, NodeList};

use super::GraphStorage;

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
