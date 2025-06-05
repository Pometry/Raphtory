use super::GraphStorage;
use crate::db::api::view::internal::{EdgeList, ListOps, NodeList};

impl ListOps for GraphStorage {
    #[inline]
    fn node_list(&self) -> NodeList {
        NodeList::All {
            len: self.unfiltered_num_nodes(),
        }
    }

    #[inline]
    fn edge_list(&self) -> EdgeList {
        EdgeList::All {
            len: self.unfiltered_num_edges(),
        }
    }
}
