use raphtory_core::entities::LayerIds;

use super::GraphStorage;
use crate::db::api::view::internal::{EdgeList, ListOps, NodeList};

impl ListOps for GraphStorage {
    #[inline]
    fn node_list(&self) -> NodeList {
        NodeList::All {
            layers: LayerIds::All,
        }
    }

    #[inline]
    fn edge_list(&self) -> EdgeList {
        EdgeList::All {
            layers: LayerIds::All,
        }
    }
}
