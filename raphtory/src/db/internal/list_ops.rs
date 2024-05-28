use crate::{
    core::entities::graph::tgraph::InternalGraph,
    db::api::view::internal::{EdgeList, ListOps, NodeList},
};

impl ListOps for InternalGraph {
    fn node_list(&self) -> NodeList {
        NodeList::All {
            num_nodes: self.inner().storage.nodes.len(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        EdgeList::All {
            num_edges: self.inner().storage.edges.len(),
        }
    }
}
