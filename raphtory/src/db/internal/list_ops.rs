use crate::{
    core::entities::graph::tgraph::InnerTemporalGraph,
    db::api::view::internal::{EdgeList, ListOps, NodeList},
};

impl<const N: usize> ListOps for InnerTemporalGraph<N> {
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
