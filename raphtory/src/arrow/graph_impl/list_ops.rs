use crate::{
    arrow::graph_impl::ArrowGraph,
    db::api::view::internal::{CoreGraphOps, EdgeList, ListOps, NodeList},
};
use rayon::prelude::*;

impl ListOps for ArrowGraph {
    fn node_list(&self) -> NodeList {
        NodeList::All {
            num_nodes: self.unfiltered_num_nodes(),
        }
    }

    fn edge_list(&self) -> EdgeList {
        let count = self
            .inner
            .layers()
            .par_iter()
            .map(|layer| layer.num_edges())
            .sum();
        EdgeList::All { num_edges: count }
    }
}
