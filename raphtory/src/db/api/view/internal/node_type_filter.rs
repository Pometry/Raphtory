use crate::{db::graph::nodes::Nodes, prelude::GraphViewOps};

pub trait NodeTypeFilter<'graph, G, GH> {
    fn node_type_filter(&self, node_types: &[impl AsRef<str>]) -> Nodes<'graph, G, GH>;
}
