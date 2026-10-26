use crate::{
    algorithms::components::connected_components::weakly_connected_components,
    db::{
        api::view::{GraphViewOps, StaticGraphViewOps},
        graph::views::node_subgraph::NodeSubgraph,
    },
    prelude::{Graph, NodeStateGroupBy},
};
use raphtory_api::core::entities::VID;
use raphtory_storage::core_ops::CoreGraphOps;

/// Gives the large connected component of a graph.
/// The large connected component is the largest (i.e., with the highest number of nodes)
/// connected sub-graph of the network.
///
/// # Example Usage:
///
/// g.largest_connected_component()
///
/// # Returns:
///
/// A raphtory graph, which essentially is a sub-graph of the graph `g`
///
pub trait LargestConnectedComponent {
    fn largest_connected_component(&self) -> NodeSubgraph<Self>
    where
        Self: StaticGraphViewOps;
}

impl LargestConnectedComponent for Graph {
    fn largest_connected_component(&self) -> NodeSubgraph<Self>
    where
        Self: StaticGraphViewOps,
    {
        let connected_components = weakly_connected_components(self).groups();

        let lcc = connected_components
            .into_iter_groups()
            .map(|(_, subgraph)| subgraph)
            .max_by(|l, r| l.len().cmp(&r.len()))
            .map(|nodes| NodeSubgraph {
                graph: self.clone(),
                nodes: nodes.nodes.into_index(self.core_graph()),
            });

        lcc.unwrap_or(self.subgraph(Vec::<VID>::new()))
    }
}
