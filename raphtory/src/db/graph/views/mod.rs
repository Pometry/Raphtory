use crate::{
    db::{
        api::{
            storage::graph::storage_ops::GraphStorage,
            view::internal::{CoreGraphOps, ListOps},
        },
        graph::views::{
            cached_view::CachedView,
            deletion_graph::PersistentGraph,
            filter::{
                edge_property_filtered_graph::EdgePropertyFilteredGraph,
                exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph,
            },
            layer_graph::LayeredGraph,
            node_subgraph::NodeSubgraph,
            node_type_filtered_subgraph::TypeFilteredSubgraph,
            window_graph::WindowedGraph,
        },
    },
    prelude::Graph,
};
use enum_dispatch::enum_dispatch;

pub mod cached_view;
pub mod deletion_graph;
pub mod filter;
pub mod layer_graph;
pub mod node_subgraph;
pub mod node_type_filtered_subgraph;
pub mod window_graph;
//
// #[enum_dispatch(InternalLayerOps)]
// #[enum_dispatch(ListOps)]
// #[enum_dispatch(TimeSemantics)]
// #[enum_dispatch(EdgeFilterOps)]
// #[enum_dispatch(NodeFilterOps)]
// #[enum_dispatch(InternalMaterialize)]
// #[enum_dispatch(TemporalPropertiesOps)]
// #[enum_dispatch(TemporalPropertyViewOps)]
// #[enum_dispatch(ConstPropertiesOps)]
// #[enum_dispatch(InternalAdditionOps)]
// #[enum_dispatch(InternalPropertyAdditionOps)]
// pub enum GraphViewEnum<G> {
//     EventGraph(Graph),
//     PersistentGraph(PersistentGraph),
//     CacheGraphView(CachedView<G>),
//     LayeredGraph(LayeredGraph<G>),
//     NodeSubgraph(NodeSubgraph<G>),
//     NodeTypeFilteredSubgraph(TypeFilteredSubgraph<G>),
//     WindowedGraph(WindowedGraph<G>),
//     NodePropertyFilteredGraph(NodePropertyFilteredGraph<G>),
//     EdgePropertyFilteredGraph(EdgePropertyFilteredGraph<G>),
//     ExplodedEdgePropertyFilteredGraph(ExplodedEdgePropertyFilteredGraph<G>),
// }
//
// impl<G: CoreGraphOps> CoreGraphOps for GraphViewEnum<G> {
//     fn core_graph(&self) -> &GraphStorage {
//         match self {
//             GraphViewEnum::EventGraph(graph) => graph.core_graph(),
//             GraphViewEnum::PersistentGraph(graph) => graph.core_graph(),
//             GraphViewEnum::CacheGraphView(graph) => graph.core_graph(),
//             GraphViewEnum::LayeredGraph(graph) => graph.core_graph(),
//             GraphViewEnum::NodeSubgraph(graph) => graph.core_graph(),
//             GraphViewEnum::NodeTypeFilteredSubgraph(graph) => graph.core_graph(),
//             GraphViewEnum::WindowedGraph(graph) => graph.core_graph(),
//             GraphViewEnum::NodePropertyFilteredGraph(graph) => graph.core_graph(),
//             GraphViewEnum::EdgePropertyFilteredGraph(graph) => graph.core_graph(),
//             GraphViewEnum::ExplodedEdgePropertyFilteredGraph(graph) => graph.core_graph(),
//         }
//     }
// }
