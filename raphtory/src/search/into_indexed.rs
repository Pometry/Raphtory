use crate::{
    db::{
        api::{
            properties::{
                dyn_props::{DynProperties, DynProps},
                Properties,
            },
            view::{
                internal::{DynamicGraph, IntoDynamic, OneHopFilter},
                time::internal::InternalTimeOps,
                StaticGraphViewOps,
            },
        },
        graph::views::{
            layer_graph::LayeredGraph, node_subgraph::NodeSubgraph, window_graph::WindowedGraph,
        },
    },
    prelude::GraphViewOps,
    search::IndexedGraph,
};
use std::sync::Arc;

pub trait DynamicIndexedGraph {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph>;
}
impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for WindowedGraph<IndexedGraph<G>> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self
                .graph
                .graph
                .internal_window(self.start, self.end)
                .into_dynamic(),
            node_index: self.graph.node_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for LayeredGraph<IndexedGraph<G>> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        let l = self.one_hop_filtered(LayeredGraph::new(
            self.graph.graph.clone(),
            self.layers.clone(),
        ));
        IndexedGraph {
            graph: l.into_dynamic(),
            node_index: self.graph.node_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for NodeSubgraph<IndexedGraph<G>> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        let g = self.graph.graph.subgraph(self.nodes());
        IndexedGraph {
            graph: g.into_dynamic(),
            node_index: self.graph.node_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for IndexedGraph<G> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self.graph.into_dynamic(),
            node_index: self.node_index,
            edge_index: self.edge_index,
            reader: self.reader,
            edge_reader: self.edge_reader,
        }
    }
}

impl From<Properties<IndexedGraph<DynamicGraph>>> for DynProperties {
    fn from(value: Properties<IndexedGraph<DynamicGraph>>) -> Self {
        let props: DynProps = Arc::new(value.props);
        Properties::new(props)
    }
}
