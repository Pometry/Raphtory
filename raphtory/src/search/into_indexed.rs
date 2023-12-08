use crate::{
    db::{
        api::view::{
            internal::{DynamicGraph, IntoDynamic, OneHopFilter},
            StaticGraphViewOps,
        },
        graph::views::{
            layer_graph::LayeredGraph, node_subgraph::NodeSubgraph, window_graph::WindowedGraph,
        },
    },
    prelude::{GraphViewOps, TimeOps},
    search::IndexedGraph,
};

impl<G: StaticGraphViewOps + IntoDynamic> WindowedGraph<IndexedGraph<G>> {
    pub fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self.graph.graph.window(self.start, self.end).into_dynamic(),
            node_index: self.graph.node_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> LayeredGraph<IndexedGraph<G>> {
    pub fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
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

impl<G: StaticGraphViewOps + IntoDynamic> NodeSubgraph<IndexedGraph<G>> {
    pub fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
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
