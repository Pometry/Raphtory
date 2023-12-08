use crate::{
    db::{
        api::view::{
            internal::{DynamicGraph, IntoDynamic, OneHopFilter},
            StaticGraphViewOps,
        },
        graph::views::{
            layer_graph::LayeredGraph, vertex_subgraph::VertexSubgraph, window_graph::WindowedGraph,
        },
    },
    prelude::{GraphViewOps, TimeOps},
    search::IndexedGraph,
};

pub trait DynamicIndexedGraph {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph>;
}
impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for WindowedGraph<IndexedGraph<G>> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self.graph.graph.window(self.start, self.end).into_dynamic(),
            vertex_index: self.graph.vertex_index,
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
            vertex_index: self.graph.vertex_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}

impl<G: StaticGraphViewOps + IntoDynamic> DynamicIndexedGraph for VertexSubgraph<IndexedGraph<G>> {
    fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        let g = self.graph.graph.subgraph(self.vertices());
        IndexedGraph {
            graph: g.into_dynamic(),
            vertex_index: self.graph.vertex_index,
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
            vertex_index: self.vertex_index,
            edge_index: self.edge_index,
            reader: self.reader,
            edge_reader: self.edge_reader,
        }
    }
}
