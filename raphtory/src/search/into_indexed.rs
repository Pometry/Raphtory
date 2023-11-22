use crate::{
    db::{
        api::view::{internal::IntoDynamic, StaticGraphViewOps},
        graph::views::window_graph::WindowedGraph,
    },
    search::IndexedGraph,
};

impl<G: StaticGraphViewOps + IntoDynamic> WindowedGraph<IndexedGraph<G>> {
    pub fn into_dynamic_indexed(self) -> IndexedGraph<DynamicGraph> {
        IndexedGraph {
            graph: self.graph.graph.window(self.start, self.end).into_dynamic(),
            vertex_index: self.graph.vertex_index,
            edge_index: self.graph.edge_index,
            reader: self.graph.reader,
            edge_reader: self.graph.edge_reader,
        }
    }
}
