use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow::static_graph::StaticGraph;
use parking_lot::RwLock;
use polars_core::prelude::*;
use raphtory::{
    core::{
        entities::{vertices::input_vertex::InputVertex, LayerIds},
        utils::errors::GraphError,
        Direction,
    },
    db::api::{view::internal::GraphOps, mutation::{TryIntoInputTime, CollectProperties}},
    prelude::*,
};

mod arrow;

pub struct PersistentTemporalGraph {
    mem_graph: Graph,
    updates_since_last_fragment: AtomicUsize,

    flush_size: usize,
    graph_fragments: Arc<RwLock<Vec<GraphFragment>>>,
}

impl PersistentTemporalGraph {
    pub fn new(flush_size: usize) -> Self {
        let mem_graph = Graph::new();
        let graph_fragments = Arc::new(RwLock::new(Vec::new()));
        Self {
            mem_graph,
            updates_since_last_fragment: AtomicUsize::new(0),
            flush_size,
            graph_fragments,
        }
    }

    fn flush(&self) {
        // iterate over all the pending updates and add them to the graph

        let edge_triplets = self
            .mem_graph
            .vertex_refs(LayerIds::All, None)
            .flat_map(|v| {
                self.mem_graph
                    .vertex_edges(v, Direction::OUT, LayerIds::All, None)
            })
            .for_each(|edge_ref| println!("{:?}", edge_ref));

        // FIXME: this is terrible
        self.updates_since_last_fragment.store(0, Ordering::SeqCst);
    }

    fn add_edge<
        V: InputVertex,
        T: TryIntoInputTime,
        PI: CollectProperties,
    >(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        if self.updates_since_last_fragment.load(Ordering::SeqCst) >= self.flush_size {
            self.flush();
        }

        self.updates_since_last_fragment
            .fetch_add(1, Ordering::SeqCst);

        self.mem_graph.add_edge(t, src, dst, props, layer)?;
        Ok(())
    }
}

struct GraphFragment {
    static_graph: StaticGraph,
    temporal: Option<TemporalGraph>,
}

impl GraphFragment {
    fn from_graph<G: GraphViewOps>(g: G) -> Self {
        let outgoing = g
            .vertex_refs(LayerIds::All, None)
            .flat_map(|v| g.vertex_edges(v, Direction::OUT, LayerIds::All, None));

        let incoming = g
            .vertex_refs(LayerIds::All, None)
            .flat_map(|v| g.vertex_edges(v, Direction::OUT, LayerIds::All, None));
        let static_graph = StaticGraph::from_sorted_edge_refs(outgoing, incoming);

        Self {
            static_graph,
            temporal: None,
        }
    }
}

struct TemporalGraph {
    vertex_active: DataFrame,
    edge_active: DataFrame,
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn add_edge_to_graph_no_flushing() {
        let g = PersistentTemporalGraph::new(2);

        g.add_edge(0, 1, 2, NO_PROPS, None)
            .expect("failed to add edge");
        g.add_edge(0, 2, 3, NO_PROPS, None)
            .expect("failed to add edge");
        g.add_edge(0, 3, 4, NO_PROPS, None)
            .expect("failed to add edge");
    }
}
