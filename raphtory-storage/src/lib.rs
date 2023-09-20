use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::static_graph::StaticGraph;
use itertools::Itertools;
use parking_lot::RwLock;
use polars_core::prelude::*;
use raphtory::{
    core::{
        entities::{
            edges::edge_ref::EdgeRef, vertices::input_vertex::InputVertex, EID, VID,
        },
        utils::errors::GraphError,
        Direction,
    },
    db::api::mutation::{internal::InternalAdditionOps, CollectProperties, TryIntoInputTime},
    prelude::*,
};

mod arrow;

pub struct PersistentTemporalGraph {
    mem_graph: Arc<[Graph; 2]>,
    cur_graph: RwLock<usize>,

    updates_since_last_fragment: AtomicUsize,

    flush_size: usize,
    graph_fragments: Arc<RwLock<Vec<GraphFragment>>>,
}

impl PersistentTemporalGraph {
    pub fn new(flush_size: usize) -> Self {
        let mem_graph1 = Graph::new();
        let mem_graph2 = Graph::new();

        let mem_graph = Arc::new([mem_graph1, mem_graph2]);
        let graph_fragments = Arc::new(RwLock::new(Vec::new()));

        Self {
            mem_graph,
            cur_graph: RwLock::new(0),
            updates_since_last_fragment: AtomicUsize::new(0),
            flush_size,
            graph_fragments,
        }
    }

    fn swap_cur_graph(&self) {
        if self.updates_since_last_fragment.load(Ordering::Relaxed) >= self.flush_size {
            let mut cur_graph = self.cur_graph.write();
            if self.updates_since_last_fragment.load(Ordering::SeqCst) >= self.flush_size {
                *cur_graph = (*cur_graph + 1) % 2;
            }
            // reset the updates
            self.updates_since_last_fragment.store(0, Ordering::SeqCst);
        }
    }

    fn graph(&self) -> &Graph {
        let cur_graph = self.cur_graph.read();
        &self.mem_graph[*cur_graph]
    }

    fn prev_graph(&self) -> &Graph {
        let cur_graph = self.cur_graph.read();
        &self.mem_graph[(*cur_graph + 1) % 2]
    }

    fn flush(&self) {
        // 1. swap over to a new graph to grab the new updates
        self.swap_cur_graph();
        // 2. create a new graph fragment from the previous graph updates
        let fragment = GraphFragment::from_graph(self.prev_graph());
        // 3. add the new fragment to the list of fragments
        {
            self.graph_fragments.write().push(fragment);
        }
        // 4. clear the previous graph
        // TODO: self.prev_graph().clear();
    }

    pub fn add_edge<V: InputVertex, T: TryIntoInputTime, PI: CollectProperties>(
        &self,
        t: T,
        src: V,
        dst: V,
        props: PI,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.updates_since_last_fragment
            .fetch_add(1, Ordering::Relaxed);

        self.flush();

        self.graph().add_edge(t, src, dst, props, layer)?;
        Ok(())
    }

    pub fn edges<V: InputVertex>(&self, v: V, dir: Direction) -> Box<dyn Iterator<Item = EdgeRef>> {
        let vid = self.graph().resolve_vertex(v.id());
        let fragments = self.graph_fragments.read();
        let iter = fragments
            .iter()
            .map(move |graph_fragment| {
                graph_fragment
                    .edges(vid, dir)
                    .map(move |(other_vid, eid)| match dir {
                        Direction::OUT => EdgeRef::new_outgoing(eid, vid, other_vid),
                        Direction::IN => EdgeRef::new_incoming(eid, other_vid, vid),
                        Direction::BOTH => todo!(),
                    })
            })
            .kmerge_by(|e1, e2| e1.local() < e2.local());
        Box::new(iter)
    }
}

struct GraphFragment {
    static_graph: StaticGraph,
}

impl GraphFragment {
    pub fn from_graph<G: GraphViewOps>(g: &G) -> Self {
        let static_graph = StaticGraph::from_graph(g);
        Self { static_graph }
    }

    fn edges(&self, v_id: VID, dir: Direction) -> Box<dyn Iterator<Item = (VID, EID)>> {
        self.static_graph
            .edges(v_id, dir)
            .map(|iter| {
                let box_iter: Box<dyn Iterator<Item = (VID, EID)>> =
                    Box::new(iter.map(|(uid, eid)| {
                        let vid = uid as usize;
                        let eid = eid as usize;
                        (vid.into(), eid.into())
                    }));
                box_iter
            })
            .unwrap_or(Box::new(std::iter::empty()))
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn add_edge_to_graph_no_flushing() {
        let g = PersistentTemporalGraph::new(2);

        g.add_edge(0, 1, 2, NO_PROPS, None)
            .expect("failed to add edge");
        g.add_edge(0, 1, 4, NO_PROPS, None)
            .expect("failed to add edge");
        g.add_edge(0, 2, 3, NO_PROPS, None)
            .expect("failed to add edge");
        g.add_edge(0, 3, 4, NO_PROPS, None)
            .expect("failed to add edge");

        g.flush();

        let edges = g.edges(1, Direction::OUT);
        assert_eq!(edges.count(), 2);

        let edges = g.edges(1, Direction::IN);
        assert_eq!(edges.count(), 0);

        let edges = g.edges(4, Direction::IN);
        assert_eq!(edges.count(), 2);
    }

}
