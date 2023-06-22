use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::TimeIndexOps;
use crate::core::vertex_ref::LocalVertexRef;
use crate::core::{Direction, Prop};
use crate::db::graph::InternalGraph;
use crate::db::mutation_api::internal::InheritMutationOps;
use crate::db::view_api::internal::{
    CoreDeletionOps, CoreGraphOps, GraphOps, InheritCoreDeletionOps, InheritCoreOps,
    InheritGraphOps, Inheritable, TimeSemantics,
};
use crate::db::view_api::BoxedIter;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone)]
pub struct GraphWithDeletions {
    graph: Arc<InternalGraph>,
}

impl GraphWithDeletions {
    fn edge_alive_at(&self, e: EdgeRef, t: i64) -> bool {
        // FIXME: assumes additions are before deletions if at the same timestamp (need to have strict ordering/secondary index)
        let additions = self.edge_additions(e);
        let deletions = self.edge_deletions(e);

        let last_addition_before_start = additions.range(i64::MIN..t + 1).last();
        let last_deletion_before_start = deletions.range(i64::MIN..t).last();

        // None is less than any value (see test below)
        last_addition_before_start > last_deletion_before_start
    }

    pub fn new(nr_shards: usize) -> Self {
        Self {
            graph: Arc::new(InternalGraph::new(nr_shards)),
        }
    }
}

impl Inheritable for GraphWithDeletions {
    type Base = InternalGraph;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl InheritMutationOps for GraphWithDeletions {}

impl InheritCoreOps for GraphWithDeletions {}

impl InheritCoreDeletionOps for GraphWithDeletions {}

impl InheritGraphOps for GraphWithDeletions {}

impl TimeSemantics for GraphWithDeletions {
    fn vertex_earliest_time(&self, v: LocalVertexRef) -> Option<i64> {
        self.graph.vertex_earliest_time(v)
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_global()
    }

    fn include_vertex_window(&self, v: LocalVertexRef, w: Range<i64>) -> bool {
        self.vertex_edges(v, Direction::BOTH, None)
            .any(move |e| self.include_edge_window(e, w.clone()))
    }

    fn include_edge_window(&self, e: EdgeRef, w: Range<i64>) -> bool {
        // includes edge if it is alive at the start of the window or added during the window
        self.edge_alive_at(e, w.start) || self.edge_additions(e).active(w)
    }

    fn vertex_history(&self, v: LocalVertexRef) -> Vec<i64> {
        self.graph.vertex_history(v)
    }

    fn vertex_history_window(&self, v: LocalVertexRef, w: Range<i64>) -> Vec<i64> {
        self.graph.vertex_history_window(v, w)
    }

    fn edge_t(&self, e: EdgeRef) -> BoxedIter<EdgeRef> {
        //Fixme: Need support for duration on exploded edges
        self.graph.edge_t(e)
    }

    fn edge_window_t(&self, e: EdgeRef, w: Range<i64>) -> BoxedIter<EdgeRef> {
        // FIXME: Need better iterators on LockedView that capture the guard
        let additions = self.edge_additions(e);
        let collected: Vec<EdgeRef> = if self.edge_alive_at(e, w.start) {
            iter::once(w.start)
                .chain(
                    additions
                        .range(w.start.saturating_add(1)..w.end)
                        .iter()
                        .copied(),
                )
                .map(|t| e.at(t))
                .collect()
        } else {
            additions.range(w).iter().map(|t| e.at(*t)).collect()
        };
        Box::new(collected.into_iter())
    }

    fn edge_earliest_time(&self, e: EdgeRef) -> Option<i64> {
        self.graph.edge_earliest_time(e)
    }

    fn edge_earliest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        if self.edge_alive_at(e, w.start) {
            Some(w.start)
        } else {
            self.edge_additions(e).range(w).first()
        }
    }

    fn edge_latest_time(&self, e: EdgeRef) -> Option<i64> {
        if self.edge_alive_at(e, i64::MAX) {
            Some(i64::MAX)
        } else {
            self.edge_deletions(e).last()
        }
    }

    fn edge_latest_time_window(&self, e: EdgeRef, w: Range<i64>) -> Option<i64> {
        if self.edge_alive_at(e, w.end) {
            Some(w.end)
        } else {
            self.edge_deletions(e).range(w).last()
        }
    }

    fn edge_deletion_history(&self, e: EdgeRef) -> Vec<i64> {
        self.edge_deletions(e).iter().copied().collect()
    }

    fn edge_deletion_history_window(&self, e: EdgeRef, w: Range<i64>) -> Vec<i64> {
        self.edge_deletions(e).range(w).iter().copied().collect()
    }

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(name)
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_prop_vec(&self, v: LocalVertexRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: LocalVertexRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        let prop = self.temporal_edge_prop(e, name);
        match prop {
            Some(p) => {
                if self.edge_alive_at(e, t_start) {
                    p.last_before(t_start.saturating_add(1))
                        .into_iter()
                        .map(|v| (t_start, v))
                        .chain(p.iter_window(t_start.saturating_add(1)..t_end))
                        .collect()
                } else {
                    p.iter_window(t_start..t_end).collect()
                }
            }
            None => Default::default(),
        }
    }

    fn temporal_edge_prop_vec(&self, e: EdgeRef, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec(e, name)
    }
}

#[cfg(test)]
mod test_deletions {
    use crate::core::{Prop, PropUnwrap};
    use crate::db::graph::Graph;
    use crate::db::graph_deletions::GraphWithDeletions;
    use crate::db::mutation_api::{AdditionOps, DeletionOps};
    use crate::db::view_api::*;

    #[test]
    fn option_partial_ord() {
        // None is less than any value
        assert!(Some(1) > None);
        assert!(!(None::<i64> > None));
    }

    #[test]
    fn test_edge_deletions() {
        let g = GraphWithDeletions::new(1);

        g.add_edge(0, 0, 1, [("added".to_string(), Prop::I64(0))], None)
            .unwrap();
        g.delete_edge(10, 0, 1, None).unwrap();

        assert_eq!(g.edges().id().collect::<Vec<_>>(), vec![(0, 1)]);

        assert_eq!(
            g.window(1, 2).edges().id().collect::<Vec<_>>(),
            vec![(0, 1)]
        );

        assert!(g.window(11, 12).is_empty());

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1, None)
                .unwrap()
                .property("added", true)
                .unwrap_i64(),
            0
        );

        assert!(g.window(11, 12).edge(0, 1, None).is_none());

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1, None)
                .unwrap()
                .property_history("added"),
            vec![(1, Prop::I64(0))]
        );

        assert_eq!(g.window(1, 2).vertex(0).unwrap().out_degree(), 1)
    }

    #[test]
    fn test_materialize_only_deletion() {
        let g = Graph::new(1);
        g.delete_edge(1, 1, 2, None).unwrap();

        assert_eq!(g.materialize().unwrap(), g);
    }
}
