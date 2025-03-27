use crate::{
    core::Prop,
    db::api::{
        storage::graph::nodes::node_ref::NodeStorageRef,
        view::internal::time_semantics::{
            base_time_semantics::BaseTimeSemantics, time_semantics_ops::NodeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::storage::timeindex::TimeIndexEntry,
    iter::{BoxedLDIter, BoxedLIter},
};
use std::ops::Range;

pub struct WindowTimeSemantics {
    pub(super) semantics: BaseTimeSemantics,
    pub(super) window: Range<i64>,
}

impl WindowTimeSemantics {
    pub fn window(self, w: Range<i64>) -> Self {
        let start = self.window.start.max(w.start);
        let end = self.window.end.min(w.end).max(start);
        WindowTimeSemantics {
            window: start..end,
            ..self
        }
    }
}

impl NodeTimeSemanticsOps for WindowTimeSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_earliest_time_window(node, view, self.window.clone())
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .node_latest_time_window(node, view, self.window.clone())
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_earliest_time_window(node, view, w)
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.node_latest_time_window(node, view, w)
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics
            .node_history_window(node, view, self.window.clone())
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics.node_history_window(node, view, w)
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics
            .node_updates_window(node, view, self.window.clone())
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics.node_updates_window(node, view, w)
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics
            .node_valid_window(node, view, self.window.clone())
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.node_valid_window(node, view, w)
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, self.window.clone())
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_iter_window(node, view, prop_id, w)
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_last_at_window(node, view, prop_id, t, self.window.clone())
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        self.semantics
            .node_tprop_last_at_window(node, view, prop_id, t, w)
    }
}
