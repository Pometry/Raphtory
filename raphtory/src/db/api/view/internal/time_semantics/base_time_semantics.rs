use crate::{
    core::Prop,
    db::api::{
        storage::graph::nodes::node_ref::NodeStorageRef,
        view::internal::time_semantics::{
            event_semantics::EventSemantics, persistent_semantics::PersistentSemantics,
            time_semantics_ops::NodeTimeSemanticsOps,
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::{
    core::storage::timeindex::TimeIndexEntry,
    iter::{BoxedLDIter, BoxedLIter},
};
use std::ops::Range;

pub enum BaseTimeSemantics {
    Persistent(PersistentSemantics),
    Event(EventSemantics),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            BaseTimeSemantics::Persistent($pattern) => $result,
            BaseTimeSemantics::Event($pattern) => $result,
        }
    };
}

impl NodeTimeSemanticsOps for BaseTimeSemantics {
    fn node_earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_earliest_time(node, view))
    }

    fn node_latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_latest_time(node, view))
    }

    fn node_earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_earliest_time_window(node, view, w))
    }

    fn node_latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.node_latest_time_window(node, view, w))
    }

    fn node_history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.node_history(node, view))
    }

    fn node_history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.node_history_window(node, view, w))
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates(node, view))
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates_window(node, view, w))
    }

    fn node_valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid(node, view))
    }

    fn node_valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.node_valid_window(node, view, w))
    }

    fn node_tprop_iter<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_iter(node, view, prop_id))
    }

    fn node_tprop_iter_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        w: Range<i64>,
    ) -> BoxedLDIter<'graph, (TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_iter_window(node, view, prop_id, w))
    }

    fn node_tprop_last_at<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at(node, view, prop_id, t))
    }

    fn node_tprop_last_at_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        prop_id: usize,
        t: TimeIndexEntry,
        w: Range<i64>,
    ) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, semantics => semantics.node_tprop_last_at_window(node, view, prop_id, t, w))
    }
}
