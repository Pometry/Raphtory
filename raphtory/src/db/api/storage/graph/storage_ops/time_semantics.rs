use super::GraphStorage;
use crate::{
    core::storage::timeindex::TimeIndexOps,
    db::api::view::internal::{GraphTimeSemanticsOps, TimeSemantics},
    prelude::Prop,
};
use raphtory_api::{
    core::{
        entities::{properties::tprop::TPropOps, LayerIds},
        storage::timeindex::EventTime,
    },
    iter::{BoxedLIter, IntoDynBoxed},
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::graph::{locked::LockedGraph, nodes::node_storage_ops::NodeStorageOps};
use rayon::iter::ParallelIterator;
use std::ops::Range;
use storage::{
    api::graph_props::{GraphPropEntryOps, GraphPropRefOps},
    generic_time_ops::ALL_LAYERS,
};

impl GraphTimeSemanticsOps for GraphStorage {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::event()
    }

    #[inline]
    fn window_filtered(&self) -> bool {
        false
    }

    fn view_start(&self) -> Option<EventTime> {
        None
    }

    fn view_end(&self) -> Option<EventTime> {
        None
    }

    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.graph_earliest_time()
            }
        }
    }

    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        match self {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.graph_latest_time()
            }
        }
    }

    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map_iter(|node| {
                node.node_prop_additions(&LayerIds::All)
                    .range(start..end)
                    .first_t()
                    .into_iter()
                    .chain(
                        node.node_edge_additions(ALL_LAYERS.clone())
                            .range(start..end)
                            .first_t(),
                    )
            })
            .min()
    }

    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.nodes()
            .par_iter()
            .flat_map_iter(|node| {
                node.node_prop_additions(ALL_LAYERS.clone())
                    .range(start..end)
                    .last_t()
                    .into_iter()
                    .chain(
                        node.node_edge_additions(ALL_LAYERS.clone())
                            .range(start..end)
                            .last_t(),
                    )
            })
            .max()
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph_props_meta()
            .temporal_prop_mapper()
            .has_id(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter()
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        let graph_entry = self.graph_entry();

        graph_entry.as_ref().get_temporal_prop(prop_id).active(w)
    }

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter_window(start..end)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .iter_window_rev(start..end)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        let graph_entry = self.graph_entry();

        graph_entry
            .as_ref()
            .get_temporal_prop(prop_id)
            .last_before(t.next())
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        if w.contains(&t) {
            let graph_entry = self.graph_entry();

            graph_entry
                .as_ref()
                .get_temporal_prop(prop_id)
                .last_before(t.next())
                .filter(|(prop_time, _)| w.contains(prop_time))
        } else {
            None
        }
    }
}
