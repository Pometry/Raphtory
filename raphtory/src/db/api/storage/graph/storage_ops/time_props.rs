use super::GraphStorage;
use crate::{
    core::utils::iter::GenLockedIter,
    db::api::{
        properties::internal::{InternalTemporalPropertiesOps, InternalTemporalPropertyViewOps},
        view::BoxedLIter,
    },
    prelude::Prop,
};
use raphtory_api::{
    core::{
        entities::properties::{prop::PropType, tprop::TPropOps},
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
    },
    iter::IntoDynBoxed,
};
use storage::api::graph_props::{GraphPropEntryOps, GraphPropRefOps};

impl InternalTemporalPropertyViewOps for GraphStorage {
    fn dtype(&self, id: usize) -> PropType {
        self.graph_meta()
            .temporal_prop_mapper()
            .get_dtype(id)
            .unwrap()
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        let graph_entry = self.graph_entry();

        // Return a boxed iterator of temporal props over the locked graph entry.
        let iter = GenLockedIter::from(graph_entry, |entry| {
            entry
                .as_ref()
                .get_temporal_prop(id)
                .iter()
                .into_dyn_boxed()
        });

        iter.into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<'_, (TimeIndexEntry, Prop)> {
        let graph_entry = self.graph_entry();

        // Return a boxed iterator of temporal props in reverse order over
        // the locked graph entry.
        let iter = GenLockedIter::from(graph_entry, |entry| {
            entry
                .as_ref()
                .get_temporal_prop(id)
                .iter_inner_rev(None)
                .into_dyn_boxed()
        });

        iter.into_dyn_boxed()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        let graph_entry = self.graph_entry();

        graph_entry
            .as_ref()
            .get_temporal_prop(id)
            .last_before(TimeIndexEntry::MAX)
            .map(|(_, prop)| prop)
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        let graph_entry = self.graph_entry();

        graph_entry
            .as_ref()
            .get_temporal_prop(id)
            .at(&TimeIndexEntry::start(t))
    }
}

impl InternalTemporalPropertiesOps for GraphStorage {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().temporal_prop_mapper().get_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph_meta().temporal_prop_mapper().get_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<'_, usize> {
        self.graph_meta()
            .temporal_prop_mapper()
            .ids()
            .into_dyn_boxed()
    }

    fn temporal_prop_keys(&self) -> BoxedLIter<'_, ArcStr> {
        self.graph_meta()
            .temporal_prop_mapper()
            .keys()
            .into_iter()
            .into_dyn_boxed()
    }
}
