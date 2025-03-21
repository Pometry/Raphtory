use std::ops::Deref;

use super::GraphStorage;
use crate::{
    core::{utils::iter::GenLockedIter, PropType},
    db::api::{
        properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        storage::graph::tprop_storage_ops::TPropOps,
        view::BoxedLIter,
    },
    prelude::Prop,
};
use raphtory_api::{
    core::storage::{
        arc_str::ArcStr,
        timeindex::{AsTime, TimeIndexEntry},
    },
    iter::IntoDynBoxed,
};

impl TemporalPropertyViewOps for GraphStorage {
    fn dtype(&self, id: usize) -> PropType {
        self.graph_meta().get_temporal_dtype(id).unwrap()
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(id)
            .into_iter()
            .flat_map(|prop| GenLockedIter::from(prop, |prop| prop.iter().into_dyn_boxed()))
            .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(id)
            .into_iter()
            .flat_map(|prop| GenLockedIter::from(prop, |prop| prop.iter().rev().into_dyn_boxed()))
            .into_dyn_boxed()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(TimeIndexEntry::MAX)
                .map(|(_, v)| v)
        })
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(TimeIndexEntry::start(t.saturating_add(1)))
                .map(|(_, v)| v)
        })
    }
}

impl TemporalPropertiesOps for GraphStorage {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().get_temporal_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph_meta().get_temporal_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.graph_meta().temporal_ids())
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(self.graph_meta().temporal_names().into_iter())
    }
}
