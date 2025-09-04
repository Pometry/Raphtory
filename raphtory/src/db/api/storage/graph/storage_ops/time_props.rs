use std::ops::Deref;

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

impl InternalTemporalPropertyViewOps for GraphStorage {
    fn dtype(&self, id: usize) -> PropType {
        self.graph_meta().get_temporal_dtype(id).unwrap()
    }

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(id)
            .into_iter()
            .flat_map(|prop| GenLockedIter::from(prop, |prop| prop.deref().iter().into_dyn_boxed()))
            .into_dyn_boxed()
    }

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph_meta()
            .get_temporal_prop(id)
            .into_iter()
            .flat_map(|prop| {
                GenLockedIter::from(prop, |prop| prop.deref().iter().rev().into_dyn_boxed())
            })
            .into_dyn_boxed()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(TimeIndexEntry::MAX)
                .map(|(_, v)| v)
        })
    }

    fn temporal_value_at(&self, id: usize, t: TimeIndexEntry) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(t.next())
                .map(|(_, v)| v)
        })
    }
}

impl InternalTemporalPropertiesOps for GraphStorage {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().get_temporal_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph_meta().get_temporal_name(id)
    }

    fn temporal_prop_ids(&self) -> BoxedLIter<usize> {
        Box::new(self.graph_meta().temporal_ids())
    }

    fn temporal_prop_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.graph_meta().temporal_names().into_iter())
    }
}
