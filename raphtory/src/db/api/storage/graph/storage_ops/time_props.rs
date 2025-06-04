use super::GraphStorage;
use crate::{
    core::{
        utils::{errors::GraphError, iter::GenLockedIter},
        PropType
    },
    db::api::{
        properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        storage::graph::tprop_storage_ops::TPropOps,
        view::BoxedLIter,
    },
    prelude::Prop,
};
use chrono::{DateTime, Utc};
use raphtory_api::core::storage::{
    arc_str::ArcStr,
    timeindex::{AsTime, TimeIndexEntry},
};
use std::ops::Deref;

impl TemporalPropertyViewOps for GraphStorage {
    fn dtype(&self, id: usize) -> PropType {
        self.graph_meta().get_temporal_dtype(id).unwrap()
    }
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph_meta()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<i64> {
        Box::new(
            self.graph_meta()
                .get_temporal_prop(id)
                .into_iter()
                .flat_map(|prop| {
                    GenLockedIter::from(prop, |prop| Box::new(prop.iter_t().map(|(t, _)| t)))
                }),
        )
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph_meta()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(TimeIndexEntry::MAX)
                .map(|(_, v)| v)
        })
    }

    fn temporal_history_date_time(
        &self,
        id: usize,
    ) -> Result<Vec<DateTime<Utc>>, GraphError> {
        match self.graph_meta().get_temporal_prop(id) {
            Some(tprop) => tprop
                .iter_t()
                .map(|(t, _)| t.dt().map_err(GraphError::from))
                .collect::<Result<Vec<_>, GraphError>>(),
            None => Ok(Vec::new())
        }
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
