use crate::{
    core::{entities::graph::tgraph::InternalGraph, storage::timeindex::AsTime, Prop},
    db::api::{
        properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        storage::tprop_storage_ops::TPropOps,
    },
};
use chrono::{DateTime, Utc};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::ops::Deref;

impl TemporalPropertyViewOps for InternalGraph {
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.deref().last_before(i64::MAX).map(|(_, v)| v))
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<DateTime<Utc>>> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.iter_t().map(|(t, _)| t.dt()).collect())
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.inner().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(t.saturating_add(1))
                .map(|(_, v)| v)
        })
    }
}

impl TemporalPropertiesOps for InternalGraph {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.inner().graph_meta.get_temporal_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.inner().graph_meta.get_temporal_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.inner().graph_meta.temporal_ids())
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(self.inner().graph_meta.temporal_names().into_iter())
    }
}
