use std::ops::Deref;

use raphtory_api::core::storage::{arc_str::ArcStr, timeindex::AsTime};

use crate::{
    db::api::{
        properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        storage::tprop_storage_ops::TPropOps,
    },
    prelude::Prop,
};

use super::GraphStorage;

impl TemporalPropertyViewOps for GraphStorage {
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph_meta()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph_meta()
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.graph_meta()
            .get_temporal_prop(id)
            .and_then(|prop| prop.deref().last_before(i64::MAX).map(|(_, v)| v))
    }

    fn temporal_history_date_time(&self, id: usize) -> Option<Vec<chrono::DateTime<chrono::Utc>>> {
        self.graph_meta()
            .get_temporal_prop(id)
            .and_then(|prop| prop.iter_t().map(|(t, _)| t.dt()).collect())
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.graph_meta().get_temporal_prop(id).and_then(|prop| {
            prop.deref()
                .last_before(t.saturating_add(1))
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
