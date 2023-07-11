use crate::core::entities::graph::tgraph::InnerTemporalGraph;
use crate::core::Prop;
use crate::db::api::properties::internal::{Key, TemporalPropertiesOps, TemporalPropertyViewOps};

impl<const N: usize> TemporalPropertyViewOps for InnerTemporalGraph<N> {
    fn temporal_value(&self, id: &Key) -> Option<Prop> {
        self.get_temporal_prop(id)
            .and_then(|prop| prop.last_before(i64::MAX))
    }

    fn temporal_history(&self, id: &Key) -> Vec<i64> {
        self.get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_values(&self, id: &Key) -> Vec<Prop> {
        self.get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop> {
        self.get_temporal_prop(id)
            .and_then(|prop| prop.last_before(t.saturating_add(1)))
    }
}

impl<const N: usize> TemporalPropertiesOps for InnerTemporalGraph<N> {
    fn temporal_property_keys(&self) -> Vec<String> {
        (*self.temporal_property_names()).clone()
    }

    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = Key> + '_> {
        Box::new(self.temporal_property_keys().into_iter()) // Fixme: rework to use internal ids
    }

    fn get_temporal_property(&self, key: &str) -> Option<Key> {
        self.get_temporal_prop(key).map(|_| key.to_owned()) // Fixme: rework to use internal ids
    }
}
