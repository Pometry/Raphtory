use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, ArcStr, Prop},
    db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
};

impl<const N: usize> TemporalPropertyViewOps for InnerTemporalGraph<N> {
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.last_before(i64::MAX).map(|(_, v)| v))
    }

    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.last_before(t.saturating_add(1)).map(|(_, v)| v))
    }
}

impl<const N: usize> TemporalPropertiesOps for InnerTemporalGraph<N> {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.inner().graph_props.get_temporal_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.inner().graph_props.get_temporal_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.inner().graph_props.temporal_ids())
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(self.inner().graph_props.temporal_names().into_iter())
    }
}
