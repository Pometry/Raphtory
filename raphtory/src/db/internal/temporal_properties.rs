use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, storage::locked_view::LockedView, Prop},
    db::api::properties::internal::{Key, TemporalPropertiesOps, TemporalPropertyViewOps},
};
use parking_lot::RwLockReadGuard;

impl<const N: usize> TemporalPropertyViewOps for InnerTemporalGraph<N> {
    fn temporal_value(&self, id: &Key) -> Option<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.last_before(i64::MAX).map(|(_, v)| v))
    }

    fn temporal_history(&self, id: &Key) -> Vec<i64> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_values(&self, id: &Key) -> Vec<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .map(|prop| prop.iter().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }

    fn temporal_value_at(&self, id: &Key, t: i64) -> Option<Prop> {
        self.inner()
            .get_temporal_prop(id)
            .and_then(|prop| prop.last_before(t.saturating_add(1)).map(|(_, v)| v))
    }
}

impl<const N: usize> TemporalPropertiesOps for InnerTemporalGraph<N> {
    fn temporal_property_keys<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        // TODO: Is this actually worth doing? the advantage is that there is definitely no writes during the iteration as we keep the guard alive...
        let guarded = self.inner().temporal_property_names();
        Box::new((0..guarded.len()).map(move |i| {
            RwLockReadGuard::map(RwLockReadGuard::rwlock(&guarded).read_recursive(), |v| {
                &v[i]
            })
            .into()
        }))
    }

    fn get_temporal_property(&self, key: &str) -> Option<Key> {
        self.inner().get_temporal_prop(key).map(|_| key.to_owned()) // Fixme: rework to use internal ids
    }
}
