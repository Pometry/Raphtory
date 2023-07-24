use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, storage::locked_view::LockedView, Prop},
    db::api::{properties::internal::StaticPropertiesOps, view::internal::CoreGraphOps},
};
use parking_lot::RwLockReadGuard;

impl<const N: usize> StaticPropertiesOps for InnerTemporalGraph<N> {
    fn static_property_keys<'a>(&'a self) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let guarded = self.static_property_names();
        Box::new((0..guarded.len()).map(move |i| {
            RwLockReadGuard::map(RwLockReadGuard::rwlock(&guarded).read(), |v| &v[i]).into()
        }))
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.static_prop(key)
    }
}
