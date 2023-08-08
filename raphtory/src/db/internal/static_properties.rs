use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, storage::locked_view::LockedView, Prop},
    db::api::properties::internal::ConstPropertiesOps,
};
use parking_lot::RwLockReadGuard;

impl<const N: usize> ConstPropertiesOps for InnerTemporalGraph<N> {
    fn const_property_keys<'a>(&'a self) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        let guarded = self.inner().static_property_names();
        Box::new((0..guarded.len()).map(move |i| {
            RwLockReadGuard::map(RwLockReadGuard::rwlock(&guarded).read(), |v| &v[i]).into()
        }))
    }

    fn get_const_property(&self, key: &str) -> Option<Prop> {
        self.inner().get_static_prop(key)
    }
}
