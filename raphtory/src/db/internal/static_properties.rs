use crate::{
    core::{
        entities::graph::tgraph::InnerTemporalGraph, storage::locked_view::LockedView, ArcStr, Prop,
    },
    db::api::{properties::internal::ConstPropertiesOps, view::BoxedIter},
};
use parking_lot::RwLockReadGuard;
use std::sync::Arc;

impl<const N: usize> ConstPropertiesOps for InnerTemporalGraph<N> {
    fn const_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr>> {
        Box::new(self.inner().constant_property_names().into_iter())
    }

    fn get_const_property(&self, key: &str) -> Option<Prop> {
        self.inner().get_constant_prop(key)
    }
}
