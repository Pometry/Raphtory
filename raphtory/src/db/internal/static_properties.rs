use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, ArcStr, Prop},
    db::api::properties::internal::ConstPropertiesOps,
};

impl<const N: usize> ConstPropertiesOps for InnerTemporalGraph<N> {
    fn const_property_keys(&self) -> Box<dyn Iterator<Item = ArcStr>> {
        Box::new(self.inner().constant_property_names().into_iter())
    }

    fn get_const_property(&self, key: &str) -> Option<Prop> {
        self.inner().get_constant_prop(key)
    }
}
