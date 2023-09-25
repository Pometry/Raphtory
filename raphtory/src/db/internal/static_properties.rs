use crate::{
    core::{entities::graph::tgraph::InnerTemporalGraph, ArcStr, Prop},
    db::api::properties::internal::ConstPropertiesOps,
};

impl<const N: usize> ConstPropertiesOps for InnerTemporalGraph<N> {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.inner().graph_props.get_const_prop_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.inner().graph_props.get_const_prop_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize>> {
        Box::new(self.inner().graph_props.const_prop_ids())
    }

    fn const_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr>> {
        Box::new(self.inner().const_prop_names().into_iter())
    }

    fn get_const_prop(&self, prop_id: usize) -> Option<Prop> {
        self.inner().get_constant_prop(prop_id)
    }
}
