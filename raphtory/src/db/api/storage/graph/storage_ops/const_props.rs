use raphtory_api::core::storage::arc_str::ArcStr;

use crate::{
    db::api::{properties::internal::ConstPropertiesOps, view::BoxedLIter},
    prelude::Prop,
};

use super::GraphStorage;

impl ConstPropertiesOps for GraphStorage {
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_meta().get_const_prop_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph_meta().get_const_prop_name(id)
    }

    fn const_prop_ids(&self) -> BoxedLIter<usize> {
        Box::new(self.graph_meta().const_prop_ids())
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph_meta().get_constant(id)
    }

    fn const_prop_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.graph_meta().constant_names().into_iter())
    }
}
