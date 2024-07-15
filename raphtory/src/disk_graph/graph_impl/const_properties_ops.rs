use crate::{db::api::properties::internal::ConstPropertiesOps, prelude::Prop};
use raphtory_api::core::storage::arc_str::ArcStr;

use crate::disk_graph::DiskGraph;

impl ConstPropertiesOps for DiskGraph {
    #[doc = " Find id for property name (note this only checks the meta-data, not if the property actually exists for the entity)"]
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_props.get_const_prop_id(name)
    }

    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.graph_props.get_const_prop_name(id)
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.graph_props.const_prop_ids())
    }

    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.graph_props.get_constant(id)
    }
}
