use crate::{core::ArcStr, db::api::properties::internal::ConstPropertiesOps, prelude::Prop};

use super::ArrowGraph;

impl ConstPropertiesOps for ArrowGraph {
    #[doc = " Find id for property name (note this only checks the meta-data, not if the property actually exists for the entity)"]
    fn get_const_prop_id(&self, _name: &str) -> Option<usize> {
        todo!()
    }

    fn get_const_prop_name(&self, _id: usize) -> ArcStr {
        todo!()
    }

    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        todo!()
    }

    fn get_const_prop(&self, _id: usize) -> Option<Prop> {
        todo!()
    }
}
