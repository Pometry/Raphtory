use crate::{
    core::ArcStr,
    db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
    prelude::Prop,
};

use super::ArrowGraph;

impl TemporalPropertiesOps for ArrowGraph {
    fn get_temporal_prop_id(&self, _name: &str) -> Option<usize> {
        todo!()
    }

    fn get_temporal_prop_name(&self, _id: usize) -> ArcStr {
        todo!()
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        todo!()
    }
}

impl TemporalPropertyViewOps for ArrowGraph {
    fn temporal_history(&self, _id: usize) -> Vec<i64> {
        todo!()
    }

    fn temporal_values(&self, _id: usize) -> Vec<Prop> {
        todo!()
    }
}
