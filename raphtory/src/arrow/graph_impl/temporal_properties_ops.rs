use crate::{
    core::ArcStr,
    db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
    prelude::Prop,
};

use super::Graph2;

impl TemporalPropertiesOps for Graph2 {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        todo!()
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        todo!()
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        todo!()
    }
}

impl TemporalPropertyViewOps for Graph2 {
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        todo!()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        todo!()
    }
}
