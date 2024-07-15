use crate::{
    db::api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
    prelude::Prop,
};
use raphtory_api::core::storage::arc_str::ArcStr;

use crate::disk_graph::DiskGraph;

impl TemporalPropertiesOps for DiskGraph {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.graph_props.get_temporal_id(name)
    }

    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.graph_props.get_temporal_name(id)
    }

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.graph_props.temporal_ids())
    }

    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(self.graph_props.temporal_names().into_iter())
    }
}

impl TemporalPropertyViewOps for DiskGraph {
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.graph_props
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(t, _)| t).collect())
            .unwrap_or_default()
    }

    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.graph_props
            .get_temporal_prop(id)
            .map(|prop| prop.iter_t().map(|(_, v)| v).collect())
            .unwrap_or_default()
    }
}
