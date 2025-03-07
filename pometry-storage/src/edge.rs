use super::{chunked_array::chunked_array::NonNull, timestamps::TimeStamps};
use crate::{
    arrow2::{array::PrimitiveArray, datatypes::Field},
    chunked_array::{chunked_array::ChunkedArray, ChunkedArraySlice},
    graph::TemporalGraph,
};
use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::AsTime,
};
use std::fmt::Debug;

#[derive(Clone, Debug, Copy)]
pub struct Edge<'a> {
    eid: EID,
    graph: &'a TemporalGraph,
}

impl<'a> Edge<'a> {
    pub(crate) fn new(eid: EID, graph: &'a TemporalGraph) -> Self {
        Self { eid, graph }
    }

    pub fn graph(&self) -> &'a TemporalGraph {
        self.graph
    }

    pub fn src_id(&self) -> VID {
        self.graph.edge_list.get_src(self.eid)
    }

    pub fn dst_id(&self) -> VID {
        self.graph.edge_list.get_dst(self.eid)
    }

    pub fn pid(&self) -> EID {
        self.eid
    }

    pub fn internal_num_layers(&self) -> usize {
        self.graph.num_layers()
    }

    pub fn has_layer_inner(&self, layer_id: usize) -> bool {
        self.graph
            .layer(layer_id)
            .edges_storage()
            .has_t_props(self.eid)
    }

    pub fn timestamp_slice(
        self,
        layer_id: usize,
    ) -> ChunkedArraySlice<'a, &'a ChunkedArray<PrimitiveArray<i64>, NonNull>> {
        self.graph
            .layer(layer_id)
            .edges_storage()
            .time_col()
            .value(self.eid.0)
    }

    pub fn temporal_prop_layer_inner(&self, layer_id: usize, prop_id: usize) -> Option<Field> {
        self.graph
            .layer(layer_id)
            .edges_storage()
            .prop_dtypes()
            .get(prop_id)
            .cloned()
    }

    pub fn get_additions<T: AsTime>(self, layer_id: usize) -> TimeStamps<'a, T> {
        TimeStamps::new(self.timestamp_slice(layer_id), None)
    }
}
