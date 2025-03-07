use crate::arrow2::array::Array;
use raphtory_api::core::{
    entities::{EID, GID, VID},
    storage::timeindex::AsTime,
};

pub trait GraphLike<T: AsTime>: Send + Sync {
    fn external_ids(&self) -> Vec<GID>;

    fn layer_names(&self) -> Vec<String>;

    fn num_nodes(&self) -> usize;

    fn num_edges(&self) -> usize;

    fn all_edges(&self) -> impl Iterator<Item = (VID, VID, EID)> + '_ {
        (0..self.num_nodes()).map(VID).flat_map(|vid| {
            self.out_neighbours(vid)
                .map(move |(n_id, eid)| (vid, n_id, eid))
        })
    }

    fn out_degree(&self, vid: VID, layer: usize) -> usize;

    fn in_degree(&self, vid: VID, layer: usize) -> usize;

    fn out_edges(&self, vid: VID, layer: usize) -> Vec<(VID, VID, EID)>;

    fn out_neighbours(&self, vid: VID) -> impl Iterator<Item = (VID, EID)> + '_;

    fn edge_additions(&self, eid: EID, layer: usize) -> impl Iterator<Item = T> + '_;

    fn edge_prop_keys(&self) -> Vec<String>;

    fn find_name(&self, vid: VID) -> Option<String>;

    fn node_names(&self) -> impl Iterator<Item = String>;

    fn node_type_ids(&self) -> Option<impl Iterator<Item = usize>>;

    fn node_types(&self) -> Option<impl Iterator<Item = String>>;

    fn prop_as_arrow<S: AsRef<str>>(
        &self,
        edges: &[u64],
        edge_id_map: &[usize],
        edge_ts: &[T],
        edge_ts_offsets: &[usize],
        layer: usize,
        prop_id: usize,
        key: S,
    ) -> Option<Box<dyn Array>>;

    fn in_edges<B>(&self, vid: VID, layer: usize, map: impl Fn(VID, EID) -> B) -> Vec<B>;

    fn earliest_time(&self) -> i64;

    fn latest_time(&self) -> i64;
}
