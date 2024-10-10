use crate::{
    core::{
        entities::{LayerIds, ELID},
        storage::timeindex::{TimeIndexIntoOps, TimeIndexOps},
        utils::iter::GenLockedIter,
        Direction,
    },
    db::api::{
        storage::graph::{
            edges::edge_storage_ops::EdgeStorageOps,
            nodes::node_storage_ops::{NodeStorageIntoOps, NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{internal::CoreGraphOps, IntoDynBoxed, TimeSemantics},
    },
    disk_graph::graph_impl::prop_conversion::arrow_array_from_props,
    prelude::*,
};
use itertools::Itertools;
use polars_arrow::array::Array;
use pometry_storage::interop::GraphLike;
use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};

impl GraphLike<TimeIndexEntry> for Graph {
    fn external_ids(&self) -> Vec<GID> {
        self.nodes().id().collect()
    }

    fn node_names(&self) -> impl Iterator<Item = String> {
        self.nodes().name().into_iter()
    }

    fn node_type_ids(&self) -> Option<impl Iterator<Item = usize>> {
        if self.core_graph().node_meta().node_type_meta().len() <= 1 {
            None
        } else {
            let core_nodes = self.core_nodes();
            Some((0..core_nodes.len()).map(move |i| core_nodes.node_entry(VID(i)).node_type_id()))
        }
    }

    fn node_types(&self) -> Option<impl Iterator<Item = String>> {
        let meta = self.core_graph().node_meta().node_type_meta();
        if meta.len() <= 1 {
            None
        } else {
            Some(meta.get_keys().into_iter().map(|s| s.to_string()))
        }
    }

    fn layer_names(&self) -> Vec<String> {
        self.edge_meta()
            .layer_meta()
            .get_keys()
            .into_iter()
            .map_into()
            .collect()
    }

    fn num_nodes(&self) -> usize {
        self.unfiltered_num_nodes()
    }

    fn num_edges(&self) -> usize {
        self.count_edges()
    }

    fn out_degree(&self, vid: VID, layer: usize) -> usize {
        self.core_node_entry(vid.0.into())
            .degree(&LayerIds::One(layer), Direction::OUT)
    }

    fn in_degree(&self, vid: VID, layer: usize) -> usize {
        self.core_node_entry(vid.0.into())
            .degree(&LayerIds::One(layer), Direction::IN)
    }

    fn in_edges<B>(&self, vid: VID, layer: usize, map: impl Fn(VID, EID) -> B) -> Vec<B> {
        let node = self.core_node_entry(vid.0.into());
        node.edges_iter(&LayerIds::One(layer), Direction::IN)
            .map(|edge| map(edge.src(), edge.pid()))
            .collect()
    }
    fn out_edges(&self, vid: VID, layer: usize) -> Vec<(VID, VID, EID)> {
        let node = self.core_node_entry(vid.0.into());
        let edges = node
            .edges_iter(&LayerIds::One(layer), Direction::OUT)
            .map(|edge| {
                let src = edge.src();
                let dst = edge.dst();
                let eid = edge.pid();
                (src, dst, eid)
            })
            .collect();
        edges
    }

    fn edge_additions(&self, eid: EID, layer: usize) -> impl Iterator<Item = TimeIndexEntry> + '_ {
        let el_id = ELID::new(eid.0.into(), Some(layer));
        let edge = self.core_edge_arc(el_id);
        GenLockedIter::from(edge, |edge| {
            edge.additions(layer).into_iter().into_dyn_boxed()
        })
    }

    fn edge_prop_keys(&self) -> Vec<String> {
        let props = self.edge_meta().temporal_prop_meta().get_keys();
        props.into_iter().map(|s| s.to_string()).collect()
    }

    fn find_name(&self, vid: VID) -> Option<String> {
        self.core_node_entry(vid.0.into())
            .name()
            .map(|s| s.to_string())
    }

    fn prop_as_arrow<S: AsRef<str>>(
        &self,
        edges: &[u64],
        edge_ts: &[TimeIndexEntry],
        edge_t_offsets: &[usize],
        layer: usize,
        prop_id: usize,
        _key: S,
    ) -> Option<Box<dyn Array>> {
        let prop_type = self
            .edge_meta()
            .temporal_prop_meta()
            .get_dtype(prop_id)
            .unwrap();
        arrow_array_from_props(
            edges.iter().enumerate().flat_map(|(index, &eid)| {
                let ts = &edge_ts[edge_t_offsets[index]..edge_t_offsets[index + 1]];
                let el_id = ELID::new((eid as usize).into(), Some(layer));
                let edge = self.core_edge(el_id);
                ts.iter()
                    .map(move |t| edge.temporal_prop_layer(layer, prop_id).at(t))
            }),
            prop_type,
        )
    }

    fn earliest_time(&self) -> i64 {
        self.earliest_time_global().unwrap_or(i64::MAX)
    }

    fn latest_time(&self) -> i64 {
        self.latest_time_global().unwrap_or(i64::MIN)
    }

    fn resolve_layer(&self, name: &str) -> Option<usize> {
        self.edge_meta().layer_meta().get_id(name)
    }

    fn out_neighbours(&self, vid: VID) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.core_node_arc(vid)
            .into_edges_iter(LayerIds::All, Direction::OUT)
            .map(|e_ref| (e_ref.dst(), e_ref.pid()))
    }
}
