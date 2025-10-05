use crate::{
    db::{
        api::view::internal::GraphTimeSemanticsOps, graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    prelude::{Graph, GraphViewOps, NodeStateOps, NodeViewOps},
};
use itertools::Itertools;
use polars_arrow::array::Array;
use pometry_storage::interop::GraphLike;
use raphtory_api::{
    core::{
        entities::{properties::tprop::TPropOps, LayerIds, EID, GID, VID},
        storage::timeindex::{EventTime, TimeIndexOps},
        Direction,
    },
    iter::IntoDynBoxed,
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{
    core_ops::CoreGraphOps,
    disk::{graph_impl::prop_conversion::arrow_array_from_props, DiskGraphStorage},
    graph::{
        edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage,
        nodes::node_storage_ops::NodeStorageOps,
    },
};
use std::{path::Path, sync::Arc};

impl From<DiskGraphStorage> for Graph {
    fn from(value: DiskGraphStorage) -> Self {
        Graph::from_internal_graph(GraphStorage::Disk(Arc::new(value)))
    }
}

impl From<DiskGraphStorage> for PersistentGraph {
    fn from(value: DiskGraphStorage) -> Self {
        PersistentGraph::from_internal_graph(GraphStorage::Disk(Arc::new(value)))
    }
}

pub trait IntoGraph {
    fn into_graph(self) -> Graph;

    fn into_persistent_graph(self) -> PersistentGraph;
}

impl IntoGraph for DiskGraphStorage {
    fn into_graph(self) -> Graph {
        self.into()
    }

    fn into_persistent_graph(self) -> PersistentGraph {
        self.into()
    }
}

impl Graph {
    pub fn persist_as_disk_graph(&self, graph_dir: impl AsRef<Path>) -> Result<Graph, GraphError> {
        Ok(Graph::from(DiskGraphStorage::from_graph(self, graph_dir)?))
    }
}

impl PersistentGraph {
    pub fn persist_as_disk_graph(
        &self,
        graph_dir: impl AsRef<Path>,
    ) -> Result<PersistentGraph, GraphError> {
        Ok(PersistentGraph::from(DiskGraphStorage::from_graph(
            &self.event_graph(),
            graph_dir,
        )?))
    }
}

impl GraphLike<EventTime> for Graph {
    fn external_ids(&self) -> Vec<GID> {
        self.nodes().id().collect()
    }

    fn node_names(&self) -> impl Iterator<Item = String> {
        self.nodes().name().into_iter_values()
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
        self.core_node(vid.0.into())
            .degree(&LayerIds::One(layer), Direction::OUT)
    }

    fn in_degree(&self, vid: VID, layer: usize) -> usize {
        self.core_node(vid.0.into())
            .degree(&LayerIds::One(layer), Direction::IN)
    }

    fn in_edges<B>(&self, vid: VID, layer: usize, map: impl Fn(VID, EID) -> B) -> Vec<B> {
        let node = self.core_node(vid.0.into());
        node.edges_iter(&LayerIds::One(layer), Direction::IN)
            .map(|edge| map(edge.src(), edge.pid()))
            .collect()
    }
    fn out_edges(&self, vid: VID, layer: usize) -> Vec<(VID, VID, EID)> {
        let node = self.core_node(vid.0.into());
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

    fn edge_additions(&self, eid: EID, layer: usize) -> impl Iterator<Item = EventTime> + '_ {
        let edge = self.core_edge(eid);
        GenLockedIter::from(edge, |edge| edge.additions(layer).iter().into_dyn_boxed())
    }

    fn edge_prop_keys(&self) -> Vec<String> {
        let props = self.edge_meta().temporal_prop_mapper().get_keys();
        props.into_iter().map(|s| s.to_string()).collect()
    }

    fn find_name(&self, vid: VID) -> Option<String> {
        self.core_node(vid.0.into()).name().map(|s| s.to_string())
    }

    fn prop_as_arrow<S: AsRef<str>>(
        &self,
        disk_edges: &[u64],
        edge_id_map: &[usize],
        edge_ts: &[EventTime],
        edge_t_offsets: &[usize],
        layer: usize,
        prop_id: usize,
        _key: S,
    ) -> Option<Box<dyn Array>> {
        let prop_type = self
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(prop_id)
            .unwrap();
        arrow_array_from_props(
            disk_edges.iter().flat_map(|&disk_eid| {
                let disk_eid = disk_eid as usize;
                let eid = edge_id_map[disk_eid];
                let ts = &edge_ts[edge_t_offsets[disk_eid]..edge_t_offsets[disk_eid + 1]];
                let edge = self.core_edge(EID(eid));
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

    fn out_neighbours(&self, vid: VID) -> impl Iterator<Item = (VID, EID)> + '_ {
        self.core_node(vid)
            .into_edges_iter(&LayerIds::All, Direction::OUT)
            .map(|e_ref| (e_ref.dst(), e_ref.pid()))
    }
}
