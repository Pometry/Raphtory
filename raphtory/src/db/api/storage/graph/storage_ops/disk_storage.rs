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
        storage::timeindex::{TimeIndexEntry, TimeIndexOps},
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

impl GraphLike<TimeIndexEntry> for Graph {
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

    fn edge_additions(&self, eid: EID, layer: usize) -> impl Iterator<Item = TimeIndexEntry> + '_ {
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
        edge_ts: &[TimeIndexEntry],
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

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use proptest::{prelude::*, sample::size_range};
    use rayon::prelude::*;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;

    use super::DiskGraphStorage;
    use crate::{
        db::api::{storage::graph::storage_ops::GraphStorage, view::StaticGraphViewOps},
        prelude::*,
    };
    use raphtory_storage::disk::Time;

    fn make_simple_graph(graph_dir: impl AsRef<Path>, edges: &[(u64, u64, i64, f64)]) -> Graph {
        let storage = DiskGraphStorage::make_simple_graph(graph_dir, edges, 1000, 1000);
        Graph::from_internal_graph(GraphStorage::Disk(Arc::new(storage)))
    }

    fn check_graph_counts(edges: &[(u64, u64, Time, f64)], g: &impl StaticGraphViewOps) {
        // check number of nodes
        let expected_len = edges
            .iter()
            .flat_map(|(src, dst, _, _)| vec![*src, *dst])
            .sorted()
            .dedup()
            .count();
        assert_eq!(g.count_nodes(), expected_len);

        // check number of edges
        let expected_len = edges
            .iter()
            .map(|(src, dst, _, _)| (*src, *dst))
            .sorted()
            .dedup()
            .count();
        assert_eq!(g.count_edges(), expected_len);

        // get edges back
        assert!(edges
            .iter()
            .all(|(src, dst, _, _)| g.edge(*src, *dst).is_some()));

        assert!(edges.iter().all(|(src, dst, _, _)| g.has_edge(*src, *dst)));

        // check earlies_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).min().unwrap();
        assert_eq!(g.earliest_time(), Some(expected));

        // check latest_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).max().unwrap();
        assert_eq!(g.latest_time(), Some(expected));

        // get edges over window

        let g = g.window(i64::MIN, i64::MAX).layers(Layer::Default).unwrap();

        // get edges back from full windows with all layers
        assert!(edges
            .iter()
            .all(|(src, dst, _, _)| g.edge(*src, *dst).is_some()));

        assert!(edges.iter().all(|(src, dst, _, _)| g.has_edge(*src, *dst)));

        // check earlies_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).min().unwrap();
        assert_eq!(g.earliest_time(), Some(expected));

        // check latest_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).max().unwrap();
        assert_eq!(g.latest_time(), Some(expected));
    }

    #[test]
    fn test_1_edge() {
        let test_dir = tempfile::tempdir().unwrap();
        let edges = vec![(1u64, 2u64, 0i64, 4.0)];
        let g = make_simple_graph(test_dir, &edges);
        check_graph_counts(&edges, &g);
    }

    #[test]
    fn test_2_edges() {
        let test_dir = tempfile::tempdir().unwrap();
        let edges = vec![(0, 0, 0, 0.0), (4, 1, 2, 0.0)];
        let g = make_simple_graph(test_dir, &edges);
        check_graph_counts(&edges, &g);
    }

    #[test]
    fn graph_degree_window() {
        let test_dir = tempfile::tempdir().unwrap();
        let mut edges = vec![
            (1u64, 1u64, 0i64, 4.0),
            (1, 1, 1, 6.0),
            (1, 2, 1, 1.0),
            (1, 3, 2, 2.0),
            (2, 1, -1, 3.0),
            (3, 2, 7, 5.0),
        ];

        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));

        let g = make_simple_graph(test_dir, &edges);
        let expected = vec![(2, 3, 0), (1, 0, 0), (1, 0, 0)];
        check_degrees(&g, &expected)
    }

    fn check_degrees(g: &impl StaticGraphViewOps, expected: &[(usize, usize, usize)]) {
        let actual = (1..=3)
            .map(|i| {
                let v = g.node(i).unwrap();
                (
                    v.window(-1, 7).in_degree(),
                    v.window(1, 7).out_degree(),
                    0, // v.window(0, 1).degree(), // we don't support both direction edges yet
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_windows() {
        let test_dir = tempfile::tempdir().unwrap();
        let mut edges = vec![
            (1u64, 1u64, -2i64, 4.0),
            (1u64, 2u64, -1i64, 4.0),
            (1u64, 2u64, 0i64, 4.0),
            (1u64, 3u64, 1i64, 4.0),
            (1u64, 4u64, 2i64, 4.0),
            (1u64, 4u64, 3i64, 4.0),
        ];

        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));

        let g = make_simple_graph(test_dir, &edges);

        let w_g = g.window(-1, 0);

        // let actual = w_g.edges().count();
        // let expected = 1;
        // assert_eq!(actual, expected);

        let out_v_deg = w_g.nodes().out_degree().iter_values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![1, 0]);

        let w_g = g.window(-2, 0);
        let out_v_deg = w_g.nodes().out_degree().iter_values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![2, 0]);

        let w_g = g.window(-2, 4);
        let out_v_deg = w_g.nodes().out_degree().iter_values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![4, 0, 0, 0]);

        let in_v_deg = w_g.nodes().in_degree().iter_values().collect::<Vec<_>>();
        assert_eq!(in_v_deg, vec![1, 1, 1, 1]);
    }

    #[test]
    fn test_temp_props() {
        let test_dir = tempfile::tempdir().unwrap();
        let mut edges = vec![
            (1u64, 2u64, -2i64, 1.0),
            (1u64, 2u64, -1i64, 2.0),
            (1u64, 2u64, 0i64, 3.0),
            (1u64, 2u64, 1i64, 4.0),
            (1u64, 3u64, 2i64, 1.0),
            (1u64, 3u64, 3i64, 2.0),
        ];

        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));

        let g = make_simple_graph(test_dir, &edges);

        // check all properties
        let edge_t_props = weight_props(&g);

        assert_eq!(
            edge_t_props,
            vec![(-2, 1.0), (-1, 2.0), (0, 3.0), (1, 4.0), (2, 1.0), (3, 2.0)]
        );

        // window the graph half way
        let w_g = g.window(-2, 0);
        let edge_t_props = weight_props(&w_g);
        assert_eq!(edge_t_props, vec![(-2, 1.0), (-1, 2.0)]);

        // window the other half
        let w_g = g.window(0, 3);
        let edge_t_props = weight_props(&w_g);
        assert_eq!(edge_t_props, vec![(0, 3.0), (1, 4.0), (2, 1.0)]);
    }

    fn weight_props(g: &impl StaticGraphViewOps) -> Vec<(i64, f64)> {
        let edge_t_props: Vec<_> = g
            .edges()
            .into_iter()
            .flat_map(|e| {
                e.properties()
                    .temporal()
                    .get("weight")
                    .into_iter()
                    .flat_map(|t_prop| t_prop.into_iter())
            })
            .filter_map(|(t, t_prop)| t_prop.into_f64().map(|v| (t, v)))
            .collect();
        edge_t_props
    }

    proptest! {
        #[test]
        fn test_graph_count_nodes(
            edges in any_with::<Vec<(u64, u64, Time, f64)>>(size_range(1..=1000).lift()).prop_map(|mut v| {
                v.sort_by(|(a1, b1, c1, _),(a2, b2, c2, _) | {
                    (a1, b1, c1).cmp(&(a2, b2, c2))
                });
                v
            })
        ) {
            let test_dir = tempfile::tempdir().unwrap();
            let g = make_simple_graph(test_dir, &edges);
            check_graph_counts(&edges, &g);

        }
    }

    #[test]
    fn test_par_nodes() {
        let test_dir = TempDir::new().unwrap();

        let mut edges = vec![
            (1u64, 2u64, -2i64, 1.0),
            (1u64, 2u64, -1i64, 2.0),
            (1u64, 2u64, 0i64, 3.0),
            (1u64, 2u64, 1i64, 4.0),
            (1u64, 3u64, 2i64, 1.0),
            (1u64, 3u64, 3i64, 2.0),
        ];

        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));

        let g = make_simple_graph(test_dir.path(), &edges);

        assert_eq!(g.nodes().par_iter().count(), g.count_nodes())
    }
}
