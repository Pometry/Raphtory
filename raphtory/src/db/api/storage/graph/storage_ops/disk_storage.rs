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
    use bigdecimal::BigDecimal;
    use itertools::Itertools;
    use polars_arrow::array::Utf8Array;
    use proptest::{prelude::*, sample::size_range};
    use rayon::prelude::*;
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
        sync::Arc,
    };
    use tempfile::TempDir;

    use super::{DiskGraphStorage, IntoGraph};
    use crate::{
        db::{
            api::{storage::graph::storage_ops::GraphStorage, view::StaticGraphViewOps},
            graph::graph::assert_graph_equal,
        },
        prelude::*,
    };
    use pometry_storage::{graph::TemporalGraph, properties::Properties};
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_storage::disk::{ParquetLayerCols, Time};

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
    fn test_no_prop_nodes() {
        let test_dir = TempDir::new().unwrap();
        let g = Graph::new();
        g.add_node(0, 0, NO_PROPS, None).unwrap();
        // g.add_node(1, 1, [("test", "test")], None).unwrap();
        let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(disk_g.node(0).unwrap().earliest_time(), Some(0));
        assert_graph_equal(&g, &disk_g);
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

    #[test]
    fn test_mem_to_disk_graph() {
        let mem_graph = Graph::new();
        mem_graph.add_edge(0, 0, 1, [("test", 0u64)], None).unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph =
            TemporalGraph::from_graph(&mem_graph, test_dir.path(), || Ok(Properties::default()))
                .unwrap();
        assert_eq!(disk_graph.num_nodes(), 2);
        assert_eq!(disk_graph.num_edges(), 1);
    }

    #[test]
    fn test_node_properties() {
        let mem_graph = Graph::new();
        let node = mem_graph
            .add_node(
                0,
                0,
                [
                    ("test_num", 0u64.into_prop()),
                    ("test_str", "test".into_prop()),
                ],
                None,
            )
            .unwrap();
        node.add_metadata([
            ("const_str", "test_c".into_prop()),
            ("const_float", 0.314f64.into_prop()),
        ])
        .unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = mem_graph.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(disk_graph.count_nodes(), 1);
        let props = disk_graph.node(0).unwrap().properties();
        let metadata = disk_graph.node(0).unwrap().metadata();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(metadata.get("const_str").unwrap_str(), "test_c");
        assert_eq!(metadata.get("const_float").unwrap_f64(), 0.314);

        let temp = disk_graph.node(0).unwrap().properties().temporal();
        assert_eq!(
            temp.get("test_num").unwrap().latest().unwrap(),
            0u64.into_prop()
        );
        assert_eq!(
            temp.get("test_str").unwrap().latest().unwrap(),
            "test".into_prop()
        );

        drop(disk_graph);

        let disk_graph: Graph = DiskGraphStorage::load_from_dir(test_dir.path())
            .unwrap()
            .into();
        let props = disk_graph.node(0).unwrap().properties();
        let metadata = disk_graph.node(0).unwrap().metadata();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(metadata.get("const_str").unwrap_str(), "test_c");
        assert_eq!(metadata.get("const_float").unwrap_f64(), 0.314);

        let temp = disk_graph.node(0).unwrap().properties().temporal();
        assert_eq!(
            temp.get("test_num").unwrap().latest().unwrap(),
            0u64.into_prop()
        );
        assert_eq!(
            temp.get("test_str").unwrap().latest().unwrap(),
            "test".into_prop()
        );
    }

    #[test]
    fn test_node_properties_2() {
        let g = Graph::new();
        g.add_edge(1, 1u64, 1u64, NO_PROPS, None).unwrap();
        let props_t1 = [
            ("prop 1", 1u64.into_prop()),
            ("prop 3", "hi".into_prop()),
            ("prop 4", true.into_prop()),
        ];
        let v = g.add_node(1, 1u64, props_t1, None).unwrap();
        let props_t2 = [
            ("prop 1", 2u64.into_prop()),
            ("prop 2", 0.6.into_prop()),
            ("prop 4", false.into_prop()),
        ];
        v.add_updates(2, props_t2).unwrap();
        let props_t3 = [
            ("prop 2", 0.9.into_prop()),
            ("prop 3", "hello".into_prop()),
            ("prop 4", true.into_prop()),
        ];
        v.add_updates(3, props_t3).unwrap();
        v.add_metadata([("static prop", 123)]).unwrap();

        let test_dir = TempDir::new().unwrap();
        let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();

        let actual = disk_graph
            .at(2)
            .node(1u64)
            .unwrap()
            .properties()
            .temporal()
            .into_iter()
            .map(|(key, t_view)| (key.to_string(), t_view.into_iter().collect::<Vec<_>>()))
            .filter(|(_, v)| !v.is_empty())
            .collect::<Vec<_>>();

        let expected = vec![
            ("prop 1".to_string(), vec![(2, 2u64.into_prop())]),
            ("prop 4".to_string(), vec![(2, false.into_prop())]),
            ("prop 2".to_string(), vec![(2, 0.6.into_prop())]),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_only_const_node_properties() {
        let g = Graph::new();
        let v = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v.add_metadata([("test", "test")]).unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();
        assert_eq!(
            disk_graph
                .node(1)
                .unwrap()
                .metadata()
                .get("test")
                .unwrap_str(),
            "test"
        );
        let disk_graph = DiskGraphStorage::load_from_dir(test_dir.path())
            .unwrap()
            .into_graph();
        assert_eq!(
            disk_graph
                .node(1)
                .unwrap()
                .metadata()
                .get("test")
                .unwrap_str(),
            "test"
        );
    }

    #[test]
    fn test_type_filter_disk_graph_loaded_from_parquets() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let graph_dir = tmp_dir.path();
        let chunk_size = 268_435_456;
        let num_threads = 4;
        let t_props_chunk_size = chunk_size / 8;

        let netflow_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/netflow.parquet"))
            .unwrap();

        let v1_layer_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/wls.parquet"))
            .unwrap();

        let node_properties = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("pometry-storage-private/resources/test/node_types.parquet"))
            .unwrap();

        let layer_parquet_cols = vec![
            ParquetLayerCols {
                parquet_dir: netflow_layer_path.to_str().unwrap(),
                layer: "netflow",
                src_col: "source",
                dst_col: "destination",
                time_col: "time",
                exclude_edge_props: vec![],
            },
            ParquetLayerCols {
                parquet_dir: v1_layer_path.to_str().unwrap(),
                layer: "wls",
                src_col: "src",
                dst_col: "dst",
                time_col: "Time",
                exclude_edge_props: vec![],
            },
        ];

        let node_type_col = Some("node_type");

        let g = DiskGraphStorage::load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            Some(&node_properties),
            chunk_size,
            t_props_chunk_size,
            num_threads,
            node_type_col,
            None,
        )
        .unwrap()
        .into_graph();

        assert_eq!(
            g.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A", "B"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp244393"], vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["C"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            Vec::<Vec<&str>>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .type_filter(["A"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(["A"])
                .neighbours()
                .type_filter(Vec::<&str>::new())
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );

        let w = g.window(6415659, 7387801);

        assert_eq!(
            w.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            w.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            w.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );

        let l = g.layers(["netflow"]).unwrap();

        assert_eq!(
            l.nodes().type_filter(["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            l.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            l.nodes().type_filter([""]).name().collect_vec(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_type_filter_disk_graph_created_from_in_memory_graph() {
        let g = Graph::new();
        g.add_node(1, 1, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 2, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 3, NO_PROPS, Some("b")).unwrap();
        g.add_node(1, 4, NO_PROPS, Some("a")).unwrap();
        g.add_node(1, 5, NO_PROPS, Some("c")).unwrap();
        g.add_node(1, 6, NO_PROPS, Some("e")).unwrap();
        g.add_node(1, 7, NO_PROPS, None).unwrap();
        g.add_node(1, 8, NO_PROPS, None).unwrap();
        g.add_node(1, 9, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 2, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 2, 4, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 4, 5, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 5, 6, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 3, 6, NO_PROPS, Some("a")).unwrap();

        let tmp_dir = tempfile::tempdir().unwrap();
        let g = DiskGraphStorage::from_graph(&g, tmp_dir.path())
            .unwrap()
            .into_graph();

        assert_eq!(
            g.nodes()
                .type_filter(["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );

        let g = DiskGraphStorage::load_from_dir(tmp_dir.path())
            .unwrap()
            .into_graph();

        assert_eq!(
            g.nodes()
                .type_filter(["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter([""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );
    }

    #[test]
    fn test_reload() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, [("weight", 0.)], None).unwrap();
        graph.add_edge(1, 0, 1, [("weight", 1.)], None).unwrap();
        graph.add_edge(2, 0, 1, [("weight", 2.)], None).unwrap();
        graph.add_edge(3, 1, 2, [("weight", 3.)], None).unwrap();
        let disk_graph = graph.persist_as_disk_graph(graph_dir.path()).unwrap();
        assert_graph_equal(&disk_graph, &graph);

        let reloaded_graph = DiskGraphStorage::load_from_dir(graph_dir.path())
            .unwrap()
            .into_graph();
        assert_graph_equal(&reloaded_graph, &graph);
    }

    #[test]
    fn test_load_node_types() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let mut dg = DiskGraphStorage::from_graph(&graph, graph_dir.path()).unwrap();
        dg.load_node_types_from_arrays([Ok(Utf8Array::<i32>::from_slice(["1", "2"]).boxed())], 100)
            .unwrap();
        assert_eq!(
            dg.into_graph().nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
    }

    #[test]
    fn test_node_type() {
        let graph_dir = TempDir::new().unwrap();
        let graph = Graph::new();
        graph.add_node(0, 0, NO_PROPS, Some("1")).unwrap();
        graph.add_node(0, 1, NO_PROPS, Some("2")).unwrap();
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let dg = graph.persist_as_disk_graph(graph_dir.path()).unwrap();
        assert_eq!(
            dg.nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
        let dg = DiskGraphStorage::load_from_dir(graph_dir.path()).unwrap();
        assert_eq!(
            dg.into_graph().nodes().node_type().collect_vec(),
            [Some("1".into()), Some("2".into())]
        );
    }
    mod addition_bounds {
        use super::{AdditionOps, Graph};
        use crate::{
            db::{
                api::storage::graph::storage_ops::disk_storage::IntoGraph,
                graph::graph::assert_graph_equal,
            },
            test_utils::{build_edge_list, build_graph_from_edge_list},
        };
        use proptest::prelude::*;
        use raphtory_storage::disk::DiskGraphStorage;
        use tempfile::TempDir;

        #[test]
        fn test_load_from_graph_missing_edge() {
            let g = Graph::new();
            g.add_edge(0, 1, 2, [("test", "test1")], Some("1")).unwrap();
            g.add_edge(1, 2, 3, [("test", "test2")], Some("2")).unwrap();
            let test_dir = TempDir::new().unwrap();
            let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
            assert_graph_equal(&disk_g, &g);
        }

        #[test]
        fn disk_graph_persist_proptest() {
            proptest!(|(edges in build_edge_list(100, 10))| {
                let g = build_graph_from_edge_list(&edges);
                let test_dir = TempDir::new().unwrap();
                let disk_g = g.persist_as_disk_graph(test_dir.path()).unwrap();
                assert_graph_equal(&disk_g, &g);
                let reloaded_disk_g = DiskGraphStorage::load_from_dir(test_dir.path()).unwrap().into_graph();
                assert_graph_equal(&reloaded_disk_g, &g);
            } )
        }
    }

    #[test]
    fn load_decimal_column() {
        let parquet_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("resources/test/data_0.parquet")
            .to_string_lossy()
            .to_string();

        let graph_dir = tempfile::tempdir().unwrap();

        let layer_parquet_cols = vec![ParquetLayerCols {
            parquet_dir: parquet_file_path.as_ref(),
            layer: "large",
            src_col: "from_address",
            dst_col: "to_address",
            time_col: "block_timestamp",
            exclude_edge_props: vec![],
        }];
        let dgs = DiskGraphStorage::load_from_parquets(
            graph_dir.path(),
            layer_parquet_cols,
            None,
            100,
            100,
            1,
            None,
            None,
        )
        .unwrap();

        let g = dgs.into_graph();
        let (_, actual): (Vec<_>, Vec<_>) = g
            .edges()
            .properties()
            .flat_map(|props| props.temporal().into_iter())
            .flat_map(|(_, view)| view.into_iter())
            .unzip();

        let expected = [
            "20000000000000000000.000000000",
            "20000000000000000000.000000000",
            "20000000000000000000.000000000",
            "24000000000000000000.000000000",
            "20000000000000000000.000000000",
            "104447267751554560119.000000000",
            "42328815976923864739.000000000",
            "23073375143032303343.000000000",
            "23069234889247394908.000000000",
            "18729358881519682914.000000000",
        ]
        .into_iter()
        .map(|s| BigDecimal::from_str(s).map(Prop::Decimal))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

        assert_eq!(actual, expected);
    }
}

#[cfg(feature = "storage")]
#[cfg(test)]
mod storage_tests {
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use proptest::prelude::*;
    use tempfile::TempDir;

    use crate::{
        db::{
            api::storage::graph::storage_ops::disk_storage::IntoGraph,
            graph::graph::assert_graph_equal,
        },
        prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS, *},
    };
    use raphtory_api::core::storage::arc_str::OptionAsStr;
    use raphtory_core::entities::nodes::node_ref::AsNodeRef;
    use raphtory_storage::{disk::DiskGraphStorage, mutation::addition_ops::InternalAdditionOps};

    #[test]
    fn test_merge() {
        let g1 = Graph::new();
        g1.add_node(0, 0, [("node_prop", 0f64)], Some("1")).unwrap();
        g1.add_node(0, 1, NO_PROPS, None).unwrap();
        g1.add_node(0, 2, [("node_prop", 2f64)], Some("2")).unwrap();
        g1.add_edge(1, 0, 1, [("test", 1i32)], None).unwrap();
        g1.add_edge(2, 0, 1, [("test", 2i32)], Some("1")).unwrap();
        g1.add_edge(2, 1, 2, [("test2", "test")], None).unwrap();
        g1.node(1)
            .unwrap()
            .add_metadata([("const_str", "test")])
            .unwrap();
        g1.node(0)
            .unwrap()
            .add_updates(3, [("test", "test")])
            .unwrap();

        let g2 = Graph::new();
        g2.add_node(1, 0, [("node_prop", 1f64)], None).unwrap();
        g2.add_node(0, 1, NO_PROPS, None).unwrap();
        g2.add_node(3, 2, [("node_prop", 3f64)], Some("3")).unwrap();
        g2.add_edge(1, 0, 1, [("test", 2i32)], None).unwrap();
        g2.add_edge(3, 0, 1, [("test", 3i32)], Some("2")).unwrap();
        g2.add_edge(2, 1, 2, [("test2", "test")], None).unwrap();
        g2.node(1)
            .unwrap()
            .add_metadata([("const_str2", "test2")])
            .unwrap();
        g2.node(0)
            .unwrap()
            .add_updates(3, [("test", "test")])
            .unwrap();
        let g1_dir = TempDir::new().unwrap();
        let g2_dir = TempDir::new().unwrap();
        let gm_dir = TempDir::new().unwrap();

        let g1_a = DiskGraphStorage::from_graph(&g1, g1_dir.path()).unwrap();
        let g2_a = DiskGraphStorage::from_graph(&g2, g2_dir.path()).unwrap();

        let gm = g1_a
            .merge_by_sorted_gids(&g2_a, &gm_dir)
            .unwrap()
            .into_graph();

        let n0 = gm.node(0).unwrap();
        assert_eq!(
            n0.properties()
                .temporal()
                .get("node_prop")
                .unwrap()
                .iter()
                .collect_vec(),
            [(0, Prop::F64(0.)), (1, Prop::F64(1.))]
        );
        assert_eq!(
            n0.properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            [(3, Prop::str("test")), (3, Prop::str("test"))]
        );
        assert_eq!(n0.node_type().as_str(), Some("1"));
        let n1 = gm.node(1).unwrap();
        assert_eq!(n1.metadata().get("const_str"), Some(Prop::str("test")));
        assert_eq!(n1.metadata().get("const_str2").unwrap_str(), "test2");
        assert!(n1
            .properties()
            .temporal()
            .values()
            .all(|prop| prop.values().next().is_none()));
        let n2 = gm.node(2).unwrap();
        assert_eq!(n2.node_type().as_str(), Some("3")); // right has priority

        assert_eq!(
            gm.default_layer()
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1), (1, 2)]
        );
        assert_eq!(
            gm.valid_layers("1")
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1)]
        );
        assert_eq!(
            gm.valid_layers("2")
                .edges()
                .id()
                .filter_map(|(a, b)| a.as_u64().zip(b.as_u64()))
                .collect::<Vec<_>>(),
            [(0, 1)]
        );
    }

    fn add_edges(g: &Graph, edges: &[(i64, u64, u64)]) {
        let nodes: BTreeSet<_> = edges
            .iter()
            .flat_map(|(_, src, dst)| [*src, *dst])
            .collect();
        for n in nodes {
            g.resolve_node(n.as_node_ref()).unwrap();
        }
        for (t, src, dst) in edges {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }
    }

    fn inner_merge_test(left_edges: &[(i64, u64, u64)], right_edges: &[(i64, u64, u64)]) {
        let left_g = Graph::new();
        add_edges(&left_g, left_edges);
        let right_g = Graph::new();
        add_edges(&right_g, right_edges);
        let merged_g_expected = Graph::new();
        add_edges(&merged_g_expected, left_edges);
        add_edges(&merged_g_expected, right_edges);

        let left_dir = TempDir::new().unwrap();
        let right_dir = TempDir::new().unwrap();
        let merged_dir = TempDir::new().unwrap();

        let left_g_disk = DiskGraphStorage::from_graph(&left_g, left_dir.path()).unwrap();
        let right_g_disk = DiskGraphStorage::from_graph(&right_g, right_dir.path()).unwrap();

        let merged_g_disk = left_g_disk
            .merge_by_sorted_gids(&right_g_disk, &merged_dir)
            .unwrap();
        assert_graph_equal(&merged_g_disk.into_graph(), &merged_g_expected)
    }

    #[test]
    fn test_merge_proptest() {
        proptest!(|(left_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100), right_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100))| {
            inner_merge_test(&left_edges, &right_edges)
        })
    }

    #[test]
    fn test_one_empty_graph_non_zero_time() {
        inner_merge_test(&[], &[(1, 0, 0)])
    }
    #[test]
    fn test_empty_graphs() {
        inner_merge_test(&[], &[])
    }

    #[test]
    fn test_one_empty_graph() {
        inner_merge_test(&[], &[(0, 0, 0)])
    }

    #[test]
    fn inbounds_not_merging() {
        inner_merge_test(&[], &[(0, 0, 0), (0, 0, 1), (0, 0, 2)])
    }

    #[test]
    fn inbounds_not_merging_take2() {
        inner_merge_test(
            &[(0, 0, 2)],
            &[
                (0, 1, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
                (0, 0, 0),
            ],
        )
    }

    #[test]
    fn offsets_panic_overflow() {
        inner_merge_test(
            &[
                (0, 0, 4),
                (0, 0, 4),
                (0, 0, 0),
                (0, 0, 4),
                (0, 1, 2),
                (0, 3, 4),
            ],
            &[(0, 0, 5), (0, 2, 0)],
        )
    }

    #[test]
    fn inbounds_not_merging_take3() {
        inner_merge_test(
            &[
                (0, 0, 4),
                (0, 0, 4),
                (0, 0, 0),
                (0, 0, 4),
                (0, 1, 2),
                (0, 3, 4),
            ],
            &[(0, 0, 3), (0, 0, 4), (0, 2, 2), (0, 0, 5), (0, 0, 6)],
        )
    }
}
