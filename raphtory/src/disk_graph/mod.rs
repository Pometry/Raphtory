use crate::{
    core::entities::{
        properties::{graph_meta::GraphMeta, props::Meta},
        LayerIds,
    },
    db::api::view::{internal::Immutable, DynamicGraph, IntoDynamic},
    disk_graph::graph_impl::{prop_conversion::make_node_properties_from_graph, ParquetLayerCols},
    prelude::{Graph, GraphViewOps},
};
use polars_arrow::{
    array::{PrimitiveArray, StructArray},
    datatypes::{ArrowDataType as DataType, Field},
};
use pometry_storage::{
    disk_hmap::DiskHashMap, graph::TemporalGraph, graph_fragment::TempColGraphFragment,
    load::ExternalEdgeList, merge::merge_graph::merge_graphs, RAError,
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
};

pub mod graph_impl;
pub mod query;
pub mod storage_interface;

pub type Time = i64;

pub mod prelude {
    pub use pometry_storage::chunked_array::array_ops::*;
}

#[derive(thiserror::Error, Debug)]
pub enum DiskGraphError {
    #[error("Raphtory Arrow Error: {0}")]
    RAError(#[from] pometry_storage::RAError),
}

#[derive(Clone, Debug)]
pub struct DiskGraph {
    pub(crate) inner: Arc<TemporalGraph>,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    graph_props: Arc<GraphMeta>,
    graph_dir: PathBuf,
}

impl Serialize for DiskGraph {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let path = self.graph_dir.clone();
        path.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DiskGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = PathBuf::deserialize(deserializer)?;
        let graph_result = DiskGraph::load_from_dir(&path).map_err(|err| {
            serde::de::Error::custom(format!("Failed to load Diskgraph: {:?}", err))
        })?;
        Ok(graph_result)
    }
}

impl Display for DiskGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Diskgraph(num_nodes={}, num_temporal_edges={}",
            self.count_nodes(),
            self.count_temporal_edges()
        )
    }
}

impl AsRef<TemporalGraph> for DiskGraph {
    fn as_ref(&self) -> &TemporalGraph {
        &self.inner
    }
}

impl Immutable for DiskGraph {}

impl IntoDynamic for DiskGraph {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl DiskGraph {
    pub fn graph_dir(&self) -> &Path {
        &self.graph_dir
    }
    fn layer_from_ids(&self, layer_ids: &LayerIds) -> Option<usize> {
        match layer_ids {
            LayerIds::One(layer_id) => Some(*layer_id),
            LayerIds::None => None,
            LayerIds::All => match self.inner.layers().len() {
                0 => None,
                1 => Some(0),
                _ => todo!("multilayer edge views not yet supported in Diskgraph"),
            },
            LayerIds::Multiple(ids) => match ids.len() {
                0 => None,
                1 => Some(ids[0]),
                _ => todo!("multilayer edge views not yet supported in Diskgraph"),
            },
        }
    }

    pub fn make_simple_graph(
        graph_dir: impl AsRef<Path>,
        edges: &[(u64, u64, i64, f64)],
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> DiskGraph {
        // unzip into 4 vectors
        let (src, (dst, (time, weight))): (Vec<_>, (Vec<_>, (Vec<_>, Vec<_>))) = edges
            .iter()
            .map(|(a, b, c, d)| (*a, (*b, (*c, *d))))
            .unzip();

        let edge_lists = vec![StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("weight", DataType::Float64, false),
            ]),
            vec![
                PrimitiveArray::from_vec(src).boxed(),
                PrimitiveArray::from_vec(dst).boxed(),
                PrimitiveArray::from_vec(time).boxed(),
                PrimitiveArray::from_vec(weight).boxed(),
            ],
            None,
        )];
        DiskGraph::load_from_edge_lists(
            &edge_lists,
            chunk_size,
            t_props_chunk_size,
            graph_dir.as_ref(),
            0,
            1,
            2,
        )
        .expect("failed to create graph")
    }

    /// Merge this graph with another `DiskGraph`. Note that both graphs should have nodes that are
    /// sorted by their global ids or the resulting graph will be nonsense!
    pub fn merge_by_sorted_gids(
        &self,
        other: &DiskGraph,
        new_graph_dir: impl AsRef<Path>,
    ) -> Result<DiskGraph, DiskGraphError> {
        let graph_dir = new_graph_dir.as_ref();
        let inner = merge_graphs(graph_dir, &self.inner, &other.inner)?;
        Ok(DiskGraph::new(inner, graph_dir.to_path_buf()))
    }

    fn new(inner_graph: TemporalGraph, graph_dir: PathBuf) -> Self {
        let node_meta = Meta::new();
        let mut edge_meta = Meta::new();
        let graph_meta = GraphMeta::new();

        for node_type in inner_graph.node_types().into_iter().flatten() {
            if let Some(node_type) = node_type {
                node_meta.get_or_create_node_type_id(node_type);
            } else {
                panic!("Node types cannot be null");
            }
        }

        for layer in inner_graph.layers() {
            let edge_props_fields = layer.edges_data_type();

            for (id, field) in edge_props_fields.iter().enumerate() {
                let prop_name = &field.name;
                let data_type = field.data_type();

                let resolved_id = edge_meta
                    .resolve_prop_id(prop_name, data_type.into(), false)
                    .expect("Arrow data types should without failing");
                if id != resolved_id {
                    println!("Warning: Layers with different edge properties are not supported by the high-level apis on top of the disk_graph graph yet, edge properties will not be available to high-level apis");
                    edge_meta = Meta::new();
                    break;
                }
            }
        }

        for l_name in inner_graph.layer_names() {
            edge_meta.layer_meta().get_or_create_id(l_name);
        }

        if let Some(props) = &inner_graph.node_properties().const_props {
            let node_const_props_fields = props.prop_dtypes();
            for field in node_const_props_fields {
                node_meta
                    .resolve_prop_id(&field.name, field.data_type().into(), true)
                    .expect("Initial resolve should not fail");
            }
        }

        if let Some(props) = &inner_graph.node_properties().temporal_props {
            let node_temporal_props_fields = props.prop_dtypes();
            for field in node_temporal_props_fields {
                node_meta
                    .resolve_prop_id(&field.name, field.data_type().into(), false)
                    .expect("Initial resolve should not fail");
            }
        }

        Self {
            inner: Arc::new(inner_graph),
            node_meta: Arc::new(node_meta),
            edge_meta: Arc::new(edge_meta),
            graph_props: Arc::new(graph_meta),
            graph_dir,
        }
    }

    pub fn from_graph(graph: &Graph, graph_dir: impl AsRef<Path>) -> Result<Self, DiskGraphError> {
        let inner_graph = TemporalGraph::from_graph(graph, graph_dir.as_ref(), || {
            make_node_properties_from_graph(graph, graph_dir.as_ref())
        })?;
        Ok(Self::new(inner_graph, graph_dir.as_ref().to_path_buf()))
    }

    pub fn load_from_edge_lists(
        edge_list: &[StructArray],
        chunk_size: usize,
        t_props_chunk_size: usize,
        graph_dir: impl AsRef<Path> + Sync,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
    ) -> Result<Self, RAError> {
        let path = graph_dir.as_ref().to_path_buf();
        let inner = TemporalGraph::from_sorted_edge_list(
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            chunk_size,
            t_props_chunk_size,
            edge_list,
        )?;
        Ok(Self::new(inner, path))
    }

    pub fn load_from_dir(graph_dir: impl AsRef<Path>) -> Result<DiskGraph, RAError> {
        let path = graph_dir.as_ref().to_path_buf();
        let inner = TemporalGraph::new(graph_dir)?;
        Ok(Self::new(inner, path))
    }

    pub fn load_from_parquets<P: AsRef<Path>>(
        graph_dir: P,
        layer_parquet_cols: Vec<ParquetLayerCols>,
        node_properties: Option<P>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
        num_threads: usize,
        node_type_col: Option<&str>,
    ) -> Result<DiskGraph, RAError> {
        let layered_edge_list: Vec<ExternalEdgeList<&Path>> = layer_parquet_cols
            .iter()
            .map(
                |ParquetLayerCols {
                     parquet_dir,
                     layer,
                     src_col,
                     dst_col,
                     time_col,
                 }| {
                    ExternalEdgeList::new(layer, parquet_dir.as_ref(), src_col, dst_col, time_col)
                        .expect("Failed to load events")
                },
            )
            .collect::<Vec<_>>();

        let t_graph = TemporalGraph::from_parquets(
            num_threads,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            graph_dir.as_ref(),
            layered_edge_list,
            node_properties.as_ref().map(|p| p.as_ref()),
            node_type_col,
        )?;
        Ok(Self::new(t_graph, graph_dir.as_ref().to_path_buf()))
    }

    pub fn filtered_layers_par<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> impl ParallelIterator<Item = &'a TempColGraphFragment> + 'a {
        self.inner
            .layers()
            .par_iter()
            .enumerate()
            .filter(|(l_id, _)| layer_ids.contains(l_id))
            .map(|(_, layer)| layer)
    }

    pub fn filtered_layers_iter<'a>(
        &'a self,
        layer_ids: &'a LayerIds,
    ) -> impl Iterator<Item = &'a TempColGraphFragment> + 'a {
        self.inner
            .layers()
            .iter()
            .enumerate()
            .filter(|(l_id, _)| layer_ids.contains(l_id))
            .map(|(_, layer)| layer)
    }

    pub fn from_layer(layer: TempColGraphFragment) -> Self {
        let path = layer.graph_dir().to_path_buf();
        let global_ordering = layer.nodes_storage().gids().clone();

        let global_order = DiskHashMap::from_sorted_dedup(global_ordering.clone())
            .expect("Failed to create global order");

        let inner = TemporalGraph::new_from_layers(
            global_ordering,
            Arc::new(global_order),
            vec![layer],
            vec!["_default".to_string()],
        );
        Self::new(inner, path)
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::{
        arrow2::datatypes::{ArrowDataType as DataType, ArrowSchema as Schema},
        db::graph::graph::assert_graph_equal,
        prelude::*,
    };
    use itertools::Itertools;
    use polars_arrow::{
        array::{PrimitiveArray, StructArray},
        datatypes::Field,
    };
    use pometry_storage::{global_order::GlobalMap, graph_fragment::TempColGraphFragment, RAError};
    use proptest::{prelude::*, sample::size_range};
    use raphtory_api::core::{
        entities::{EID, VID},
        Direction,
    };
    use tempfile::TempDir;

    fn edges_sanity_node_list(edges: &[(u64, u64, i64)]) -> Vec<u64> {
        edges
            .iter()
            .map(|(s, _, _)| *s)
            .chain(edges.iter().map(|(_, d, _)| *d))
            .sorted()
            .dedup()
            .collect()
    }

    pub fn edges_sanity_check_build_graph<P: AsRef<Path>>(
        test_dir: P,
        edges: &[(u64, u64, i64)],
        nodes: &[u64],
        input_chunk_size: u64,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Result<TempColGraphFragment, RAError> {
        let chunks = edges
            .iter()
            .map(|(src, _, _)| *src)
            .chunks(input_chunk_size as usize);
        let srcs = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, dst, _)| *dst)
            .chunks(input_chunk_size as usize);
        let dsts = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));
        let chunks = edges
            .iter()
            .map(|(_, _, times)| *times)
            .chunks(input_chunk_size as usize);
        let times = chunks
            .into_iter()
            .map(|chunk| PrimitiveArray::from_vec(chunk.collect()));

        let schema = Schema::from(vec![
            Field::new("srcs", DataType::UInt64, false),
            Field::new("dsts", DataType::UInt64, false),
            Field::new("time", DataType::Int64, false),
        ]);

        let triples = srcs.zip(dsts).zip(times).map(move |((a, b), c)| {
            StructArray::new(
                DataType::Struct(schema.fields.clone()),
                vec![a.boxed(), b.boxed(), c.boxed()],
                None,
            )
        });

        let go: GlobalMap = nodes.iter().copied().collect();
        let node_gids = PrimitiveArray::from_slice(nodes).boxed();

        let mut graph = TempColGraphFragment::load_from_edge_list(
            test_dir.as_ref(),
            0,
            chunk_size,
            t_props_chunk_size,
            go.into(),
            node_gids,
            0,
            1,
            2,
            triples,
        )?;
        graph.build_node_additions(chunk_size)?;
        Ok(graph)
    }

    fn check_graph_sanity(edges: &[(u64, u64, i64)], nodes: &[u64], graph: &TempColGraphFragment) {
        let expected_graph = Graph::new();
        for (src, dst, t) in edges {
            expected_graph
                .add_edge(*t, *src, *dst, NO_PROPS, None)
                .unwrap();
        }

        let graph_dir = TempDir::new().unwrap();
        // check persist_as_disk_graph works
        let disk_graph_from_expected = expected_graph
            .persist_as_disk_graph(graph_dir.path())
            .unwrap();
        assert_graph_equal(&disk_graph_from_expected, &expected_graph);

        let actual_num_verts = nodes.len();
        let g_num_verts = graph.num_nodes();
        assert_eq!(actual_num_verts, g_num_verts);
        assert!(graph
            .all_edges_iter()
            .all(|e| e.src().0 < g_num_verts && e.dst().0 < g_num_verts));

        for v in 0..g_num_verts {
            let v = VID(v);
            assert!(graph
                .edges(v, Direction::OUT)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
            assert!(graph
                .edges(v, Direction::IN)
                .map(|(_, v)| v)
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
        }

        let exploded_edges: Vec<_> = graph
            .exploded_edges()
            .map(|e| (nodes[e.src().0], nodes[e.dst().0], e.timestamp()))
            .collect();
        assert_eq!(exploded_edges, edges);

        // check incoming edges
        for (v_id, g_id) in nodes.iter().enumerate() {
            let node = expected_graph.node(*g_id).unwrap();
            let mut expected_inbound = node.in_edges().id().map(|(v, _)| v).collect::<Vec<_>>();
            expected_inbound.sort();

            let actual_inbound = graph
                .edges(VID(v_id), Direction::IN)
                .map(|(_, v)| nodes[v.0])
                .collect::<Vec<_>>();

            assert_eq!(expected_inbound, actual_inbound);
        }

        let unique_edges = edges.iter().map(|(src, dst, _)| (*src, *dst)).dedup();

        for (e_id, (src, dst)) in unique_edges.enumerate() {
            let edge = graph.edge(EID(e_id));
            let VID(src_id) = edge.src();
            let VID(dst_id) = edge.dst();

            assert_eq!(nodes[src_id], src);
            assert_eq!(nodes[dst_id], dst);
        }
    }

    fn edges_sanity_check_inner(
        edges: Vec<(u64, u64, i64)>,
        input_chunk_size: u64,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) {
        let test_dir = TempDir::new().unwrap();
        let nodes = edges_sanity_node_list(&edges);
        match edges_sanity_check_build_graph(
            test_dir.path(),
            &edges,
            &nodes,
            input_chunk_size,
            chunk_size,
            t_props_chunk_size,
        ) {
            Ok(graph) => {
                // check graph is sane
                check_graph_sanity(&edges, &nodes, &graph);
                let node_gids = PrimitiveArray::from_slice(&nodes).boxed();

                // check that reloading from graph dir works
                let reloaded_graph =
                    TempColGraphFragment::new(test_dir.path(), true, 0, node_gids).unwrap();
                check_graph_sanity(&edges, &nodes, &reloaded_graph)
            }
            Err(RAError::NoEdgeLists | RAError::EmptyChunk) => assert!(edges.is_empty()),
            Err(error) => panic!("{}", error.to_string()),
        };
    }

    proptest! {
        #[test]
        fn edges_sanity_check(
            edges in any_with::<Vec<(u8, u8, Vec<i64>)>>(size_range(1..=100).lift()).prop_map(|v| {
                let mut v: Vec<(u64, u64, i64)> = v.into_iter().flat_map(|(src, dst, times)| {
                    let src = src as u64;
                    let dst = dst as u64;
                    times.into_iter().map(move |t| (src, dst, t))}).collect();
                v.sort();
                v}),
            input_chunk_size in 1..1024u64,
            chunk_size in 1..1024usize,
            t_props_chunk_size in 1..128usize
        ) {
            edges_sanity_check_inner(edges, input_chunk_size, chunk_size, t_props_chunk_size);
        }
    }

    #[test]
    fn edge_sanity_bad() {
        let edges = vec![
            (0, 85, -8744527736816607775),
            (0, 85, -8533859256444633783),
            (0, 85, -7949123054744509169),
            (0, 85, -7208573652910411733),
            (0, 85, -7004677070223473589),
            (0, 85, -6486844751834401685),
            (0, 85, -6420653301843451067),
            (0, 85, -6151481582745013767),
            (0, 85, -5577061971106014565),
            (0, 85, -5484794766797320810),
        ];
        edges_sanity_check_inner(edges, 3, 5, 6)
    }

    #[test]
    fn edge_sanity_more_bad() {
        let edges = vec![
            (1, 3, -8622734205120758463),
            (2, 0, -8064563587743129892),
            (2, 0, 0),
            (2, 0, 66718116),
            (2, 0, 733950369757766878),
            (2, 0, 2044789983495278802),
            (2, 0, 2403967656666566197),
            (2, 4, -9199293364914546702),
            (2, 4, -9104424882442202562),
            (2, 4, -8942117006530427874),
            (2, 4, -8805351871358148900),
            (2, 4, -8237347600058197888),
        ];
        edges_sanity_check_inner(edges, 3, 5, 6)
    }

    #[test]
    fn edges_sanity_chunk_1() {
        edges_sanity_check_inner(vec![(876787706323152993, 0, 0)], 1, 1, 1)
    }

    #[test]
    fn edges_sanity_chunk_2() {
        edges_sanity_check_inner(vec![(4, 3, 2), (4, 5, 0)], 2, 2, 2)
    }

    #[test]
    fn large_failing_edge_sanity_repeated() {
        let edges = vec![
            (0, 0, 0),
            (0, 1, 0),
            (0, 2, 0),
            (0, 3, 0),
            (0, 4, 0),
            (0, 5, 0),
            (0, 6, -30),
            (4, 7, -83),
            (4, 7, -77),
            (6, 8, -68),
            (6, 8, -65),
            (9, 10, 46),
            (9, 10, 46),
            (9, 10, 51),
            (9, 10, 54),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 59),
            (9, 10, 65),
            (9, 11, -75),
        ];
        let input_chunk_size = 411;
        let edge_chunk_size = 5;
        let edge_max_list_size = 7;

        edges_sanity_check_inner(edges, input_chunk_size, edge_chunk_size, edge_max_list_size);
    }

    #[test]
    fn edge_sanity_chunk_broken_incoming() {
        let edges = vec![
            (0, 0, 0),
            (0, 0, 0),
            (0, 0, 66),
            (0, 1, 0),
            (2, 0, 0),
            (3, 4, 0),
            (4, 0, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (4, 4, 0),
            (5, 0, 0),
            (6, 7, 7274856480798084567),
            (8, 3, -7707029126214574305),
        ];

        edges_sanity_check_inner(edges, 853, 122, 98)
    }

    #[test]
    fn edge_sanity_chunk_broken_something() {
        let edges = vec![(0, 3, 0), (1, 2, 0), (3, 2, 0)];
        edges_sanity_check_inner(edges, 1, 1, 1)
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
        let graph = disk_graph.inner.layer(0);

        let all_exploded: Vec<_> = graph
            .exploded_edges()
            .map(|e| (e.src(), e.dst(), e.timestamp()))
            .collect();
        let expected: Vec<_> = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(1), 1),
            (VID(0), VID(1), 2),
            (VID(1), VID(2), 3),
        ];
        assert_eq!(all_exploded, expected);

        let node_gids = PrimitiveArray::from_slice([0u64, 1, 2]).boxed();
        let reloaded_graph =
            TempColGraphFragment::new(graph.graph_dir(), true, 0, node_gids).unwrap();

        check_graph_sanity(
            &[(0, 1, 0), (0, 1, 1), (0, 1, 2), (1, 2, 3)],
            &[0, 1, 2],
            &reloaded_graph,
        );
    }

    mod addition_bounds {
        use itertools::Itertools;
        use proptest::{prelude::*, sample::size_range};
        use raphtory_api::core::entities::VID;
        use tempfile::TempDir;

        use super::{
            edges_sanity_check_build_graph, AdditionOps, Graph, GraphViewOps, NodeViewOps,
            TempColGraphFragment, NO_PROPS,
        };

        fn compare_raphtory_graph(edges: Vec<(u64, u64, i64)>, chunk_size: usize) {
            let nodes = edges
                .iter()
                .flat_map(|(src, dst, _)| [*src, *dst])
                .sorted()
                .dedup()
                .collect::<Vec<_>>();

            let rg = Graph::new();

            for (src, dst, time) in &edges {
                rg.add_edge(*time, *src, *dst, NO_PROPS, None)
                    .expect("failed to add edge");
            }

            let test_dir = TempDir::new().unwrap();
            let graph: TempColGraphFragment = edges_sanity_check_build_graph(
                test_dir.path(),
                &edges,
                &nodes,
                edges.len() as u64,
                chunk_size,
                chunk_size,
            )
            .unwrap();

            for (v_id, node) in nodes.into_iter().enumerate() {
                let node = rg.node(node).expect("failed to get node id");
                let expected = node.history();
                let node = graph.node(VID(v_id));
                let actual = node.timestamps().into_iter_t().collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }

        #[test]
        fn node_additions_bounds_to_arrays() {
            let edges = vec![(0, 0, -2), (0, 0, -1), (0, 0, 0), (0, 0, 1), (0, 0, 2)];

            compare_raphtory_graph(edges, 2);
        }

        #[test]
        fn test_load_from_graph_missing_edge() {
            let g = Graph::new();
            g.add_edge(0, 1, 2, [("test", "test1")], Some("1")).unwrap();
            g.add_edge(1, 2, 3, [("test", "test2")], Some("2")).unwrap();
            let test_dir = TempDir::new().unwrap();
            let _ = g.persist_as_disk_graph(test_dir.path()).unwrap();
        }

        #[test]
        fn one_edge_bounds_chunk_remainder() {
            let edges = vec![(0u64, 1, 0)];
            compare_raphtory_graph(edges, 3);
        }

        #[test]
        fn same_edge_twice() {
            let edges = vec![(0, 1, 0), (0, 1, 1)];
            compare_raphtory_graph(edges, 3);
        }

        proptest! {
            #[test]
            fn node_addition_bounds_test(
                edges in any_with::<Vec<(u8, u8, Vec<i64>)>>(size_range(1..=100).lift()).prop_map(|v| {
                    let mut v: Vec<(u64, u64, i64)> = v.into_iter().flat_map(|(src, dst, times)| {
                        let src = src as u64;
                        let dst = dst as u64;
                        times.into_iter().map(move |t| (src, dst, t))}).collect();
                    v.sort();
                    v}).prop_filter("edge list mut have one edge at least",|edges| !edges.is_empty()),
                chunk_size in 1..300usize,
            ) {
                compare_raphtory_graph(edges, chunk_size);
            }
        }
    }
}
