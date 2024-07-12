use crate::{
    core::{
        entities::{
            properties::{graph_meta::GraphMeta, props::Meta},
            LayerIds,
        },
        utils::errors::GraphError,
    },
    disk_graph::{graph_impl::prop_conversion::make_node_properties_from_graph, Error},
    prelude::{Graph, Layer},
};
use pometry_storage::{
    disk_hmap::DiskHashMap, graph::TemporalGraph, graph_fragment::TempColGraphFragment,
    load::ExternalEdgeList, RAError,
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use rayon::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
};
};
use crate::disk_graph::DiskGraphError;

mod edge_storage_ops;
mod interop;
pub mod prop_conversion;
mod time_index_into_ops;
pub mod tprops;

#[derive(Debug)]
pub struct ParquetLayerCols<'a> {
    pub parquet_dir: &'a str,
    pub layer: &'a str,
    pub src_col: &'a str,
    pub dst_col: &'a str,
    pub time_col: &'a str,
}

#[derive(Clone, Debug)]
pub struct DiskGraphStorage {
    pub(crate) inner: Arc<TemporalGraph>,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    graph_props: Arc<GraphMeta>,
    graph_dir: PathBuf,
}

impl Serialize for DiskGraphStorage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let path = self.graph_dir.clone();
        path.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DiskGraphStorage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let path = PathBuf::deserialize(deserializer)?;
        let graph_result = DiskGraphStorage::load_from_dir(&path).map_err(|err| {
            serde::de::Error::custom(format!("Failed to load Diskgraph: {:?}", err))
        })?;
        Ok(graph_result)
    }
}

impl Display for DiskGraphStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Diskgraph(num_nodes={}, num_temporal_edges={}",
            self.inner.num_nodes(),
            self.inner.count_temporal_edges()
        )
    }
}

impl AsRef<TemporalGraph> for DiskGraphStorage {
    fn as_ref(&self) -> &TemporalGraph {
        &self.inner
    }
}

impl Graph {
    pub fn persist_as_disk_graph(
        &self,
        graph_dir: impl AsRef<Path>,
    ) -> Result<DiskGraphStorage, DiskGraphError> {
        DiskGraphStorage::from_graph(self, graph_dir)
    }
}

fn get_valid_layers(graph: &Arc<TemporalGraph>) -> Vec<String> {
    graph.layer_names().into_iter().map(|x| x.clone()).collect()
}

impl DiskGraphStorage {
    pub fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All | Layer::Default => LayerIds::One(0), // FIXME: need to handle all correctly
            Layer::One(name) => {
                let name = name.as_ref();
                self.inner
                    .layer_names()
                    .iter()
                    .enumerate()
                    .find(move |(_, ref n)| n == &name)
                    .map(|(i, _)| LayerIds::One(i))
                    .unwrap_or(LayerIds::None)
            }
            _ => todo!("Layer ids for multiple names not implemented for Diskgraph"),
        }
    }

    pub fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match key {
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(name) => {
                let id = self.inner.find_layer_id(&name).ok_or_else(|| {
                    GraphError::invalid_layer(name.to_string(), get_valid_layers(&self.inner))
                })?;
                Ok(LayerIds::One(id))
            }
            Layer::None => Ok(LayerIds::None),
            Layer::Multiple(names) => {
                let ids = names
                    .iter()
                    .map(|name| {
                        self.inner.find_layer_id(name).ok_or_else(|| {
                            GraphError::invalid_layer(
                                name.to_string(),
                                get_valid_layers(&self.inner),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LayerIds::Multiple(ids.into()))
            }
        }
    }

    pub fn into_graph(self) -> Graph {
        Graph::from_internal_graph(&crate::db::api::storage::storage_ops::GraphStorage::Disk(
            Arc::new(self),
        ))
    }

    pub(crate) fn core_temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let layer_id = match layer_ids.constrain_from_edge(e) {
            LayerIds::One(id) => id,
            _ => panic!("Only one layer is supported"),
        };
        Box::new(1..self.inner.edges_data_type(layer_id).len())
    }

    pub fn layer_from_ids(&self, layer_ids: &LayerIds) -> Option<usize> {
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
    ) -> DiskGraphStorage {
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
        DiskGraphStorage::load_from_edge_lists(
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

    pub fn from_graph(graph: &Graph, graph_dir: impl AsRef<Path>) -> Result<Self, Error> {
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

    pub fn load_from_dir(graph_dir: impl AsRef<Path>) -> Result<DiskGraphStorage, RAError> {
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
    ) -> Result<DiskGraphStorage, RAError> {
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

    pub fn node_meta(&self) -> &Meta {
        &self.node_meta
    }

    pub fn edge_meta(&self) -> &Meta {
        &self.edge_meta
    }

    pub fn graph_meta(&self) -> &GraphMeta {
        &self.graph_props
    }
}

#[cfg(test)]
mod test {
    use super::{DiskGraphStorage, ParquetLayerCols};
    use crate::{
        db::api::{storage::storage_ops::GraphStorage, view::StaticGraphViewOps},
        disk_graph::Time,
        prelude::*,
    };
    use itertools::Itertools;
    use pometry_storage::{graph::TemporalGraph, properties::Properties};
    use proptest::{prelude::*, sample::size_range};
    use rayon::prelude::*;
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
    };
    use tempfile::TempDir;

    fn make_simple_graph(graph_dir: impl AsRef<Path>, edges: &[(u64, u64, i64, f64)]) -> Graph {
        let storage = DiskGraphStorage::make_simple_graph(graph_dir, edges, 1000, 1000);
        Graph::from_internal_graph(&GraphStorage::Disk(Arc::new(storage)))
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

        let out_v_deg = w_g.nodes().out_degree().values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![1, 0]);

        let w_g = g.window(-2, 0);
        let out_v_deg = w_g.nodes().out_degree().values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![2, 0]);

        let w_g = g.window(-2, 4);
        let out_v_deg = w_g.nodes().out_degree().values().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![4, 0, 0, 0]);

        let in_v_deg = w_g.nodes().in_degree().values().collect::<Vec<_>>();
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
        assert_eq!(disk_graph.num_edges(0), 1);
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
        node.add_constant_properties([
            ("const_str", "test_c".into_prop()),
            ("const_float", 0.314f64.into_prop()),
        ])
        .unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = DiskGraphStorage::from_graph(&mem_graph, test_dir.path())
            .unwrap()
            .into_graph();
        assert_eq!(disk_graph.count_nodes(), 1);
        let props = disk_graph.node(0).unwrap().properties();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(props.get("const_str").unwrap_str(), "test_c");
        assert_eq!(props.get("const_float").unwrap_f64(), 0.314);

        drop(disk_graph);

        let disk_graph = DiskGraphStorage::load_from_dir(test_dir.path())
            .unwrap()
            .into_graph();
        let props = disk_graph.node(0).unwrap().properties();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(props.get("const_str").unwrap_str(), "test_c");
        assert_eq!(props.get("const_float").unwrap_f64(), 0.314);
    }

    #[test]
    fn test_only_const_node_properties() {
        let g = Graph::new();
        let v = g.add_node(0, 1, NO_PROPS, None).unwrap();
        v.add_constant_properties([("test", "test")]).unwrap();
        let test_dir = TempDir::new().unwrap();
        let disk_graph = g
            .persist_as_disk_graph(test_dir.path())
            .unwrap()
            .into_graph();
        assert_eq!(
            disk_graph
                .node(1)
                .unwrap()
                .properties()
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
                .properties()
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
        let read_chunk_size = 4_000_000;
        let concurrent_files = 1;

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
            },
            ParquetLayerCols {
                parquet_dir: v1_layer_path.to_str().unwrap(),
                layer: "wls",
                src_col: "src",
                dst_col: "dst",
                time_col: "Time",
            },
        ];

        let node_type_col = Some("node_type");

        let g = DiskGraphStorage::load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            Some(&node_properties),
            chunk_size,
            t_props_chunk_size,
            Some(read_chunk_size as usize),
            Some(concurrent_files),
            num_threads,
            node_type_col,
        )
        .unwrap()
        .into_graph();

        println!("node types = {:?}", g.nodes().node_type().collect_vec());

        assert_eq!(
            g.nodes().type_filter(&vec!["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter(&vec![""]).name().collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["A"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["A", "B"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp244393"], vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["C"])
                .neighbours()
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            Vec::<Vec<&str>>::new()
        );

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["A"])
                .neighbours()
                .type_filter(&vec!["A"])
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec!["Comp844043"], vec!["Comp710070"]]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["A"])
                .neighbours()
                .type_filter(&Vec::<&str>::new())
                .name()
                .map(|n| { n.collect::<Vec<_>>() })
                .collect_vec(),
            vec![vec![], Vec::<&str>::new()]
        );

        let w = g.window(6415659, 7387801);

        assert_eq!(
            w.nodes().type_filter(&vec!["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            w.nodes()
                .type_filter(&Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            w.nodes().type_filter(&vec![""]).name().collect_vec(),
            Vec::<String>::new()
        );

        let l = g.layers(["netflow"]).unwrap();

        assert_eq!(
            l.nodes().type_filter(&vec!["A"]).name().collect_vec(),
            vec!["Comp710070", "Comp844043"]
        );

        assert_eq!(
            l.nodes()
                .type_filter(&Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            l.nodes().type_filter(&vec![""]).name().collect_vec(),
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
                .type_filter(&vec!["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter(&vec![""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );

        let g = DiskGraphStorage::load_from_dir(tmp_dir.path())
            .unwrap()
            .into_graph();

        assert_eq!(
            g.nodes()
                .type_filter(&vec!["a", "b", "c", "e"])
                .name()
                .collect_vec(),
            vec!["1", "2", "3", "4", "5", "6"]
        );

        assert_eq!(
            g.nodes()
                .type_filter(&Vec::<String>::new())
                .name()
                .collect_vec(),
            Vec::<String>::new()
        );

        assert_eq!(
            g.nodes().type_filter(&vec![""]).name().collect_vec(),
            vec!["7", "8", "9"]
        );
    }
}

#[cfg(feature = "storage")]
#[cfg(test)]
mod storage_tests {
    use crate::{
        core::Prop,
        db::graph::graph::assert_graph_equal,
        prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS, *},
    };
    use itertools::Itertools;
    use proptest::prelude::*;
    use raphtory_api::core::storage::arc_str::OptionAsStr;
    use std::collections::BTreeSet;
    use tempfile::TempDir;

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
            .add_constant_properties([("const_str", "test")])
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
            .add_constant_properties([("const_str2", "test2")])
            .unwrap();
        g2.node(0)
            .unwrap()
            .add_updates(3, [("test", "test")])
            .unwrap();
        let g1_dir = TempDir::new().unwrap();
        let g2_dir = TempDir::new().unwrap();
        let gm_dir = TempDir::new().unwrap();

        let g1_a = g1.persist_as_disk_graph(&g1_dir).unwrap();
        let g2_a = g2.persist_as_disk_graph(&g2_dir).unwrap();

        let gm = g1_a.merge_by_sorted_gids(&g2_a, &gm_dir).unwrap();

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
        assert_eq!(n1.properties().get("const_str"), Some(Prop::str("test")));
        assert_eq!(n1.properties().get("const_str2").unwrap_str(), "test2");
        assert!(n1
            .properties()
            .temporal()
            .values()
            .all(|prop| prop.values().is_empty()));
        let n2 = gm.node(2).unwrap();
        assert_eq!(n2.node_type().as_str(), Some("3")); // right has priority

        assert_eq!(
            gm.default_layer().edges().id().collect::<Vec<_>>(),
            [(0, 1), (1, 2)]
        );
        assert_eq!(
            gm.valid_layers("1").edges().id().collect::<Vec<_>>(),
            [(0, 1)]
        );
        assert_eq!(
            gm.valid_layers("2").edges().id().collect::<Vec<_>>(),
            [(0, 1)]
        );
    }

    fn add_edges(g: &Graph, edges: &[(i64, u64, u64)]) {
        let nodes: BTreeSet<_> = edges
            .iter()
            .flat_map(|(_, src, dst)| [*src, *dst])
            .collect();
        for n in nodes {
            g.add_node(0, n, NO_PROPS, None).unwrap();
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

        let left_g_disk = left_g.persist_as_disk_graph(&left_dir).unwrap();
        let right_g_disk = right_g.persist_as_disk_graph(&right_dir).unwrap();

        let merged_g_disk = left_g_disk
            .merge_by_sorted_gids(&right_g_disk, &merged_dir)
            .unwrap();
        assert_graph_equal(&merged_g_disk, &merged_g_expected)
    }

    #[test]
    fn test_merge_proptest() {
        proptest!(|(left_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100), right_edges in prop::collection::vec((0i64..10, 0u64..10, 0u64..10), 0..=100))| {
            inner_merge_test(&left_edges, &right_edges)
        })
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
