use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    core::{
        entities::{
            properties::{graph_meta::GraphMeta, props::Meta},
            LayerIds,
        },
        utils::{errors::GraphError, iter::GenLockedIter},
    },
    db::{
        api::{storage::graph::storage_ops, view::internal::CoreGraphOps},
        graph::views::deletion_graph::PersistentGraph,
    },
    disk_graph::graph_impl::{prop_conversion::make_node_properties_from_graph, ParquetLayerCols},
    prelude::{Graph, Layer},
};
use itertools::Itertools;
use polars_arrow::{
    array::{PrimitiveArray, StructArray},
    datatypes::{ArrowDataType as DataType, Field},
};
use pometry_storage::{
    graph::TemporalGraph, graph_fragment::TempColGraphFragment, load::ExternalEdgeList,
    merge::merge_graph::merge_graphs, RAError,
};
use raphtory_api::core::entities::edges::edge_ref::EdgeRef;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod graph_impl;
pub mod storage_interface;

pub type Time = i64;

pub mod prelude {
    pub use pometry_storage::chunked_array::array_ops::*;
}

pub use pometry_storage as disk_storage;

#[derive(Clone, Debug)]
pub struct DiskGraphStorage {
    pub(crate) inner: Arc<TemporalGraph>,
    graph_props: Arc<GraphMeta>,
}

impl Serialize for DiskGraphStorage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let path = self.graph_dir();
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

impl DiskGraphStorage {
    pub fn inner(&self) -> &Arc<TemporalGraph> {
        &self.inner
    }

    pub fn graph_dir(&self) -> &Path {
        self.inner.graph_dir()
    }

    pub fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All => LayerIds::All,
            Layer::Default => LayerIds::One(0),
            Layer::One(name) => self
                .inner
                .find_layer_id(&name)
                .map(|id| LayerIds::One(id))
                .unwrap_or(LayerIds::None),
            Layer::None => LayerIds::None,
            Layer::Multiple(names) => {
                let mut new_layers = names
                    .iter()
                    .filter_map(|name| self.inner.find_layer_id(name))
                    .collect::<Vec<_>>();

                let num_layers = self.inner.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    LayerIds::None
                } else if num_new_layers == 1 {
                    LayerIds::One(new_layers[0])
                } else if num_new_layers == num_layers {
                    LayerIds::All
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    LayerIds::Multiple(new_layers.into())
                }
            }
        }
    }

    pub fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match key {
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(name) => {
                let id = self.inner.find_layer_id(&name).ok_or_else(|| {
                    GraphError::invalid_layer(name.to_string(), self.inner.get_valid_layers())
                })?;
                Ok(LayerIds::One(id))
            }
            Layer::None => Ok(LayerIds::None),
            Layer::Multiple(names) => {
                let mut new_layers = names
                    .iter()
                    .map(|name| {
                        self.inner.find_layer_id(name).ok_or_else(|| {
                            GraphError::invalid_layer(
                                name.to_string(),
                                self.inner.get_valid_layers(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let num_layers = self.inner.num_layers();
                let num_new_layers = new_layers.len();
                if num_new_layers == 0 {
                    Ok(LayerIds::None)
                } else if num_new_layers == 1 {
                    Ok(LayerIds::One(new_layers[0]))
                } else if num_new_layers == num_layers {
                    Ok(LayerIds::All)
                } else {
                    new_layers.sort_unstable();
                    new_layers.dedup();
                    Ok(LayerIds::Multiple(new_layers.into()))
                }
            }
        }
    }

    pub fn into_graph(self) -> Graph {
        Graph::from_internal_graph(storage_ops::GraphStorage::Disk(Arc::new(self)))
    }

    pub fn into_persistent_graph(self) -> PersistentGraph {
        PersistentGraph::from_internal_graph(storage_ops::GraphStorage::Disk(Arc::new(self)))
    }

    pub(crate) fn core_temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: &LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        match layer_ids.constrain_from_edge(e).into_owned() {
            LayerIds::None => Box::new(std::iter::empty()),
            LayerIds::All => Box::new(0..self.edge_meta().temporal_prop_meta().len()),
            LayerIds::One(id) => Box::new(self.inner().edge_global_mapping(id)),
            LayerIds::Multiple(arc) => Box::new(GenLockedIter::from((self, arc), |(dg, arc)| {
                Box::new(
                    arc.iter()
                        .map(|layer_id| dg.inner().edge_global_mapping(*layer_id))
                        .kmerge()
                        .dedup(),
                )
            })),
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
            2,
            0,
            1,
        )
        .expect("failed to create graph")
    }

    /// Merge this graph with another `DiskGraph`. Note that both graphs should have nodes that are
    /// sorted by their global ids or the resulting graph will be nonsense!
    pub fn merge_by_sorted_gids(
        &self,
        other: &DiskGraphStorage,
        new_graph_dir: impl AsRef<Path>,
    ) -> Result<DiskGraphStorage, GraphError> {
        let graph_dir = new_graph_dir.as_ref();
        let inner = merge_graphs(graph_dir, &self.inner, &other.inner)?;
        Ok(DiskGraphStorage::new(inner))
    }

    pub fn new(inner_graph: TemporalGraph) -> Self {
        let graph_meta = GraphMeta::new();

        Self {
            inner: Arc::new(inner_graph),
            graph_props: Arc::new(graph_meta),
        }
    }

    pub fn from_graph(graph: &Graph, graph_dir: impl AsRef<Path>) -> Result<Self, GraphError> {
        let inner_graph = TemporalGraph::from_graph(graph, graph_dir.as_ref(), || {
            make_node_properties_from_graph(graph, graph_dir.as_ref())
        })?;
        let mut storage = Self::new(inner_graph);
        storage.graph_props = Arc::new(graph.graph_meta().deep_clone());
        Ok(storage)
    }

    pub fn load_from_edge_lists(
        edge_list: &[StructArray],
        chunk_size: usize,
        t_props_chunk_size: usize,
        graph_dir: impl AsRef<Path> + Sync,
        time_col_idx: usize,
        src_col_idx: usize,
        dst_col_idx: usize,
    ) -> Result<Self, RAError> {
        let inner = TemporalGraph::from_sorted_edge_list(
            graph_dir,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            chunk_size,
            t_props_chunk_size,
            edge_list,
        )?;
        Ok(Self::new(inner))
    }

    pub fn load_from_dir(graph_dir: impl AsRef<Path>) -> Result<DiskGraphStorage, RAError> {
        let inner = TemporalGraph::new(graph_dir)?;
        Ok(Self::new(inner))
    }

    pub fn load_from_parquets<P: AsRef<Path>>(
        graph_dir: P,
        layer_parquet_cols: Vec<ParquetLayerCols>,
        node_properties: Option<P>,
        chunk_size: usize,
        t_props_chunk_size: usize,
        num_threads: usize,
        node_type_col: Option<&str>,
    ) -> Result<DiskGraphStorage, RAError> {
        let edge_lists: Vec<ExternalEdgeList<&Path>> = layer_parquet_cols
            .into_iter()
            .map(
                |ParquetLayerCols {
                     parquet_dir,
                     layer,
                     src_col,
                     dst_col,
                     time_col,
                     exclude_edge_props,
                 }| {
                    ExternalEdgeList::new(
                        layer,
                        parquet_dir.as_ref(),
                        src_col,
                        dst_col,
                        time_col,
                        exclude_edge_props,
                    )
                    .expect("Failed to load events")
                },
            )
            .collect::<Vec<_>>();

        let t_graph = TemporalGraph::from_parquets(
            num_threads,
            chunk_size,
            t_props_chunk_size,
            graph_dir.as_ref(),
            edge_lists,
            &[],
            node_properties.as_ref().map(|p| p.as_ref()),
            node_type_col,
        )?;
        Ok(Self::new(t_graph))
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

    pub fn node_meta(&self) -> &Meta {
        self.inner.node_meta()
    }

    pub fn edge_meta(&self) -> &Meta {
        self.inner.edge_meta()
    }

    pub fn graph_meta(&self) -> &GraphMeta {
        &self.graph_props
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use itertools::Itertools;
    use polars_arrow::{
        array::{PrimitiveArray, StructArray},
        datatypes::Field,
    };
    use proptest::{prelude::*, sample::size_range};
    use tempfile::TempDir;

    use pometry_storage::{graph::TemporalGraph, RAError};
    use raphtory_api::core::entities::{EID, VID};

    use crate::{
        arrow2::datatypes::{ArrowDataType as DataType, ArrowSchema as Schema},
        db::graph::graph::assert_graph_equal,
        prelude::*,
    };

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
        input_chunk_size: u64,
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> Result<TemporalGraph, RAError> {
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

        let triples = srcs
            .zip(dsts)
            .zip(times)
            .map(move |((a, b), c)| {
                StructArray::new(
                    DataType::Struct(schema.fields.clone()),
                    vec![a.boxed(), b.boxed(), c.boxed()],
                    None,
                )
            })
            .collect::<Vec<_>>();

        TemporalGraph::from_sorted_edge_list(
            test_dir.as_ref(),
            0,
            1,
            2,
            chunk_size,
            t_props_chunk_size,
            &triples,
        )
    }

    fn check_graph_sanity(edges: &[(u64, u64, i64)], nodes: &[u64], graph: &TemporalGraph) {
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
            .unwrap()
            .into_graph();
        assert_graph_equal(&disk_graph_from_expected, &expected_graph);

        let actual_num_verts = nodes.len();
        let g_num_verts = graph.num_nodes();
        assert_eq!(actual_num_verts, g_num_verts);
        assert!(graph
            .edges_iter()
            .map(|edge| (edge.src_id(), edge.dst_id()))
            .all(|(VID(src), VID(dst))| src < g_num_verts && dst < g_num_verts));

        for v in 0..g_num_verts {
            let v = VID(v);
            assert!(graph
                .node(v, 0)
                .out_neighbours()
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
            assert!(graph
                .node(v, 0)
                .in_neighbours()
                .tuple_windows()
                .all(|(v1, v2)| v1 <= v2));
        }

        let exploded_edges: Vec<_> = graph
            .exploded_edges()
            .map(|(src, dst, time)| (nodes[src.0], nodes[dst.0], time))
            .collect();
        assert_eq!(exploded_edges, edges);

        // check incoming edges
        for (v_id, g_id) in nodes.iter().enumerate() {
            let node = expected_graph.node(*g_id).unwrap();
            let mut expected_inbound = node.in_edges().id().map(|(v, _)| v).collect::<Vec<_>>();
            expected_inbound.sort();

            let actual_inbound = graph
                .node(VID(v_id), 0)
                .in_neighbours()
                .map(|v| GID::U64(nodes[v.0]))
                .collect::<Vec<_>>();

            assert_eq!(expected_inbound, actual_inbound);
        }

        let unique_edges = edges.iter().map(|(src, dst, _)| (*src, *dst)).dedup();

        for (e_id, (src, dst)) in unique_edges.enumerate() {
            let edge = graph.edge(EID(e_id));
            let VID(src_id) = edge.src_id();
            let VID(dst_id) = edge.dst_id();

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
            input_chunk_size,
            chunk_size,
            t_props_chunk_size,
        ) {
            Ok(graph) => {
                // check graph is sane
                check_graph_sanity(&edges, &nodes, &graph);

                // check that reloading from graph dir works
                let reloaded_graph = TemporalGraph::new(&test_dir).unwrap();
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
    fn edge_sanity_fail1() {
        let edges = vec![(0, 17, 0), (1, 0, -1), (17, 0, 0)];
        edges_sanity_check_inner(edges, 4, 4, 4)
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
        edges_sanity_check_inner(edges, 3, 5, 12)
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
        let graph = disk_graph.inner;

        let all_exploded: Vec<_> = graph.exploded_edges().collect();
        let expected: Vec<_> = vec![
            (VID(0), VID(1), 0),
            (VID(0), VID(1), 1),
            (VID(0), VID(1), 2),
            (VID(1), VID(2), 3),
        ];
        assert_eq!(all_exploded, expected);

        let reloaded_graph = TemporalGraph::new(graph.graph_dir()).unwrap();

        check_graph_sanity(
            &[(0, 1, 0), (0, 1, 1), (0, 1, 2), (1, 2, 3)],
            &[0, 1, 2],
            &reloaded_graph,
        );
    }

    mod addition_bounds {
        use itertools::Itertools;
        use proptest::{prelude::*, sample::size_range};
        use tempfile::TempDir;

        use raphtory_api::core::entities::VID;

        use super::{
            edges_sanity_check_build_graph, AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS,
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
            let graph = edges_sanity_check_build_graph(
                test_dir.path(),
                &edges,
                edges.len() as u64,
                chunk_size,
                chunk_size,
            )
            .unwrap();

            for (v_id, node) in nodes.into_iter().enumerate() {
                let node = rg.node(node).expect("failed to get node id");
                let expected = node.history();
                let node = graph.node(VID(v_id), 0);
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
