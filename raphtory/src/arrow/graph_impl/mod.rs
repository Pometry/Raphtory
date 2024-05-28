use std::{
    fmt::{Display, Formatter},
    path::Path,
    sync::Arc,
};

use raphtory_arrow::{
    arrow_hmap::ArrowHashMap, graph::TemporalGraph, graph_fragment::TempColGraphFragment,
    load::ExternalEdgeList, RAError,
};
use rayon::prelude::*;

use crate::{
    arrow::{graph_impl::prop_conversion::make_node_properties_from_graph, Error},
    arrow2::{
        array::{PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
    },
    core::entities::{
        properties::{graph_meta::GraphMeta, props::Meta},
        LayerIds,
    },
    db::api::view::{internal::Immutable, DynamicGraph, IntoDynamic},
    prelude::{Graph, GraphViewOps},
};

pub mod const_properties_ops;
pub mod core_ops;
pub mod edge_filter_ops;
mod edge_storage_ops;
mod interop;
pub mod layer_ops;
mod list_ops;
pub mod materialize;
mod node_filter_ops;
pub mod prop_conversion;
pub mod temporal_properties_ops;
mod time_index_into_ops;
pub mod time_semantics;
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
pub struct ArrowGraph {
    pub(crate) inner: Arc<TemporalGraph>,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    graph_props: Arc<GraphMeta>,
}

impl Display for ArrowGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArrowGraph(num_nodes={}, num_temporal_edges={}",
            self.count_nodes(),
            self.count_temporal_edges()
        )
    }
}

impl AsRef<TemporalGraph> for ArrowGraph {
    fn as_ref(&self) -> &TemporalGraph {
        &self.inner
    }
}

impl Graph {
    pub fn persist_as_arrow(&self, graph_dir: impl AsRef<Path>) -> Result<ArrowGraph, Error> {
        ArrowGraph::from_graph(self, graph_dir)
    }
}

impl Immutable for ArrowGraph {}

impl IntoDynamic for ArrowGraph {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl ArrowGraph {
    pub fn layer_from_ids(&self, layer_ids: &LayerIds) -> Option<usize> {
        match layer_ids {
            LayerIds::One(layer_id) => Some(*layer_id),
            LayerIds::None => None,
            LayerIds::All => match self.inner.layers().len() {
                0 => None,
                1 => Some(0),
                _ => todo!("multilayer edge views not yet supported in arrow"),
            },
            LayerIds::Multiple(ids) => match ids.len() {
                0 => None,
                1 => Some(ids[0]),
                _ => todo!("multilayer edge views not yet supported in arrow"),
            },
        }
    }

    pub fn make_simple_graph(
        graph_dir: impl AsRef<Path>,
        edges: &[(u64, u64, i64, f64)],
        chunk_size: usize,
        t_props_chunk_size: usize,
    ) -> ArrowGraph {
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
        ArrowGraph::load_from_edge_lists(
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

    fn new(inner_graph: TemporalGraph) -> Self {
        let node_meta = Meta::new();
        let mut edge_meta = Meta::new();
        let graph_meta = GraphMeta::new();

        for layer in inner_graph.layers() {
            let edge_props_fields = layer.edges_data_type();

            for (id, field) in edge_props_fields.iter().enumerate() {
                let prop_name = &field.name;
                let data_type = field.data_type();

                let resolved_id = edge_meta
                    .resolve_prop_id(prop_name, data_type.into(), false)
                    .expect("Arrow data types should without failing");
                if id != resolved_id {
                    println!("Warning: Layers with different edge properties are not supported by the high-level apis on top of the arrow graph yet, edge properties will not be available to high-level apis");
                    edge_meta = Meta::new();
                    break;
                }
            }
        }

        for l_name in inner_graph.layer_names() {
            edge_meta.layer_meta().get_or_create_id(l_name);
        }

        if let Some(props) = inner_graph.node_properties().as_ref() {
            let node_const_props_fields = props.const_props.prop_dtypes();
            for field in node_const_props_fields {
                node_meta
                    .resolve_prop_id(&field.name, field.data_type().into(), true)
                    .expect("Initial resolve should not fail");
            }

            let node_temporal_props_fields = props.temporal_props.prop_dtypes();
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
        }
    }

    pub fn from_graph(graph: &Graph, graph_dir: impl AsRef<Path>) -> Result<Self, Error> {
        let inner_graph = TemporalGraph::from_graph(graph, graph_dir.as_ref(), || {
            make_node_properties_from_graph(graph, graph_dir.as_ref())
        })?;
        Ok(Self::new(inner_graph))
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

    pub fn load_from_dir(graph_dir: impl AsRef<Path>) -> Result<ArrowGraph, RAError> {
        let inner = TemporalGraph::new(graph_dir)?;
        Ok(Self::new(inner))
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
    ) -> Result<ArrowGraph, RAError> {
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

        let t_graph = TemporalGraph::from_edge_lists(
            num_threads,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            graph_dir.as_ref(),
            layered_edge_list,
            node_properties.as_ref().map(|p| p.as_ref()),
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

    pub fn from_layer(layer: TempColGraphFragment) -> Self {
        let global_ordering = layer.nodes_storage().gids().clone();

        let global_order = ArrowHashMap::from_sorted_dedup(global_ordering.clone())
            .expect("Failed to create global order");

        let inner = TemporalGraph::new_from_layers(
            global_ordering,
            Arc::new(global_order),
            vec![layer],
            vec!["_default".to_string()],
        );
        Self::new(inner)
    }
}

#[cfg(test)]
mod test {
    use std::{cmp::Reverse, iter::once, path::Path};

    use itertools::{chain, Itertools};
    use proptest::{prelude::*, sample::size_range};
    use raphtory_arrow::graph::TemporalGraph;
    use rayon::prelude::*;
    use tempfile::TempDir;

    use crate::{
        algorithms::components::weakly_connected_components, arrow::Time,
        db::api::view::StaticGraphViewOps, prelude::*,
    };

    use super::ArrowGraph;

    fn make_simple_graph(
        graph_dir: impl AsRef<Path>,
        edges: &[(u64, u64, i64, f64)],
    ) -> ArrowGraph {
        ArrowGraph::make_simple_graph(graph_dir, edges, 1000, 1000)
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

        let out_v_deg = w_g.nodes().out_degree().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![1, 0]);

        let w_g = g.window(-2, 0);
        let out_v_deg = w_g.nodes().out_degree().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![2, 0]);

        let w_g = g.window(-2, 4);
        let out_v_deg = w_g.nodes().out_degree().collect::<Vec<_>>();
        assert_eq!(out_v_deg, vec![4, 0, 0, 0]);

        let in_v_deg = w_g.nodes().in_degree().collect::<Vec<_>>();
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

    fn connected_components_check(vs: Vec<u64>) {
        let vs = vs.into_iter().unique().collect::<Vec<u64>>();

        let smallest = vs.iter().min().unwrap();

        let first = vs[0];

        // pairs of nodes from vs one after the next
        let mut edges = vs
            .iter()
            .zip(chain!(vs.iter().skip(1), once(&first)))
            .enumerate()
            .map(|(t, (a, b))| (*a, *b, t as i64, 1f64))
            .collect::<Vec<(u64, u64, i64, f64)>>();

        edges.sort_by_key(|(src, dst, t, _)| (*src, *dst, *t));

        let test_dir = tempfile::tempdir().unwrap();
        let graph = make_simple_graph(test_dir, &edges);

        let res = weakly_connected_components(&graph, usize::MAX, None).group_by();

        let actual = res
            .into_iter()
            .map(|(cc, group)| (cc, Reverse(group.len())))
            .sorted_by(|l, r| l.1.cmp(&r.1))
            .map(|(cc, count)| (cc, count.0))
            .take(1)
            .next()
            .unwrap();

        assert_eq!(actual, (*smallest, edges.len()));
    }

    #[test]
    fn cc_smallest_in_2_edges() {
        let vs = vec![9616798649147808099, 0];
        connected_components_check(vs);
    }

    proptest! {
        #[test]
        fn connected_components_smallest_values(vs in any_with::<Vec<u64>>(size_range(1..=100).lift())){ connected_components_check(vs) }
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
    fn test_mem_to_arrow_graph() {
        let mem_graph = Graph::new();
        mem_graph.add_edge(0, 0, 1, [("test", 0u64)], None).unwrap();
        let test_dir = TempDir::new().unwrap();
        let arrow_graph =
            TemporalGraph::from_graph(&mem_graph, test_dir.path(), || Ok(None)).unwrap();
        assert_eq!(arrow_graph.num_nodes(), 2);
        assert_eq!(arrow_graph.num_edges(0), 1);
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
        let arrow_graph = ArrowGraph::from_graph(&mem_graph, test_dir.path()).unwrap();
        assert_eq!(arrow_graph.count_nodes(), 1);
        let props = arrow_graph.node(0).unwrap().properties();
        assert_eq!(props.get("test_num").unwrap_u64(), 0);
        assert_eq!(props.get("test_str").unwrap_str(), "test");
        assert_eq!(props.get("const_str").unwrap_str(), "test_c");
        assert_eq!(props.get("const_float").unwrap_f64(), 0.314);

        drop(arrow_graph);

        let arrow_graph = ArrowGraph::load_from_dir(test_dir.path()).unwrap();
        let props = arrow_graph.node(0).unwrap().properties();
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
        let arrow_graph = g.persist_as_arrow(test_dir.path()).unwrap();
        assert_eq!(
            arrow_graph
                .node(1)
                .unwrap()
                .properties()
                .get("test")
                .unwrap_str(),
            "test"
        );
        let arrow_graph = ArrowGraph::load_from_dir(test_dir.path()).unwrap();
        assert_eq!(
            arrow_graph
                .node(1)
                .unwrap()
                .properties()
                .get("test")
                .unwrap_str(),
            "test"
        );
    }
}
