use arrow2::array::StructArray;

use crate::{
    core::entities::properties::graph_props::GraphProps,
    db::api::view::{DynamicGraph, IntoDynamic},
};
use std::{num::NonZeroUsize, ops::Deref, path::Path, sync::Arc};

use crate::core::entities::properties::props::Meta;

use super::{graph::TemporalGraph, loader::ExternalEdgeList, Error};

pub mod const_properties_ops;
pub mod core_ops;
pub mod edge_filter_ops;
pub mod graph_ops;
pub mod layer_ops;
pub mod materialize;
mod prop_conversion;
pub mod temporal_properties_ops;
pub mod time_semantics;
pub mod tprops;

#[derive(Clone)]
pub struct Graph2 {
    inner: Arc<TemporalGraph>,
    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    graph_props: Arc<GraphProps>,
}

impl IntoDynamic for Graph2 {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl Deref for Graph2 {
    type Target = TemporalGraph;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Graph2 {
    // take the datatype from the struct array of the edge properties and fill in the edge_meta
    fn init_meta(&mut self) {
        let edge_props_fields = self.edges_data_type(0); // layer 0 for now
                                                         // let layer_id = self.edge_meta.get_or_create_layer_id("default");
                                                         // assert_eq!(layer_id, 0);

        for field in edge_props_fields {
            let prop_name = &field.name;
            let data_type = field.data_type();

            self.edge_meta
                .resolve_prop_id(prop_name, data_type.into(), false)
                .expect("Arrow data types should without failing");
        }
    }

    pub fn from_edge_lists(
        edge_lists: &[StructArray],
        num_threads: NonZeroUsize,

        node_chunk_size: usize,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,

        graph_dir: impl AsRef<Path> + Sync,

        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
    ) -> Result<Self, Error> {
        let inner = TemporalGraph::from_sorted_edge_list(
            graph_dir,
            num_threads,
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            node_chunk_size,
            edge_chunk_size,
            t_props_chunk_size,
            || edge_lists.iter(),
        )?;
        let node_meta = Arc::new(Meta::new());
        let edge_meta = Arc::new(Meta::new());
        let mut grapho = Self {
            inner: Arc::new(inner),
            node_meta,
            edge_meta,
            graph_props: Arc::new(GraphProps::new()),
        };
        grapho.init_meta();
        Ok(grapho)
    }

    pub fn open_path(path: impl AsRef<Path>) -> Result<Graph2, Error> {
        let inner = TemporalGraph::new(path)?;
        let node_meta = Arc::new(Meta::new());
        let edge_meta = Arc::new(Meta::new());
        let mut grapho = Self {
            inner: Arc::new(inner),
            node_meta,
            edge_meta,
            graph_props: Arc::new(GraphProps::new()),
        };
        grapho.init_meta();
        Ok(grapho)
    }

    pub fn load_from_dir(
        graph_dir: impl AsRef<Path>,
        parquet_dir: impl AsRef<Path>,
        src_col: &str,
        src_hash_col: &str,
        dst_col: &str,
        dst_hash_col: &str,
        time_col: &str,
        node_chunk_size: usize,
        edge_chunk_size: usize,
        t_props_chunk_size: usize,
        read_chunk_size: Option<usize>,
        concurrent_files: Option<usize>,
    ) -> Result<Graph2, Error> {
        let edge_list = ExternalEdgeList::new(
            "default",
            parquet_dir.as_ref(),
            src_col,
            src_hash_col,
            dst_col,
            dst_hash_col,
            time_col,
        )?;
        let t_graph = TemporalGraph::from_edge_lists(
            32,
            node_chunk_size,
            edge_chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            graph_dir.as_ref(),
            [edge_list],
        )?;

        let mut grapho = Graph2 {
            inner: Arc::new(t_graph),
            node_meta: Arc::new(Meta::new()),
            edge_meta: Arc::new(Meta::new()),
            graph_props: Arc::new(GraphProps::new()),
        };
        grapho.init_meta();

        Ok(grapho)
    }
}

#[cfg(test)]
mod test {
    use std::{cmp::Reverse, iter::once, path::Path};

    use arrow2::{
        array::PrimitiveArray,
        datatypes::{DataType, Field},
    };
    use itertools::{chain, Itertools};

    use crate::{
        algorithms::components::weakly_connected_components, arrow::Time,
        db::api::view::StaticGraphViewOps, prelude::*,
    };

    use proptest::{prelude::*, sample::size_range};

    use super::Graph2;

    fn make_simple_graph(graph_dir: impl AsRef<Path>, edges: &[(u64, u64, Time, f64)]) -> Graph2 {
        // unzip into 4 vectors
        let (src, (dst, (time, weight))): (Vec<_>, (Vec<_>, (Vec<_>, Vec<_>))) = edges
            .into_iter()
            .map(|(a, b, c, d)| (*a, (*b, (*c, *d))))
            .unzip();

        let edge_lists = vec![arrow2::array::StructArray::new(
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
        Graph2::from_edge_lists(
            &edge_lists,
            std::num::NonZeroUsize::new(1).unwrap(),
            1000,
            1000,
            1000,
            graph_dir.as_ref(),
            0,
            1,
            2,
        )
        .expect("failed to create graph")
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

        assert!(edges
            .iter()
            .all(|(src, dst, _, _)| g.has_edge(*src, *dst, Layer::All)));

        // check earlies_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).min().unwrap();
        assert_eq!(g.earliest_time(), Some(expected));

        // check latest_time
        let expected = edges.iter().map(|(_, _, t, _)| *t).max().unwrap();
        assert_eq!(g.latest_time(), Some(expected));

        // get edges over window

        let g = g.window(i64::MIN, i64::MAX).layer(Layer::Default).unwrap();

        // get edges back from full windows with all layers
        assert!(edges
            .iter()
            .all(|(src, dst, _, _)| g.edge(*src, *dst).is_some()));

        assert!(edges
            .iter()
            .all(|(src, dst, _, _)| g.has_edge(*src, *dst, Layer::All)));

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

        let actual = w_g.edges().count();
        let expected = 1;
        assert_eq!(actual, expected);

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
        fn connected_components_smallest_values(vs in any_with::<Vec<u64>>(size_range(1..=1000).lift())){ connected_components_check(vs) }
    }
}
