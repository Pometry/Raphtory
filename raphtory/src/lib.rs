//! # raphtory
//!
//! `raphtory` is a Rust library for analysing time-based graph data.
//! It is designed to be horizontally scalable,and can be used for a variety of applications
//! such as social network, cyber security, fraud analysis and more.
//!
//! The core feature of raphtory is the ability to analyse time-based graph data.
//!
//! You can run periodic graph analytics on your graph, and see how the graph changes over time.
//!
//! For example:
//!
//! - Run a PageRank algorithm on your graph every 5 minutes, and see how the PageRank scores change.
//! - View the graph a previous point in time, to see how the graph looked.
//!
//!
//! ## Features
//!
//! - **Time-based Graphs** - raphtory allows you to create and analyse time-based graphs.
//! - **Graph Analytics** - raphtory provides a variety of graph analytics algorithms.
//! - **Horizontal Scalability** - raphtory is designed to be horizontally scalable.
//! - **Distributed** - raphtory can be distributed across multiple machines.
//! - **Fast** - raphtory is fast, and can process large amounts of data in a short amount of time.
//! - **Open Source** - raphtory is open source, and is available on Github under a GPL-3.0 license.
//!
//! ## Example
//!
//! Create your own graph below
//! ```
//! use raphtory::prelude::*;
//!
//! // Create your GraphDB object and state the number of shards you would like, here we have 2
//! let graph = Graph::new();
//!
//! // Add node and edges to your graph with the respective properties
//! graph.add_node(
//!   1,
//!   "Gandalf",
//!   [("type", Prop::str("Character"))],
//!   None
//! ).unwrap();
//!
//! graph.add_node(
//!   2,
//!   "Frodo",
//!   [("type", Prop::str("Character"))],
//!   None,
//! ).unwrap();
//!
//! graph.add_edge(
//!   3,
//!   "Gandalf",
//!   "Frodo",
//!   [(
//!       "meeting",
//!       Prop::str("Character Co-occurrence"),
//!   )],
//!   None,
//! ).unwrap();
//!
//! // Get the in-degree, out-degree and degree of Gandalf
//! println!("Number of nodes {:?}", graph.count_nodes());
//! println!("Number of Edges {:?}", graph.count_edges());
//! ```
//!
//! ## Supported Operating Systems
//! This library requires Rust 1.54 or later.
//!
//! The following operating systems are supported:
//!
//! - `Linux`
//! - `macOS`
//! - `Windows`
//!
//! ## License
//!
//! This project is licensed under the terms of the GPL-3.0 license.
//! Please see the Github repository for more information.
//!
//! ## Contributing
//!
//! raphtory is created by [Pometry](https://pometry.com).
//! We are always looking for contributors to help us improve the library.
//! If you are interested in contributing, please see
//! our [GitHub repository](https://github.com/Raphtory/raphtory)
pub mod algorithms;
pub mod core;
pub mod db;
pub mod graphgen;

#[cfg(feature = "storage")]
pub mod disk_graph;

#[cfg(all(feature = "python", not(doctest)))]
// no doctests in python as the docstrings are python not rust format
pub mod python;

#[cfg(feature = "io")]
pub mod graph_loader;

#[cfg(feature = "search")]
pub mod search;

#[cfg(feature = "vectors")]
pub mod vectors;

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "proto")]
pub mod serialise;

pub mod prelude {
    pub const NO_PROPS: [(&str, Prop); 0] = [];
    pub use crate::{
        core::{IntoProp, Prop, PropUnwrap},
        db::{
            api::{
                mutation::{AdditionOps, DeletionOps, ImportOps, PropertyAdditionOps},
                state::{
                    AsOrderedNodeStateOps, NodeStateGroupBy, NodeStateOps, OrderedNodeStateOps,
                },
                view::{
                    EdgePropertyFilterOps, EdgeViewOps, ExplodedEdgePropertyFilterOps,
                    GraphViewOps, Layer, LayerOps, NodePropertyFilterOps, NodeViewOps, ResetFilter,
                    TimeOps,
                },
            },
            graph::{graph::Graph, views::property_filter::PropertyFilter},
        },
    };
    pub use raphtory_api::core::{entities::GID, input::input_node::InputNode};

    #[cfg(feature = "proto")]
    pub use crate::serialise::{
        parquet::{ParquetDecoder, ParquetEncoder},
        CacheOps, StableDecode, StableEncode,
    };
}

#[cfg(feature = "storage")]
pub use polars_arrow as arrow2;

pub use raphtory_api::{atomic_extra, core::utils::logging};

#[cfg(test)]
mod test_utils {
    use crate::prelude::*;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use itertools::Itertools;
    use proptest::{arbitrary::any, prelude::*, sample::SizeRange};
    use raphtory_api::core::PropType;
    use std::collections::HashMap;
    #[cfg(feature = "storage")]
    use tempfile::TempDir;

    pub(crate) fn test_graph(graph: &Graph, test: impl FnOnce(&Graph)) {
        test(graph)
    }

    #[macro_export]
    macro_rules! test_storage {
        ($graph:expr, $test:expr) => {
            $crate::test_utils::test_graph($graph, $test);
            #[cfg(feature = "storage")]
            $crate::test_utils::test_disk_graph($graph, $test);
        };
    }
    #[cfg(feature = "storage")]
    pub(crate) fn test_disk_graph(graph: &Graph, test: impl FnOnce(&Graph)) {
        let test_dir = TempDir::new().unwrap();
        let disk_graph = graph
            .persist_as_disk_graph(test_dir.path())
            .unwrap()
            .into_graph();
        test(&disk_graph)
    }

    pub(crate) fn build_edge_list(
        len: usize,
        num_nodes: u64,
    ) -> impl Strategy<Value = Vec<(u64, u64, i64, String, i64)>> {
        proptest::collection::vec(
            (
                0..num_nodes,
                0..num_nodes,
                i64::MIN..i64::MAX,
                any::<String>(),
                any::<i64>(),
            ),
            0..=len,
        )
    }

    pub(crate) fn prop(p_type: &PropType) -> impl Strategy<Value = Prop> {
        match p_type {
            PropType::Str => any::<String>().prop_map(|s| Prop::str(s)).boxed(),
            PropType::I64 => any::<i64>().prop_map(|i| Prop::I64(i)).boxed(),
            PropType::F64 => any::<f64>().prop_map(Prop::F64).boxed(),
            PropType::U8 => any::<u8>().prop_map(Prop::U8).boxed(),
            PropType::Bool => any::<bool>().prop_map(Prop::Bool).boxed(),
            PropType::DTime => (1900..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
                .prop_map(|(year, month, day, h, m, s)| {
                    Prop::DTime(
                        format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                            year, month, day, h, m, s
                        )
                        .parse::<DateTime<Utc>>()
                        .unwrap(),
                    )
                })
                .boxed(),
            PropType::NDTime => (1970..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
                .prop_map(|(year, month, day, h, m, s)| {
                    // 2015-09-18T23:56:04
                    Prop::NDTime(
                        format!(
                            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                            year, month, day, h, m, s
                        )
                        .parse::<NaiveDateTime>()
                        .unwrap(),
                    )
                })
                .boxed(),
            PropType::List(p_type) => proptest::collection::vec(prop(p_type), 0..10)
                .prop_map(|props| Prop::List(props.into()))
                .boxed(),
            PropType::Map(p_types) => {
                let prop_types: Vec<BoxedStrategy<(String, Prop)>> = p_types
                    .clone()
                    .into_iter()
                    .map(|(name, p_type)| {
                        let pt_strat = prop(&p_type)
                            .prop_map(move |prop| (name.clone(), prop.clone()))
                            .boxed();
                        pt_strat
                    })
                    .collect_vec();

                let props = proptest::sample::select(prop_types).prop_flat_map(|prop| prop);

                proptest::collection::vec(props, 1..10)
                    .prop_map(|props| Prop::map(props))
                    .boxed()
            }
            _ => todo!(),
        }
    }

    pub(crate) fn prop_type() -> impl Strategy<Value = PropType> {
        let leaf = proptest::sample::select(&[
            PropType::Bool,
            PropType::U8,
            PropType::I64,
            PropType::F64,
            PropType::Str,
            PropType::DTime,
            PropType::NDTime,
        ]);

        leaf.prop_recursive(3, 10, 10, |inner| {
            let dict = proptest::collection::hash_map(r"\w{1,10}", inner.clone(), 1..10)
                .prop_map(|map| PropType::map(map));
            let list = inner
                .clone()
                .prop_map(|p_type| PropType::List(Box::new(p_type)));
            prop_oneof![list, dict]
        })
    }

    #[derive(Debug, Clone)]
    pub struct GraphFixture {
        pub nodes: NodeFixture,
        pub no_props_edges: Vec<(u64, u64, i64)>,
        pub edges: Vec<(u64, u64, i64, Vec<(String, Prop)>, Option<&'static str>)>,
        pub edge_deletions: Vec<(u64, u64, i64)>,
        pub edge_const_props: HashMap<(u64, u64), Vec<(String, Prop)>>,
    }

    #[derive(Debug, Default, Clone)]
    pub struct NodeFixture {
        pub nodes: Vec<(u64, i64, Vec<(String, Prop)>)>,
        pub node_const_props: HashMap<u64, Vec<(String, Prop)>>,
    }

    impl<V, T, I: IntoIterator<Item = (V, T, Vec<(String, Prop)>)>> From<I> for NodeFixture
    where
        u64: TryFrom<V>,
        i64: TryFrom<T>,
    {
        fn from(value: I) -> Self {
            Self {
                nodes: value
                    .into_iter()
                    .filter_map(|(node, time, props)| {
                        Some((node.try_into().ok()?, time.try_into().ok()?, props))
                    })
                    .collect(),
                node_const_props: HashMap::new(),
            }
        }
    }

    impl From<NodeFixture> for GraphFixture {
        fn from(node_fix: NodeFixture) -> Self {
            Self {
                nodes: node_fix,
                edges: vec![],
                edge_deletions: vec![],
                no_props_edges: vec![],
                edge_const_props: HashMap::new(),
            }
        }
    }

    impl<V, T, I: IntoIterator<Item = (V, V, T, Vec<(String, Prop)>, Option<&'static str>)>> From<I>
        for GraphFixture
    where
        u64: TryFrom<V>,
        i64: TryFrom<T>,
    {
        fn from(edges: I) -> Self {
            Self {
                edges: edges
                    .into_iter()
                    .filter_map(|(src, dst, t, props, layer)| {
                        Some((
                            src.try_into().ok()?,
                            dst.try_into().ok()?,
                            t.try_into().ok()?,
                            props,
                            layer,
                        ))
                    })
                    .collect(),
                no_props_edges: vec![],
                edge_deletions: vec![],
                edge_const_props: HashMap::new(),
                nodes: Default::default(),
            }
        }
    }

    fn make_props(
        schema: HashMap<String, PropType>,
    ) -> (
        BoxedStrategy<Vec<(String, Prop)>>,
        BoxedStrategy<Vec<(String, Prop)>>,
    ) {
        let mut iter = schema.iter();

        // split in half, one temporal one constant
        let t_prop_s = (&mut iter)
            .take(schema.len() / 2)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        let c_prop_s = iter
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();

        let num_tprops = t_prop_s.len();
        let num_cprops = c_prop_s.len();

        let t_props =
            proptest::sample::subsequence(t_prop_s, 0..num_tprops).prop_flat_map(|schema| {
                schema
                    .into_iter()
                    .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
                    .collect::<Vec<_>>()
            });
        let c_props =
            proptest::sample::subsequence(c_prop_s, 0..num_cprops).prop_flat_map(|schema| {
                schema
                    .into_iter()
                    .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
                    .collect::<Vec<_>>()
            });
        (t_props.boxed(), c_props.boxed())
    }
    pub(crate) fn build_nodes_dyn(
        nodes: Vec<u64>,
        len: usize,
    ) -> impl Strategy<Value = NodeFixture> {
        proptest::collection::hash_map(r"\w{1,10}", prop_type(), 2..10).prop_flat_map(
            move |schema| {
                let (t_props, c_props) = make_props(schema);

                proptest::collection::vec(
                    (
                        proptest::sample::select(nodes.clone()),
                        i64::MIN..i64::MAX,
                        t_props,
                        c_props,
                    ),
                    0..=len,
                )
                .prop_map(|edges| {
                    let const_props = edges
                        .iter()
                        .into_group_map_by(|(src, _, _, _)| src)
                        .iter()
                        .map(|(&src, &ref b)| {
                            let c_props = b
                                .iter()
                                .flat_map(|(_, _, _, c)| c.clone())
                                .collect::<Vec<_>>();
                            (*src, c_props)
                        })
                        .collect::<HashMap<_, _>>();

                    let nodes = edges
                        .into_iter()
                        .map(|(node, time, t_props, _)| (node, time, t_props))
                        .collect::<Vec<_>>();

                    NodeFixture {
                        nodes,
                        node_const_props: const_props,
                    }
                })
            },
        )
    }

    pub(crate) fn build_edge_list_dyn(
        len: usize,
        num_nodes: usize,
    ) -> impl Strategy<Value = GraphFixture> {
        let num_nodes = num_nodes as u64;
        let edges = proptest::collection::hash_map(any::<String>(), prop_type(), 0..10)
            .prop_flat_map(move |schema| {
                let (t_props, c_props) = make_props(schema);

                proptest::collection::vec(
                    (
                        0..num_nodes,
                        0..num_nodes,
                        i64::MIN..i64::MAX,
                        t_props,
                        c_props,
                        proptest::sample::select(vec![Some("a"), Some("b"), None]),
                    ),
                    0..=len,
                )
                .prop_flat_map(move |edges| {
                    let no_props = proptest::collection::vec(
                        (0..num_nodes, 0..num_nodes, i64::MIN..i64::MAX),
                        0..=len,
                    );
                    let del_edges = proptest::collection::vec(
                        (0..num_nodes, 0..num_nodes, i64::MIN..i64::MAX),
                        0..=len,
                    );
                    (no_props, del_edges).prop_map(move |(no_prop_edges, del_edges)| {
                        let edges = edges.clone();
                        let const_props = edges
                            .iter()
                            .into_group_map_by(|(src, dst, _, _, _, _)| (src, dst))
                            .iter()
                            .map(|(&a, &ref b)| {
                                let (src, dst) = a;
                                let c_props = b
                                    .iter()
                                    .flat_map(|(_, _, _, _, c, _)| c.clone())
                                    .collect::<Vec<_>>();
                                ((*src, *dst), c_props)
                            })
                            .collect::<HashMap<_, _>>();

                        let edges = edges
                            .into_iter()
                            .map(|(src, dst, time, t_props, _, layer)| {
                                (src, dst, time, t_props, layer)
                            })
                            .collect::<Vec<_>>();

                        GraphFixture {
                            edges,
                            edge_const_props: const_props,
                            edge_deletions: del_edges,
                            no_props_edges: no_prop_edges,
                            nodes: Default::default(),
                        }
                    })
                })
            });
        edges
    }

    pub(crate) fn build_graph_strat(
        len: usize,
        num_nodes: usize,
    ) -> impl Strategy<Value = GraphFixture> {
        build_edge_list_dyn(len, num_nodes).prop_flat_map(|g_fixture| {
            let mut nodes = g_fixture
                .edges
                .iter()
                .flat_map(|(src, dst, _, _, _)| [*src, *dst])
                .collect_vec();
            nodes.sort_unstable();
            nodes.dedup();

            if nodes.is_empty() {
                Just(g_fixture).boxed()
            } else {
                let GraphFixture {
                    edges,
                    edge_const_props,
                    no_props_edges,
                    edge_deletions,
                    ..
                } = g_fixture;
                build_nodes_dyn(nodes, 10)
                    .prop_map(move |nodes_f| GraphFixture {
                        nodes: nodes_f,
                        edges: edges.clone(),
                        edge_deletions: edge_deletions.clone(),
                        no_props_edges: no_props_edges.clone(),
                        edge_const_props: edge_const_props.clone(),
                    })
                    .boxed()
            }
        })
    }

    pub(crate) fn build_node_props(
        max_num_nodes: u64,
    ) -> impl Strategy<Value = Vec<(u64, Option<String>, Option<i64>)>> {
        (0..max_num_nodes).prop_flat_map(|num_nodes| {
            (0..num_nodes)
                .map(|node| (Just(node), any::<Option<String>>(), any::<Option<i64>>()))
                .collect_vec()
        })
    }

    pub(crate) fn build_window() -> impl Strategy<Value = (i64, i64)> {
        (i64::MIN..i64::MAX, i64::MIN..i64::MAX)
    }

    pub(crate) fn build_graph_from_edge_list<'a>(
        edge_list: impl IntoIterator<Item = &'a (u64, u64, i64, String, i64)>,
    ) -> Graph {
        let g = Graph::new();
        for (src, dst, time, str_prop, int_prop) in edge_list {
            g.add_edge(
                *time,
                src,
                dst,
                [
                    ("str_prop", str_prop.into_prop()),
                    ("int_prop", int_prop.into_prop()),
                ],
                None,
            )
            .unwrap();
        }
        g
    }

    pub(crate) fn build_graph<'a>(graph_fix: impl Into<GraphFixture>) -> Graph {
        let g = Graph::new();
        let graph_fix = graph_fix.into();
        for (src, dst, time) in &graph_fix.no_props_edges {
            g.add_edge(*time, *src, *dst, NO_PROPS, None).unwrap();
        }
        for (src, dst, time, props, layer) in &graph_fix.edges {
            g.add_edge(*time, src, dst, props.clone(), *layer).unwrap();
        }
        for (src, dst, time) in &graph_fix.edge_deletions {
            if let Some(edge) = g.edge(*src, *dst) {
                edge.delete(*time, None).unwrap();
            }
        }

        for ((src, dst), props) in graph_fix.edge_const_props {
            let edge = g.add_edge(0, src, dst, NO_PROPS, None).unwrap();
            edge.update_constant_properties(props, None).unwrap();
        }
        for (node, t, t_props) in &graph_fix.nodes.nodes {
            if let Some(n) = g.node(*node) {
                n.add_updates(*t, t_props.clone()).unwrap();
            } else {
                g.add_node(0, *node, t_props.clone(), None).unwrap();
            }
        }
        for (node, c_props) in &graph_fix.nodes.node_const_props {
            if let Some(n) = g.node(*node) {
                n.update_constant_properties(c_props.clone()).unwrap();
            } else {
                let node = g.add_node(0, *node, NO_PROPS, None).unwrap();
                node.update_constant_properties(c_props.clone()).unwrap();
            }
        }

        g
    }

    pub(crate) fn add_node_props<'a>(
        graph: &'a Graph,
        nodes: impl IntoIterator<Item = &'a (u64, Option<String>, Option<i64>)>,
    ) {
        for (node, str_prop, int_prop) in nodes {
            let props = [
                str_prop.as_ref().map(|v| ("str_prop", v.into_prop())),
                int_prop.as_ref().map(|v| ("int_prop", (*v).into())),
            ]
            .into_iter()
            .flatten();
            graph.add_node(0, *node, props, None).unwrap();
        }
    }

    pub(crate) fn node_filtered_graph(
        edge_list: &[(u64, u64, i64, String, i64)],
        nodes: &[(u64, Option<String>, Option<i64>)],
        filter: impl Fn(Option<&String>, Option<&i64>) -> bool,
    ) -> Graph {
        let node_map: HashMap<_, _> = nodes
            .iter()
            .map(|(n, str_v, int_v)| (n, (str_v.as_ref(), int_v.as_ref())))
            .collect();
        let g = build_graph_from_edge_list(edge_list.iter().filter(|(src, dst, ..)| {
            let (src_str_v, src_int_v) = node_map.get(src).copied().unwrap_or_default();
            let (dst_str_v, dst_int_v) = node_map.get(dst).copied().unwrap_or_default();
            filter(src_str_v, src_int_v) && filter(dst_str_v, dst_int_v)
        }));
        add_node_props(
            &g,
            nodes
                .iter()
                .filter(|(_, str_v, int_v)| filter(str_v.as_ref(), int_v.as_ref())),
        );
        g
    }
}
