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

pub mod core;
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
                    EdgePropertyFilterOps,
                    EdgeViewOps, // ExplodedEdgePropertyFilterOps,
                    GraphViewOps,
                    Layer,
                    LayerOps,
                    NodePropertyFilterOps,
                    NodeViewOps,
                    ResetFilter,
                    TimeOps,
                },
            },
            graph::{graph::Graph, views::filter::model::property_filter::PropertyFilter},
        },
    };
    pub use raphtory_api::core::{entities::GID, input::input_node::InputNode};

    #[cfg(feature = "proto")]
    pub use crate::serialise::{
        parquet::{ParquetDecoder, ParquetEncoder},
        CacheOps, StableDecode, StableEncode,
    };

    #[cfg(feature = "search")]
    pub use crate::db::api::view::SearchableGraphOps;
}

#[cfg(feature = "storage")]
pub use polars_arrow as arrow2;

pub use raphtory_api::{atomic_extra, core::utils::logging};

#[cfg(test)]
mod test_utils {
    use crate::{
        core::DECIMAL_MAX,
        db::api::{mutation::internal::InternalAdditionOps, storage::storage::Storage},
        prelude::*,
    };
    use bigdecimal::BigDecimal;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use itertools::Itertools;
    use proptest::{arbitrary::any, prelude::*};
    use proptest_derive::Arbitrary;
    use raphtory_api::core::PropType;
    use std::{collections::HashMap, sync::Arc};
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

    #[derive(Debug, Arbitrary, PartialOrd, PartialEq, Eq, Ord)]
    pub(crate) enum Update {
        Addition(String, i64),
        Deletion,
    }

    pub(crate) fn build_edge_list_with_deletions(
        len: usize,
        num_nodes: u64,
    ) -> impl Strategy<Value = HashMap<(u64, u64), Vec<(i64, Update)>>> {
        proptest::collection::hash_map(
            (0..num_nodes, 0..num_nodes),
            proptest::collection::vec(any::<(i64, Update)>(), 0..=len),
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
            PropType::Decimal { scale } => {
                let scale = *scale;
                let dec_max = DECIMAL_MAX;
                ((scale as i128)..dec_max)
                    .prop_map(move |int| Prop::Decimal(BigDecimal::new(int.into(), scale)))
                    .boxed()
            }
            _ => todo!(),
        }
    }

    pub(crate) fn prop_type() -> impl Strategy<Value = PropType> {
        let leaf = proptest::sample::select(&[
            PropType::Str,
            PropType::I64,
            PropType::F64,
            PropType::U8,
            PropType::Bool,
            PropType::DTime,
            PropType::NDTime,
            // PropType::Decimal { scale }, decimal breaks the tests because of polars-parquet
        ]);

        leaf.prop_recursive(3, 10, 10, |inner| {
            let dict = proptest::collection::hash_map(r"\w{1,10}", inner.clone(), 1..10)
                .prop_map(|map| PropType::map(map));
            let list = inner
                .clone()
                .prop_map(|p_type| PropType::List(Box::new(p_type)));
            prop_oneof![inner, list, dict]
        })
    }

    #[derive(Debug, Clone)]
    pub struct GraphFixture {
        pub nodes: NodeFixture,
        pub edges: EdgeFixture,
    }

    impl GraphFixture {
        pub fn edges(
            &self,
        ) -> impl Iterator<Item = ((u64, u64, Option<&str>), &EdgeUpdatesFixture)> {
            self.edges.iter()
        }

        pub fn nodes(&self) -> impl Iterator<Item = (u64, &NodeUpdatesFixture)> {
            self.nodes.iter()
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct NodeFixture(pub HashMap<u64, NodeUpdatesFixture>);

    impl FromIterator<(u64, NodeUpdatesFixture)> for NodeFixture {
        fn from_iter<T: IntoIterator<Item = (u64, NodeUpdatesFixture)>>(iter: T) -> Self {
            Self(iter.into_iter().collect())
        }
    }

    impl NodeFixture {
        pub fn iter(&self) -> impl Iterator<Item = (u64, &NodeUpdatesFixture)> {
            self.0.iter().map(|(k, v)| (*k, v))
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct PropUpdatesFixture {
        pub t_props: Vec<(i64, Vec<(String, Prop)>)>,
        pub c_props: Vec<(String, Prop)>,
    }

    #[derive(Debug, Default, Clone)]
    pub struct NodeUpdatesFixture {
        pub props: PropUpdatesFixture,
        pub node_type: Option<&'static str>,
    }

    #[derive(Debug, Default, Clone)]
    pub struct EdgeUpdatesFixture {
        pub props: PropUpdatesFixture,
        pub deletions: Vec<i64>,
    }

    #[derive(Debug, Default, Clone)]
    pub struct EdgeFixture(pub HashMap<(u64, u64, Option<&'static str>), EdgeUpdatesFixture>);

    impl EdgeFixture {
        pub fn iter(
            &self,
        ) -> impl Iterator<Item = ((u64, u64, Option<&str>), &EdgeUpdatesFixture)> {
            self.0.iter().map(|(k, v)| (*k, v))
        }
    }

    impl FromIterator<((u64, u64, Option<&'static str>), EdgeUpdatesFixture)> for EdgeFixture {
        fn from_iter<
            T: IntoIterator<Item = ((u64, u64, Option<&'static str>), EdgeUpdatesFixture)>,
        >(
            iter: T,
        ) -> Self {
            Self(iter.into_iter().collect())
        }
    }

    impl<V, T, I: IntoIterator<Item = (V, T, Vec<(String, Prop)>)>> From<I> for NodeFixture
    where
        u64: TryFrom<V>,
        i64: TryFrom<T>,
    {
        fn from(value: I) -> Self {
            Self(
                value
                    .into_iter()
                    .filter_map(|(node, time, props)| {
                        Some((node.try_into().ok()?, (time.try_into().ok()?, props)))
                    })
                    .into_group_map()
                    .into_iter()
                    .map(|(k, t_props)| {
                        (
                            k,
                            NodeUpdatesFixture {
                                props: PropUpdatesFixture {
                                    t_props,
                                    ..Default::default()
                                },
                                node_type: None,
                            },
                        )
                    })
                    .collect(),
            )
        }
    }

    impl From<NodeFixture> for GraphFixture {
        fn from(node_fix: NodeFixture) -> Self {
            Self {
                nodes: node_fix,
                edges: Default::default(),
            }
        }
    }

    impl From<EdgeFixture> for GraphFixture {
        fn from(edges: EdgeFixture) -> Self {
            GraphFixture {
                nodes: Default::default(),
                edges,
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
            let edges = edges
                .into_iter()
                .filter_map(|(src, dst, t, props, layer)| {
                    Some((
                        (src.try_into().ok()?, dst.try_into().ok()?, layer),
                        (t.try_into().ok()?, props),
                    ))
                })
                .into_group_map()
                .into_iter()
                .map(|(k, t_props)| {
                    (
                        k,
                        EdgeUpdatesFixture {
                            props: PropUpdatesFixture {
                                t_props,
                                c_props: vec![],
                            },
                            deletions: vec![],
                        },
                    )
                })
                .collect();
            Self {
                edges: EdgeFixture(edges),
                nodes: Default::default(),
            }
        }
    }

    pub fn make_node_type() -> impl Strategy<Value = Option<&'static str>> {
        proptest::sample::select(vec![None, Some("one"), Some("two")])
    }

    pub fn make_node_types() -> impl Strategy<Value = Vec<&'static str>> {
        proptest::sample::subsequence(vec!["_default", "one", "two"], 0..=3)
    }

    pub fn build_window() -> impl Strategy<Value = (i64, i64)> {
        any::<(i64, i64)>()
    }

    fn make_props(schema: Vec<(String, PropType)>) -> impl Strategy<Value = Vec<(String, Prop)>> {
        let num_props = schema.len();
        proptest::sample::subsequence(schema, 0..=num_props).prop_flat_map(|schema| {
            schema
                .into_iter()
                .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
                .collect::<Vec<_>>()
        })
    }

    fn prop_schema(len: usize) -> impl Strategy<Value = Vec<(String, PropType)>> {
        proptest::collection::hash_map(0..len, prop_type(), 0..=len)
            .prop_map(|v| v.into_iter().map(|(k, p)| (k.to_string(), p)).collect())
    }

    fn t_props(
        schema: Vec<(String, PropType)>,
        len: usize,
    ) -> impl Strategy<Value = Vec<(i64, Vec<(String, Prop)>)>> {
        proptest::collection::vec((any::<i64>(), make_props(schema)), 0..=len)
    }

    fn prop_updates(
        schema: Vec<(String, PropType)>,
        len: usize,
    ) -> impl Strategy<Value = PropUpdatesFixture> {
        let t_props = t_props(schema.clone(), len);
        let c_props = make_props(schema);
        (t_props, c_props).prop_map(|(t_props, c_props)| {
            if t_props.is_empty() {
                PropUpdatesFixture {
                    t_props,
                    c_props: vec![],
                }
            } else {
                PropUpdatesFixture { t_props, c_props }
            }
        })
    }

    fn node_updates(
        schema: Vec<(String, PropType)>,
        len: usize,
    ) -> impl Strategy<Value = NodeUpdatesFixture> {
        (prop_updates(schema, len), make_node_type())
            .prop_map(|(props, node_type)| NodeUpdatesFixture { props, node_type })
    }

    fn edge_updates(
        schema: Vec<(String, PropType)>,
        len: usize,
        deletions: bool,
    ) -> impl Strategy<Value = EdgeUpdatesFixture> {
        let del_len = if deletions { len } else { 0 };
        (
            prop_updates(schema, len),
            proptest::collection::vec(i64::MIN..i64::MAX, 0..=del_len),
        )
            .prop_map(|(props, deletions)| EdgeUpdatesFixture { props, deletions })
    }

    pub(crate) fn build_nodes_dyn(
        num_nodes: usize,
        len: usize,
    ) -> impl Strategy<Value = NodeFixture> {
        let schema = prop_schema(len);
        schema.prop_flat_map(move |schema| {
            proptest::collection::hash_map(
                0..num_nodes as u64,
                node_updates(schema.clone(), len),
                0..=len,
            )
            .prop_map(NodeFixture)
        })
    }

    pub(crate) fn build_edge_list_dyn(
        len: usize,
        num_nodes: usize,
        del_edges: bool,
    ) -> impl Strategy<Value = EdgeFixture> {
        let num_nodes = num_nodes as u64;

        let schema = prop_schema(len);
        schema.prop_flat_map(move |schema| {
            proptest::collection::hash_map(
                (
                    0..num_nodes,
                    0..num_nodes,
                    proptest::sample::select(vec![Some("a"), Some("b"), None]),
                ),
                edge_updates(schema.clone(), len, del_edges),
                0..=len,
            )
            .prop_map(EdgeFixture)
        })
    }

    pub(crate) fn build_props_dyn(len: usize) -> impl Strategy<Value = PropUpdatesFixture> {
        let schema = prop_schema(len);
        schema.prop_flat_map(move |schema| prop_updates(schema, len))
    }

    pub(crate) fn build_graph_strat(
        len: usize,
        num_nodes: usize,
        del_edges: bool,
    ) -> impl Strategy<Value = GraphFixture> {
        let nodes = build_nodes_dyn(num_nodes, len);
        let edges = build_edge_list_dyn(len, num_nodes, del_edges);
        (nodes, edges).prop_map(|(nodes, edges)| GraphFixture { nodes, edges })
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

    pub(crate) fn build_graph<'a>(graph_fix: &GraphFixture) -> Arc<Storage> {
        let g = Arc::new(Storage::default());
        for ((src, dst, layer), updates) in graph_fix.edges() {
            for (t, props) in updates.props.t_props.iter() {
                g.add_edge(*t, src, dst, props.clone(), layer).unwrap();
            }
            if let Some(e) = g.edge(src, dst) {
                if !updates.props.c_props.is_empty() {
                    e.add_constant_properties(updates.props.c_props.clone(), layer)
                        .unwrap();
                }
            }
            for t in updates.deletions.iter() {
                g.delete_edge(*t, src, dst, layer).unwrap();
            }
        }

        for (node, updates) in graph_fix.nodes() {
            for (t, props) in updates.props.t_props.iter() {
                g.add_node(*t, node, props.clone(), None).unwrap();
            }
            if let Some(node) = g.node(node) {
                node.add_constant_properties(updates.props.c_props.clone())
                    .unwrap();
                if let Some(node_type) = updates.node_type {
                    node.set_node_type(node_type).unwrap();
                }
            }
        }

        g
    }

    pub(crate) fn build_graph_layer(
        graph_fix: &GraphFixture,
        layers: impl Into<Layer>,
    ) -> Arc<Storage> {
        let g = Arc::new(Storage::default());
        let layers = layers.into();

        for ((src, dst, layer), updates) in graph_fix.edges() {
            // properties always exist in the graph
            for (_, props) in updates.props.t_props.iter() {
                for (key, value) in props {
                    g.resolve_edge_property(key, value.dtype(), false).unwrap();
                }
            }
            for (key, value) in updates.props.c_props.iter() {
                g.resolve_edge_property(key, value.dtype(), true).unwrap();
            }

            if layers.contains(layer.unwrap_or("_default")) {
                for (t, props) in updates.props.t_props.iter() {
                    g.add_edge(*t, src, dst, props.clone(), layer).unwrap();
                }
                if let Some(e) = g.edge(src, dst) {
                    if !updates.props.c_props.is_empty() {
                        e.add_constant_properties(updates.props.c_props.clone(), layer)
                            .unwrap();
                    }
                }
                for t in updates.deletions.iter() {
                    g.delete_edge(*t, src, dst, layer).unwrap();
                }
            }
        }

        for (node, updates) in graph_fix.nodes() {
            for (t, props) in updates.props.t_props.iter() {
                g.add_node(*t, node, props.clone(), None).unwrap();
            }
            if let Some(node) = g.node(node) {
                node.add_constant_properties(updates.props.c_props.clone())
                    .unwrap();
                if let Some(node_type) = updates.node_type {
                    node.set_node_type(node_type).unwrap();
                }
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
