use crate::{db::api::storage::storage::Storage, prelude::*};
use ahash::HashSet;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use proptest::{arbitrary::any, prelude::*};
use proptest_derive::Arbitrary;
use rand::seq::SliceRandom;
use raphtory_api::core::entities::properties::prop::{PropType, DECIMAL_MAX};
use raphtory_storage::{
    core_ops::CoreGraphOps,
    mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps},
};
use std::{
    collections::{hash_map, HashMap},
    ops::{Range, RangeInclusive},
    sync::Arc,
};

pub fn test_graph(graph: &Graph, test: impl FnOnce(&Graph)) {
    test(graph)
}

#[macro_export]
macro_rules! test_storage {
    ($graph:expr, $test:expr) => {
        $crate::test_utils::test_graph($graph, $test);
    };
}

pub fn build_edge_list(
    len: usize,
    num_nodes: u64,
) -> impl Strategy<Value = Vec<(u64, u64, i64, String, i64)>> {
    proptest::collection::vec(
        (
            0..num_nodes,
            0..num_nodes,
            -100i64..100i64,
            any::<String>(),
            any::<i64>(),
        ),
        0..=len,
    )
}

pub fn build_edge_list_str(
    len: usize,
    num_nodes: u64,
) -> impl Strategy<Value = Vec<(String, String, i64, String, i64)>> {
    proptest::collection::vec(
        (
            (0..num_nodes).prop_map(|i| i.to_string()),
            (0..num_nodes).prop_map(|i| i.to_string()),
            i64::MIN..i64::MAX,
            any::<String>(),
            any::<i64>(),
        ),
        0..=len,
    )
}

pub fn build_edge_list_with_secondary_index(
    len: usize,
    num_nodes: u64,
) -> impl Strategy<Value = Vec<(u64, u64, i64, u64, String, i64)>> {
    Just(()).prop_flat_map(move |_| {
        // Generate a shuffled set of unique secondary indices
        let mut secondary_index: Vec<u64> = (0..len as u64).collect();
        let mut rng = rand::rng();
        secondary_index.shuffle(&mut rng);

        prop::collection::vec(
            (
                0..num_nodes,       // src
                0..num_nodes,       // dst
                i64::MIN..i64::MAX, // time
                any::<String>(),    // str_prop
                i64::MIN..i64::MAX, // int_prop
            ),
            len,
        )
        .prop_map(move |edges| {
            // add secondary indices to the edges
            edges
                .into_iter()
                .zip(secondary_index.iter())
                .map(|((src, dst, time, str_prop, int_prop), &sec_index)| {
                    (src, dst, time, sec_index, str_prop, int_prop)
                })
                .collect::<Vec<_>>()
        })
    })
}

pub fn build_edge_deletions(
    len: usize,
    num_nodes: u64,
) -> impl Strategy<Value = Vec<(u64, u64, i64)>> {
    proptest::collection::vec((0..num_nodes, 0..num_nodes, i64::MIN..i64::MAX), 0..=len)
}

#[derive(Debug, Arbitrary, PartialOrd, PartialEq, Eq, Ord)]
pub enum Update {
    Addition(String, i64),
    Deletion,
}

pub fn build_edge_list_with_deletions(
    len: usize,
    num_nodes: u64,
) -> impl Strategy<Value = HashMap<(u64, u64), Vec<(i64, Update)>>> {
    proptest::collection::hash_map(
        (0..num_nodes, 0..num_nodes),
        proptest::collection::vec(any::<(i64, Update)>(), 0..=len),
        0..=len,
    )
}

pub fn prop(p_type: &PropType) -> BoxedStrategy<Prop> {
    match p_type {
        PropType::Str => any::<String>().prop_map(Prop::str).boxed(),
        PropType::I64 => any::<i64>().prop_map(Prop::I64).boxed(),
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
            let key_val: Vec<_> = p_types
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let len = key_val.len();
            let samples = proptest::sample::subsequence(key_val, 0..=len); // FIXME size 0..=len breaks type merging because empty maps {} needs looking into
            samples
                .prop_flat_map(|key_vals| {
                    let props: Vec<_> = key_vals
                        .into_iter()
                        .map(|(key, val_type)| {
                            prop(&val_type).prop_map(move |val| (key.clone(), val))
                        })
                        .collect();
                    props.prop_map(Prop::map)
                })
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

pub fn prop_type(nested_prop_size: usize) -> impl Strategy<Value = PropType> {
    let leaf = proptest::sample::select(&[
        PropType::Str,
        PropType::I64,
        PropType::F64,
        PropType::U8,
        PropType::Bool,
        PropType::DTime,
        PropType::NDTime,
        PropType::Decimal { scale: 7 },
    ]);

    leaf.prop_recursive(3, 10, 10, move |inner| {
        let dict = proptest::collection::hash_map(r"\w{1,10}", inner.clone(), 0..=nested_prop_size) // FIXME size 0..=len breaks type merging because empty maps {} needs looking into
            .prop_map(PropType::map);
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
    pub fn edges(&self) -> impl Iterator<Item = ((u64, u64, Option<&str>), &EdgeUpdatesFixture)> {
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

impl IntoIterator for NodeFixture {
    type Item = (u64, NodeUpdatesFixture);
    type IntoIter = hash_map::IntoIter<u64, NodeUpdatesFixture>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
    pub fn iter(&self) -> impl Iterator<Item = ((u64, u64, Option<&str>), &EdgeUpdatesFixture)> {
        self.0.iter().map(|(k, v)| (*k, v))
    }
}

impl IntoIterator for EdgeFixture {
    type Item = ((u64, u64, Option<&'static str>), EdgeUpdatesFixture);
    type IntoIter = hash_map::IntoIter<(u64, u64, Option<&'static str>), EdgeUpdatesFixture>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<((u64, u64, Option<&'static str>), EdgeUpdatesFixture)> for EdgeFixture {
    fn from_iter<T: IntoIterator<Item = ((u64, u64, Option<&'static str>), EdgeUpdatesFixture)>>(
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

fn prop_schema(num_props: RangeInclusive<usize>) -> impl Strategy<Value = Vec<(String, PropType)>> {
    proptest::collection::hash_map(num_props.clone(), prop_type(*num_props.end()), num_props)
        .prop_map(|v| v.into_iter().map(|(k, p)| (k.to_string(), p)).collect())
}

fn t_props(
    schema: Vec<(String, PropType)>,
    num_props: RangeInclusive<usize>,
) -> impl Strategy<Value = Vec<(i64, Vec<(String, Prop)>)>> {
    proptest::collection::vec((any::<i64>(), make_props(schema)), num_props)
}

fn prop_updates(
    schema: Vec<(String, PropType)>,
    num_props: RangeInclusive<usize>,
) -> impl Strategy<Value = PropUpdatesFixture> {
    let t_props = t_props(schema.clone(), num_props);
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
    num_updates: RangeInclusive<usize>,
) -> impl Strategy<Value = NodeUpdatesFixture> {
    (prop_updates(schema, num_updates), make_node_type())
        .prop_map(|(props, node_type)| NodeUpdatesFixture { props, node_type })
}

fn edge_updates(
    schema: Vec<(String, PropType)>,
    num_updates: RangeInclusive<usize>,
    deletions: bool,
) -> impl Strategy<Value = EdgeUpdatesFixture> {
    let del_len = if deletions { *num_updates.end() } else { 0 };
    (
        prop_updates(schema, num_updates),
        proptest::collection::vec(-150i64..150, 0..=del_len),
    )
        .prop_map(|(props, deletions)| EdgeUpdatesFixture { props, deletions })
}

pub fn build_nodes_dyn(
    num_nodes: Range<usize>,
    num_props: RangeInclusive<usize>,
    num_updates: RangeInclusive<usize>,
) -> impl Strategy<Value = NodeFixture> {
    let schema = prop_schema(num_props);
    schema.prop_flat_map(move |schema| {
        num_nodes
            .clone()
            .map(|node| {
                (
                    Just(node as u64),
                    node_updates(schema.clone(), num_updates.clone()),
                )
            })
            .collect_vec()
            .prop_map(|updates| {
                NodeFixture::from_iter(
                    updates
                        .into_iter()
                        .filter(|(_, v)| !v.props.t_props.is_empty()),
                )
            })
    })
}

pub fn build_edge_list_dyn(
    num_edges: RangeInclusive<usize>,
    num_nodes: Range<usize>,
    num_properties: RangeInclusive<usize>,
    num_updates: RangeInclusive<usize>,
    del_edges: bool,
) -> impl Strategy<Value = EdgeFixture> {
    let schema = prop_schema(num_properties);
    schema.prop_flat_map(move |schema| {
        proptest::collection::hash_map(
            (
                num_nodes.clone().prop_map(|n| n as u64),
                num_nodes.clone().prop_map(|n| n as u64),
                proptest::sample::select(vec![Some("a"), Some("b"), None]),
            ),
            edge_updates(schema.clone(), num_updates.clone(), del_edges),
            num_edges.clone(),
        )
        .prop_map(|values| {
            EdgeFixture::from_iter(
                values
                    .into_iter()
                    .filter(|(_, updates)| !updates.props.t_props.is_empty()),
            )
        })
    })
}

pub fn build_props_dyn(
    num_props: RangeInclusive<usize>,
) -> impl Strategy<Value = PropUpdatesFixture> {
    let schema = prop_schema(num_props.clone());
    schema.prop_flat_map(move |schema| prop_updates(schema, num_props.clone()))
}

pub fn build_graph_strat(
    num_nodes: usize,
    num_edges: usize,
    num_properties: usize,
    num_updates: usize,
    del_edges: bool,
) -> impl Strategy<Value = GraphFixture> {
    build_graph_strat_r(
        0..num_nodes,
        0..=num_edges,
        0..=num_properties,
        0..=num_updates,
        del_edges,
    )
}

pub fn build_graph_strat_r(
    num_nodes: Range<usize>,
    num_edges: RangeInclusive<usize>,
    num_properties: RangeInclusive<usize>,
    num_updates: RangeInclusive<usize>,
    del_edges: bool,
) -> impl Strategy<Value = GraphFixture> {
    let nodes = build_nodes_dyn(
        num_nodes.clone(),
        num_properties.clone(),
        num_updates.clone(),
    );
    let edges = build_edge_list_dyn(num_edges, num_nodes, num_properties, num_updates, del_edges);
    (nodes, edges).prop_map(|(nodes, edges)| GraphFixture { nodes, edges })
}

pub fn build_node_props(
    max_num_nodes: u64,
) -> impl Strategy<Value = Vec<(u64, Option<String>, Option<i64>)>> {
    (0..max_num_nodes).prop_flat_map(|num_nodes| {
        (0..num_nodes)
            .map(|node| (Just(node), any::<Option<String>>(), any::<Option<i64>>()))
            .collect_vec()
    })
}

pub fn build_graph_from_edge_list<'a>(
    edge_list: impl IntoIterator<Item = &'a (u64, u64, i64, String, i64)>,
) -> Graph {
    let g = Graph::new();
    for (src, dst, time, str_prop, int_prop) in edge_list {
        g.add_edge(
            *time,
            src,
            dst,
            [
                ("str_prop", str_prop.as_str().into_prop()),
                ("int_prop", int_prop.into_prop()),
            ],
            None,
        )
        .unwrap();
    }
    g
}

pub fn build_graph(graph_fix: &GraphFixture) -> Arc<Storage> {
    let g = Arc::new(Storage::default());
    for ((src, dst, layer), updates) in graph_fix.edges() {
        for (t, props) in updates.props.t_props.iter() {
            g.add_edge(*t, src, dst, props.clone(), layer).unwrap();
        }
        if let Some(e) = g.edge(src, dst) {
            if !updates.props.c_props.is_empty() {
                e.add_metadata(updates.props.c_props.clone(), layer)
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
            node.add_metadata(updates.props.c_props.clone()).unwrap();
            if let Some(node_type) = updates.node_type {
                node.set_node_type(node_type).unwrap();
            }
        }
    }

    g
}

pub fn build_graph_layer(graph_fix: &GraphFixture, layers: &[&str]) -> Arc<Storage> {
    let g = Arc::new(Storage::default());
    let actual_layer_set: HashSet<_> = graph_fix
        .edges()
        .filter(|(_, updates)| !updates.deletions.is_empty() || !updates.props.t_props.is_empty())
        .map(|((_, _, layer), _)| layer.unwrap_or("_default"))
        .collect();

    // make sure the graph has the layers in the right order
    for layer in layers {
        if actual_layer_set.contains(layer) {
            g.resolve_layer(Some(layer)).unwrap();
        }
    }

    let layers = g.edge_meta().layer_meta();

    let session = g.write_session().unwrap();
    for ((src, dst, layer), updates) in graph_fix.edges() {
        // properties always exist in the graph
        for (_, props) in updates.props.t_props.iter() {
            for (key, value) in props {
                session
                    .resolve_edge_property(key, value.dtype(), false)
                    .unwrap();
            }
        }
        for (key, value) in updates.props.c_props.iter() {
            session
                .resolve_edge_property(key, value.dtype(), true)
                .unwrap();
        }

        if layers.contains(layer.unwrap_or("_default")) {
            for (t, props) in updates.props.t_props.iter() {
                g.add_edge(*t, src, dst, props.clone(), layer).unwrap();
            }
            if let Some(e) = g.edge(src, dst) {
                if !updates.props.c_props.is_empty() {
                    e.add_metadata(updates.props.c_props.clone(), layer)
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
            node.add_metadata(updates.props.c_props.clone()).unwrap();
            if let Some(node_type) = updates.node_type {
                node.set_node_type(node_type).unwrap();
            }
        }
    }
    g
}

pub fn add_node_props<'a>(
    graph: &'a Graph,
    nodes: impl IntoIterator<Item = &'a (u64, Option<String>, Option<i64>)>,
) {
    for (node, str_prop, int_prop) in nodes {
        let props = [
            str_prop.as_deref().map(|v| ("str_prop", v.into_prop())),
            int_prop.as_ref().map(|v| ("int_prop", (*v).into())),
        ]
        .into_iter()
        .flatten();
        graph.add_node(0, *node, props, None).unwrap();
    }
}

pub fn node_filtered_graph(
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
