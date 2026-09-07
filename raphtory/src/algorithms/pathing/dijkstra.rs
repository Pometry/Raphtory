/// Dijkstra's algorithm
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::StaticGraphViewOps};
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::state::{
            ops::Const, GenericNodeState, Index, NodeStateOutputType, NodeStateValue,
            TypedNodeState,
        },
        graph::{edge::EdgeView, edges::Edges, node::NodeView, nodes::Nodes},
    },
    errors::GraphError,
    prelude::*,
};
use ahash::AHashMap;
use bigdecimal::BigDecimal;
use indexmap::IndexSet;
use num_traits::{One, Zero};
use ordered_float::OrderedFloat;
use raphtory_api::core::{
    entities::{
        properties::prop::{PropExact, PropType, PropUnwrap, SerdeArrowProp},
        VID,
    },
    Direction,
};
use roaring::RoaringTreemap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    cmp::Reverse,
    collections::{hash_map::Entry, BinaryHeap, HashMap},
    ops::Add,
};

fn prop_serialize<S: Serializer>(v: &Prop, s: S) -> Result<S::Ok, S::Error> {
    SerdeArrowProp(v).serialize(s)
}

fn prop_deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Prop, D::Error> {
    PropExact::deserialize(d).map(|v| v.0)
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
pub struct DistanceState {
    #[serde(
        serialize_with = "prop_serialize",
        deserialize_with = "prop_deserialize"
    )]
    pub distance: Prop,
    pub path: Vec<VID>,
}

#[derive(Clone, Debug)]
pub struct TransformedDistanceState<'graph, G>
where
    G: GraphViewOps<'graph>,
{
    pub distance: Prop,
    pub path: Nodes<'graph, G, G>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
struct TypedDistanceState<V> {
    distance: V,
    path: Vec<VID>,
}

impl DistanceState {
    pub fn node_transform<'graph, G>(
        state: &GenericNodeState<'graph, G>,
        value: Self,
    ) -> TransformedDistanceState<'graph, G>
    where
        G: GraphViewOps<'graph>,
    {
        TransformedDistanceState {
            distance: value.distance,
            path: Nodes::new_filtered(
                state.base_graph.clone(),
                state.base_graph.clone(),
                Const(true),
                Index::from_iter(value.path),
            ),
        }
    }
}

/// A state in the Dijkstra algorithm with a cost and a node name.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct State<V> {
    cost: V,
    node: VID,
}

#[derive(Default, Debug)]
struct TreeMapIndex {
    map: RoaringTreemap,
}

impl FromIterator<VID> for TreeMapIndex {
    fn from_iter<T: IntoIterator<Item = VID>>(iter: T) -> Self {
        let map = RoaringTreemap::from_iter(iter.into_iter().map(|v| v.as_u64()));
        Self { map }
    }
}

impl TreeMapIndex {
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn remove(&mut self, v: VID) -> bool {
        self.map.remove(v.as_u64())
    }

    fn insert(&mut self, v: VID) -> bool {
        self.map.insert(v.as_u64())
    }

    fn len(&self) -> usize {
        self.map.len() as usize
    }
}

fn dijkstra_inner<
    'a,
    G: StaticGraphViewOps,
    V: Clone + Add<Output = V> + Zero + One + Ord + IntoProp + NodeStateValue,
>(
    g: &'a G,
    source: VID,
    mut targets: TreeMapIndex,
    weight_fn: impl Fn(EdgeView<&'a G>) -> V,
    neighbours_fn: impl Fn(NodeView<'a, &'a G>) -> Edges<'a, &'a G>,
) -> GenericNodeState<'static, G> {
    // BinaryHeap is a max-heap; Reverse makes it pop the lowest cost first
    let mut heap = BinaryHeap::new();
    heap.push(Reverse(State {
        cost: V::zero(),
        node: source,
    }));

    let mut index = IndexSet::<VID, ahash::RandomState>::with_capacity_and_hasher(
        targets.len(),
        ahash::RandomState::new(),
    );
    let mut values = Vec::with_capacity(targets.len());

    // map from node to predecessor in path and distance from src
    let mut predecessor_and_dist = AHashMap::new();
    let mut visited = TreeMapIndex::default();

    predecessor_and_dist.insert(source, (VID::default(), V::zero()));

    while let Some(Reverse(State {
        cost,
        node: node_vid,
    })) = heap.pop()
    {
        if targets.remove(node_vid) {
            index.insert(node_vid);
            let mut path = Vec::new();
            path.push(node_vid);
            let mut current_node_id = node_vid;
            while let Some(prev_node) = predecessor_and_dist
                .get(&current_node_id)
                .and_then(|(v, _)| v.into_option())
            {
                path.push(prev_node);
                current_node_id = prev_node;
            }
            path.reverse();
            values.push(TypedDistanceState {
                distance: cost.clone(),
                path,
            });
            if targets.is_empty() {
                break;
            }
        }

        if !visited.insert(node_vid) {
            continue;
        }

        let Some(edges) = (&g).node(node_vid).map(&neighbours_fn) else {
            continue;
        };
        for edge in edges {
            let next_node_vid = edge.nbr().node;
            let edge_val = weight_fn(edge);

            let next_cost = cost.clone() + edge_val;
            if match predecessor_and_dist.entry(next_node_vid) {
                Entry::Occupied(entry) => {
                    let (vid, v) = entry.into_mut();
                    if next_cost < *v {
                        *vid = node_vid;
                        *v = next_cost.clone();
                        true
                    } else {
                        false
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((node_vid, next_cost.clone()));
                    true
                }
            } {
                heap.push(Reverse(State {
                    cost: next_cost,
                    node: next_node_vid,
                }));
            }
        }
    }
    GenericNodeState::new_from_eval_with_index(
        g.clone(),
        values,
        index.into(),
        Some(HashMap::from([(
            "path".to_string(),
            (NodeStateOutputType::Nodes, None),
        )])),
    )
}

/// Finds the shortest paths from a single source to multiple targets in a graph.
///
/// # Arguments
///
/// * `graph`: The graph to search in.
/// * `source`: The source node.
/// * `targets`: A vector of target nodes.
/// * `weight`: Option, The name of the weight property for the edges. If not set then defaults all edges to weight=1.
/// * `direction`: The direction of the edges of the shortest path. Defaults to both directions (undirected graph).
///
/// # Returns
///
/// Returns a `HashMap` where the key is the target node and the value is a tuple containing
/// the total cost and a vector of nodes representing the shortest path.
///
pub fn dijkstra_single_source_shortest_paths<'a, G: StaticGraphViewOps, T: AsNodeRef>(
    g: &'a G,
    source: T,
    targets: Vec<T>,
    weight: Option<&str>,
    direction: Direction,
    default: Option<Prop>,
) -> Result<
    TypedNodeState<'static, DistanceState, G, TransformedDistanceState<'static, G>>,
    GraphError,
> {
    let source_ref = source.as_node_ref();
    let source_node = match (&g).node(source_ref) {
        Some(src) => src.node,
        None => {
            let gid = match source_ref {
                NodeRef::Internal(vid) => g.node_id(vid),
                NodeRef::External(gid) => gid.to_owned(),
            };
            return Err(GraphError::NodeMissingError(gid));
        }
    };

    let target_nodes: TreeMapIndex = targets
        .into_iter()
        .filter_map(|target| (&g).node(target).map(|n| n.node))
        .collect();

    let edge_fn = match direction {
        Direction::OUT => |node: NodeView<'a, &'a G>| node.out_edges(),
        Direction::IN => |node: NodeView<'a, &'a G>| node.in_edges(),
        Direction::BOTH => |node: NodeView<'a, &'a G>| node.edges(),
    };

    let paths = match weight {
        None => match default {
            None => dijkstra_inner(g, source_node, target_nodes, |_| 1u64, edge_fn),
            Some(default) => match default {
                default if default.is_signed_int() => {
                    let default_v = default.as_i64_lossless().unwrap();
                    dijkstra_inner(g, source_node, target_nodes, |_| default_v, edge_fn)
                }
                default if default.is_unsigned_int() => {
                    let default_v = default.as_u64_lossless().unwrap();
                    dijkstra_inner(g, source_node, target_nodes, |_| default_v, edge_fn)
                }
                Prop::F64(default_v) => dijkstra_inner(
                    g,
                    source_node,
                    target_nodes,
                    |_| OrderedFloat(default_v),
                    edge_fn,
                ),
                Prop::F32(default_v) => dijkstra_inner(
                    g,
                    source_node,
                    target_nodes,
                    |_| OrderedFloat(default_v),
                    edge_fn,
                ),
                Prop::Decimal(default_v) => {
                    dijkstra_inner(g, source_node, target_nodes, |_| default_v.clone(), edge_fn)
                }
                _ => {
                    return Err(GraphError::InvalidValue {
                        reason: "Default value does not match property type".to_string(),
                    })
                }
            },
        },
        Some(weight_prop) => {
            let (weight_prop_id, dtype) = g
                .edge_meta()
                .get_prop_id_and_type(weight_prop, false)
                .ok_or_else(|| GraphError::PropertyMissingError(weight_prop.to_string()))?;
            match dtype {
                PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => {
                    let default_val = match default {
                        None => 1u64,
                        Some(v) => v
                            .as_u64_lossless()
                            .ok_or_else(|| GraphError::InvalidValue {
                                reason: "Default value does not match property type".to_string(),
                            })?,
                    };
                    dijkstra_inner(
                        g,
                        source_node,
                        target_nodes,
                        |e| {
                            e.properties()
                                .get_by_id(weight_prop_id)
                                .map_or(default_val, |v| v.as_u64_lossless().unwrap())
                        },
                        edge_fn,
                    )
                }
                PropType::I32 | PropType::I64 => {
                    let default_val = match default {
                        None => 1i64,
                        Some(v) => v
                            .as_i64_lossless()
                            .ok_or_else(|| GraphError::InvalidValue {
                                reason: "Default value does not match property type".to_string(),
                            })?,
                    };
                    dijkstra_inner(
                        g,
                        source_node,
                        target_nodes,
                        |e| {
                            e.properties()
                                .get_by_id(weight_prop_id)
                                .map_or(default_val, |v| v.as_i64_lossless().unwrap())
                        },
                        edge_fn,
                    )
                }
                PropType::F32 => {
                    let default_val: OrderedFloat<_> = match default {
                        None => 1f32,
                        Some(v) => v.as_f32().ok_or_else(|| GraphError::InvalidValue {
                            reason: "Default value does not match property type".to_string(),
                        })?,
                    }
                    .into();
                    dijkstra_inner(
                        g,
                        source_node,
                        target_nodes,
                        |e| {
                            e.properties()
                                .get_by_id(weight_prop_id)
                                .map_or(default_val, |v| v.unwrap_f32().into())
                        },
                        edge_fn,
                    )
                }
                PropType::F64 => {
                    let default_val: OrderedFloat<_> = match default {
                        None => 1f64,
                        Some(v) => v.as_f64().ok_or_else(|| GraphError::InvalidValue {
                            reason: "Default value does not match property type".to_string(),
                        })?,
                    }
                    .into();
                    dijkstra_inner(
                        g,
                        source_node,
                        target_nodes,
                        |e| {
                            e.properties()
                                .get_by_id(weight_prop_id)
                                .map_or(default_val, |v| v.unwrap_f64().into())
                        },
                        edge_fn,
                    )
                }
                PropType::Decimal { .. } => {
                    let default_val = match default {
                        None => BigDecimal::from(1),
                        Some(v) => v.into_decimal().ok_or_else(|| GraphError::InvalidValue {
                            reason: "Default value does not match property type".to_string(),
                        })?,
                    };
                    dijkstra_inner(
                        g,
                        source_node,
                        target_nodes,
                        |e| {
                            e.properties()
                                .get_by_id(weight_prop_id)
                                .map_or(default_val.clone(), |v| v.unwrap_decimal())
                        },
                        edge_fn,
                    )
                }
                p_type => {
                    return Err(GraphError::InvalidProperty {
                        reason: format!("Weight type: {:?}, not supported", p_type),
                    })
                }
            }
        }
    };

    Ok(TypedNodeState::new_mapped(
        paths,
        DistanceState::node_transform,
    ))
}
