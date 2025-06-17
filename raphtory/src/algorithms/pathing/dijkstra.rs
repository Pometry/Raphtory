/// Dijkstra's algorithm
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::StaticGraphViewOps};
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::{
        api::state::{Index, NodeState},
        graph::nodes::Nodes,
    },
    errors::GraphError,
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, Prop},
};
use indexmap::IndexSet;
use raphtory_api::core::{
    entities::{
        properties::prop::{PropType, PropUnwrap},
        VID,
    },
    Direction,
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

/// A state in the Dijkstra algorithm with a cost and a node name.
#[derive(PartialEq)]
struct State {
    cost: Prop,
    node: VID,
}

impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        other.cost.partial_cmp(&self.cost)
    }
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
pub fn dijkstra_single_source_shortest_paths<G: StaticGraphViewOps, T: AsNodeRef>(
    g: &G,
    source: T,
    targets: Vec<T>,
    weight: Option<&str>,
    direction: Direction,
) -> Result<NodeState<'static, (f64, Nodes<'static, G>), G>, GraphError> {
    let source_ref = source.as_node_ref();
    let source_node = match g.node(source_ref) {
        Some(src) => src,
        None => {
            let gid = match source_ref {
                NodeRef::Internal(vid) => g.node_id(vid),
                NodeRef::External(gid) => gid.to_owned(),
            };
            return Err(GraphError::NodeMissingError(gid));
        }
    };
    let mut weight_type = PropType::U8;
    if let Some(weight) = weight {
        let maybe_weight_type = match g.edge_meta().temporal_prop_meta().get_id(weight) {
            Some(weight_id) => g.edge_meta().temporal_prop_meta().get_dtype(weight_id),
            None => g
                .edge_meta()
                .const_prop_meta()
                .get_id(weight)
                .map(|weight_id| {
                    g.edge_meta()
                        .const_prop_meta()
                        .get_dtype(weight_id)
                        .unwrap()
                }),
        };
        match maybe_weight_type {
            None => {
                return Err(GraphError::PropertyMissingError(weight.to_string()));
            }
            Some(dtype) => {
                weight_type = dtype;
            }
        }
    }

    let mut target_nodes = vec![false; g.unfiltered_num_nodes()];
    for target in targets {
        if let Some(target_node) = g.node(target) {
            target_nodes[target_node.node.index()] = true;
        }
    }

    // Turn below into a generic function, then add a closure to ensure the prop is correctly unwrapped
    // after the calc is done
    let cost_val = match weight_type {
        PropType::F32 => Prop::F32(0f32),
        PropType::F64 => Prop::F64(0f64),
        PropType::U8 => Prop::U8(0u8),
        PropType::U16 => Prop::U16(0u16),
        PropType::U32 => Prop::U32(0u32),
        PropType::U64 => Prop::U64(0u64),
        PropType::I32 => Prop::I32(0i32),
        PropType::I64 => Prop::I64(0i64),
        p_type => {
            return Err(GraphError::InvalidProperty {
                reason: format!("Weight type: {:?}, not supported", p_type),
            })
        }
    };
    let max_val = match weight_type {
        PropType::F32 => Prop::F32(f32::MAX),
        PropType::F64 => Prop::F64(f64::MAX),
        PropType::U8 => Prop::U8(u8::MAX),
        PropType::U16 => Prop::U16(u16::MAX),
        PropType::U32 => Prop::U32(u32::MAX),
        PropType::U64 => Prop::U64(u64::MAX),
        PropType::I32 => Prop::I32(i32::MAX),
        PropType::I64 => Prop::I64(i64::MAX),
        p_type => {
            return Err(GraphError::InvalidProperty {
                reason: format!("Weight type: {:?}, not supported", p_type),
            })
        }
    };
    let mut heap = BinaryHeap::new();
    heap.push(State {
        cost: cost_val.clone(),
        node: source_node.node,
    });

    let mut dist: HashMap<VID, Prop> = HashMap::new();
    let mut predecessor: HashMap<VID, VID> = HashMap::new();
    let mut visited: HashSet<VID> = HashSet::new();
    let mut paths: HashMap<VID, (f64, IndexSet<VID, ahash::RandomState>)> = HashMap::new();

    dist.insert(source_node.node, cost_val.clone());

    while let Some(State {
        cost,
        node: node_vid,
    }) = heap.pop()
    {
        if target_nodes[node_vid.index()] && !paths.contains_key(&node_vid) {
            let mut path = IndexSet::default();
            path.insert(node_vid);
            let mut current_node_id = node_vid;
            while let Some(prev_node) = predecessor.get(&current_node_id) {
                path.insert(*prev_node);
                current_node_id = *prev_node;
            }
            path.reverse();
            paths.insert(node_vid, (cost.as_f64().unwrap(), path));
        }
        if !visited.insert(node_vid) {
            continue;
        }

        let edges = match direction {
            Direction::OUT => g.node(node_vid).unwrap().out_edges(),
            Direction::IN => g.node(node_vid).unwrap().in_edges(),
            Direction::BOTH => g.node(node_vid).unwrap().edges(),
        };

        // Replace this loop with your actual logic to iterate over the outgoing edges
        for edge in edges {
            let next_node_vid = edge.nbr().node;

            let edge_val = match weight {
                None => Prop::U8(1),
                Some(weight) => match edge.properties().get(weight) {
                    Some(prop) => prop,
                    _ => continue,
                },
            };

            let next_cost = cost.clone().add(edge_val).unwrap();
            if next_cost < *dist.entry(next_node_vid).or_insert(max_val.clone()) {
                heap.push(State {
                    cost: next_cost.clone(),
                    node: next_node_vid,
                });
                dist.insert(next_node_vid, next_cost);
                predecessor.insert(next_node_vid, node_vid);
            }
        }
    }
    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = paths
        .into_iter()
        .map(|(id, (cost, path))| {
            let nodes = Nodes::new_filtered(g.clone(), g.clone(), Some(Index::new(path)), None);
            (id, (cost, nodes))
        })
        .unzip();
    Ok(NodeState::new(
        g.clone(),
        g.clone(),
        values.into(),
        Some(Index::new(index)),
    ))
}

#[cfg(test)]
mod dijkstra_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::*,
        test_storage,
    };

    fn load_graph(edges: Vec<(i64, &str, &str, Vec<(&str, f32)>)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }
        graph
    }

    fn basic_graph() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4.0f32)]),
            (1, "A", "C", vec![("weight", 4.0f32)]),
            (2, "B", "C", vec![("weight", 2.0f32)]),
            (3, "C", "D", vec![("weight", 3.0f32)]),
            (4, "C", "E", vec![("weight", 1.0f32)]),
            (5, "C", "F", vec![("weight", 6.0f32)]),
            (6, "D", "F", vec![("weight", 2.0f32)]),
            (7, "E", "F", vec![("weight", 3.0f32)]),
        ])
    }

    #[test]
    fn test_dijkstra_multiple_targets() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );

            let results = results.unwrap();

            assert_eq!(results.get_by_node("D").unwrap().0, 7.0f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().0, 8.0f64);
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 5.0f64);
            assert_eq!(results.get_by_node("E").unwrap().0, 3.0f64);
            assert_eq!(results.get_by_node("F").unwrap().0, 6.0f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_no_weight() {
        let graph = basic_graph();

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["C", "E", "F"];
            let results =
                dijkstra_single_source_shortest_paths(graph, "A", targets, None, Direction::OUT)
                    .unwrap();
            assert_eq!(results.get_by_node("C").unwrap().1.name(), vec!["A", "C"]);
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["A", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_multiple_targets_node_ids() {
        let edges = vec![
            (0, 1, 2, vec![("weight", 4u64)]),
            (1, 1, 3, vec![("weight", 4u64)]),
            (2, 2, 3, vec![("weight", 2u64)]),
            (3, 3, 4, vec![("weight", 3u64)]),
            (4, 3, 5, vec![("weight", 1u64)]),
            (5, 3, 6, vec![("weight", 6u64)]),
            (6, 4, 6, vec![("weight", 2u64)]),
            (7, 5, 6, vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();
        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets = vec![4, 6];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                1,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("4").unwrap().1.name(),
                vec!["1", "3", "4"]
            );

            assert_eq!(results.get_by_node("6").unwrap().0, 8f64);
            assert_eq!(
                results.get_by_node("6").unwrap().1.name(),
                vec!["1", "3", "5", "6"]
            );

            let targets = vec![4, 5, 6];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                2,
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("4").unwrap().0, 5f64);
            assert_eq!(results.get_by_node("5").unwrap().0, 3f64);
            assert_eq!(results.get_by_node("6").unwrap().0, 6f64);
            assert_eq!(
                results.get_by_node("4").unwrap().1.name(),
                vec!["2", "3", "4"]
            );
            assert_eq!(
                results.get_by_node("5").unwrap().1.name(),
                vec!["2", "3", "5"]
            );
            assert_eq!(
                results.get_by_node("6").unwrap().1.name(),
                vec!["2", "3", "5", "6"]
            );
        });
    }

    #[test]
    fn test_dijkstra_multiple_targets_u64() {
        let edges = vec![
            (0, "A", "B", vec![("weight", 4u64)]),
            (1, "A", "C", vec![("weight", 4u64)]),
            (2, "B", "C", vec![("weight", 2u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
            (4, "C", "E", vec![("weight", 1u64)]),
            (5, "C", "F", vec![("weight", 6u64)]),
            (6, "D", "F", vec![("weight", 2u64)]),
            (7, "E", "F", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );

            assert_eq!(results.get_by_node("F").unwrap().0, 8f64);
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["A", "C", "E", "F"]
            );

            let targets: Vec<&str> = vec!["D", "E", "F"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "B",
                targets,
                Some("weight"),
                Direction::OUT,
            );
            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 5f64);
            assert_eq!(results.get_by_node("E").unwrap().0, 3f64);
            assert_eq!(results.get_by_node("F").unwrap().0, 6f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["B", "C", "D"]
            );
            assert_eq!(
                results.get_by_node("E").unwrap().1.name(),
                vec!["B", "C", "E"]
            );
            assert_eq!(
                results.get_by_node("F").unwrap().1.name(),
                vec!["B", "C", "E", "F"]
            );
        });
    }

    #[test]
    fn test_dijkstra_undirected() {
        let edges = vec![
            (0, "C", "A", vec![("weight", 4u64)]),
            (1, "A", "B", vec![("weight", 4u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D"];
            let results = dijkstra_single_source_shortest_paths(
                graph,
                "A",
                targets,
                Some("weight"),
                Direction::BOTH,
            );

            let results = results.unwrap();
            assert_eq!(results.get_by_node("D").unwrap().0, 7f64);
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );
        });
    }

    #[test]
    fn test_dijkstra_no_weight_undirected() {
        let edges = vec![
            (0, "C", "A", vec![("weight", 4u64)]),
            (1, "A", "B", vec![("weight", 4u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
        ];

        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let targets: Vec<&str> = vec!["D"];
            let results =
                dijkstra_single_source_shortest_paths(graph, "A", targets, None, Direction::BOTH)
                    .unwrap();
            assert_eq!(
                results.get_by_node("D").unwrap().1.name(),
                vec!["A", "C", "D"]
            );
        });
    }
}
