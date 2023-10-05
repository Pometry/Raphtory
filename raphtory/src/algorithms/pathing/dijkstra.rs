/// Dijkstra's algorithm
use crate::{
    core::entities::vertices::input_vertex::InputVertex,
    core::PropType,
    prelude::Prop,
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

/// A state in the Dijkstra algorithm with a cost and a vertex name.
#[derive(PartialEq)]
struct State {
    cost: Prop,
    vertex: String, // TODO MOVE AWAY VERTEX FROM STRING INTO VERTEXVIEW
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
/// * `source`: The source vertex.
/// * `targets`: A vector of target vertices.
/// * `weight`: The name of the weight property for the edges.
///
/// # Returns
///
/// Returns a `HashMap` where the key is the target vertex and the value is a tuple containing
/// the total cost and a vector of vertices representing the shortest path.
///
pub fn dijkstra_single_source_shortest_paths<G: GraphViewOps, T: InputVertex>(
    graph: &G,
    source: T,
    targets: Vec<T>,
    weight: String,
) -> Result<HashMap<String, (Prop, Vec<String>)>, &'static str> {
    let source_vertex = match graph.vertex(source) {
        Some(src) => src,
        None => return Err("Source vertex not found"),
    };
    let weight_type = match graph.edge_meta().temporal_prop_meta().get_id(&weight) {
        Some(weight_id) => graph.edge_meta().temporal_prop_meta().get_dtype(weight_id),
        None => graph
            .edge_meta()
            .const_prop_meta()
            .get_id(&weight)
            .map(|weight_id| {
                graph
                    .edge_meta()
                    .const_prop_meta()
                    .get_dtype(weight_id)
                    .unwrap()
            }),
    };
    if weight_type.is_none() {
        return Err("Weight property not found on edges");
    }

    let target_nodes: Vec<String> = targets
        .iter()
        .filter_map(|p| match graph.has_vertex(p.clone()) {
            true => Some(graph.vertex(p.clone())?.name()),
            false => None,
        })
        .collect();

    // Turn below into a generic function, then add a closure to ensure the prop is correctly unwrapped
    // after the calc is done
    let cost_val = match weight_type.unwrap() {
        PropType::Empty => return Err("Weight type: Empty, not supported"),
        PropType::Str => return Err("Weight type: Str, not supported"),
        PropType::F32 => Prop::F32(0f32),
        PropType::F64 => Prop::F64(0f64),
        PropType::U8 => Prop::U8(0u8),
        PropType::U16 => Prop::U16(0u16),
        PropType::U32 => Prop::U32(0u32),
        PropType::U64 => Prop::U64(0u64),
        PropType::I32 => Prop::I32(0i32),
        PropType::I64 => Prop::I64(0i64),
        PropType::Bool => return Err("Weight type: Bool, not supported"),
        PropType::List => return Err("Weight type: List, not supported"),
        PropType::Map => return Err("Weight type: Map, not supported"),
        PropType::DTime => return Err("Weight type: DTime, not supported"),
        PropType::Graph => return Err("Weight type: Graph, not supported"),
    };
    let max_val = match weight_type.unwrap() {
        PropType::Empty => return Err("Weight type: Empty, not supported"),
        PropType::Str => return Err("Weight type: Str, not supported"),
        PropType::F32 => Prop::F32(f32::MAX),
        PropType::F64 => Prop::F64(f64::MAX),
        PropType::U8 => Prop::U8(u8::MAX),
        PropType::U16 => Prop::U16(u16::MAX),
        PropType::U32 => Prop::U32(u32::MAX),
        PropType::U64 => Prop::U64(u64::MAX),
        PropType::I32 => Prop::I32(i32::MAX),
        PropType::I64 => Prop::I64(i64::MAX),
        PropType::Bool => return Err("Weight type: Bool, not supported"),
        PropType::List => return Err("Weight type: List, not supported"),
        PropType::Map => return Err("Weight type: Map, not supported"),
        PropType::DTime => return Err("Weight type: DTime, not supported"),
        PropType::Graph => return Err("Weight type: Graph, not supported"),
    };
    let mut heap = BinaryHeap::new();
    heap.push(State {
        cost: cost_val.clone(),
        vertex: source_vertex.name(),
    });

    let mut dist: HashMap<String, Prop> = HashMap::new();
    let mut predecessor: HashMap<String, String> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut paths: HashMap<String, (Prop, Vec<String>)> = HashMap::new();

    dist.insert(source_vertex.name(), cost_val.clone());

    while let Some(State {
        cost,
        vertex: vertex_name,
    }) = heap.pop()
    {
        if target_nodes.contains(&vertex_name) && !paths.contains_key(&vertex_name) {
            let mut path = vec![vertex_name.clone()];
            let mut current_vertex_name = vertex_name.clone();
            while let Some(prev_vertex) = predecessor.get(&current_vertex_name) {
                path.push(prev_vertex.clone());
                current_vertex_name = prev_vertex.clone();
            }
            path.reverse();
            paths.insert(vertex_name.clone(), (cost.clone(), path));
        }
        if !visited.insert(vertex_name.clone()) {
            continue;
        }
        // Replace this loop with your actual logic to iterate over the outgoing edges
        for edge in graph.vertex(vertex_name.clone()).unwrap().out_edges() {
            let next_vertex_name = edge.dst().name();
            let edge_val = match edge.properties().get(&weight) {
                Some(prop) => prop,
                _ => continue,
            };
            let next_cost = cost.clone().add(edge_val).unwrap();
            if next_cost
                < *dist
                    .entry(next_vertex_name.clone())
                    .or_insert(max_val.clone())
            {
                heap.push(State {
                    cost: next_cost.clone(),
                    vertex: next_vertex_name.clone(),
                });
                dist.insert(next_vertex_name.clone(), next_cost);
                predecessor.insert(next_vertex_name, vertex_name.clone());
            }
        }
    }
    Ok(paths)
}

#[cfg(test)]
mod dijkstra_tests {
    use super::*;
    use crate::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::Prop,
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

        let targets: Vec<&str> = vec!["D", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "A", targets, "weight".to_string());

        let results = results.unwrap();

        assert_eq!(results.get("D").unwrap().0, Prop::F32(7.0f32));
        assert_eq!(results.get("D").unwrap().1, vec!["A", "C", "D"]);

        assert_eq!(results.get("F").unwrap().0, Prop::F32(8.0f32));
        assert_eq!(results.get("F").unwrap().1, vec!["A", "C", "E", "F"]);

        let targets: Vec<&str> = vec!["D", "E", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "B", targets, "weight".to_string());
        let results = results.unwrap();
        assert_eq!(results.get("D").unwrap().0, Prop::F32(5.0f32));
        assert_eq!(results.get("E").unwrap().0, Prop::F32(3.0f32));
        assert_eq!(results.get("F").unwrap().0, Prop::F32(6.0f32));
        assert_eq!(results.get("D").unwrap().1, vec!["B", "C", "D"]);
        assert_eq!(results.get("E").unwrap().1, vec!["B", "C", "E"]);
        assert_eq!(results.get("F").unwrap().1, vec!["B", "C", "E", "F"]);
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

        let targets: Vec<&str> = vec!["D", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "A", targets, "weight".to_string());
        let results = results.unwrap();
        assert_eq!(results.get("D").unwrap().0, Prop::U64(7u64));
        assert_eq!(results.get("D").unwrap().1, vec!["A", "C", "D"]);

        assert_eq!(results.get("F").unwrap().0, Prop::U64(8u64));
        assert_eq!(results.get("F").unwrap().1, vec!["A", "C", "E", "F"]);

        let targets: Vec<&str> = vec!["D", "E", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "B", targets, "weight".to_string());
        let results = results.unwrap();
        assert_eq!(results.get("D").unwrap().0, Prop::U64(5u64));
        assert_eq!(results.get("E").unwrap().0, Prop::U64(3u64));
        assert_eq!(results.get("F").unwrap().0, Prop::U64(6u64));
        assert_eq!(results.get("D").unwrap().1, vec!["B", "C", "D"]);
        assert_eq!(results.get("E").unwrap().1, vec!["B", "C", "E"]);
        assert_eq!(results.get("F").unwrap().1, vec!["B", "C", "E", "F"]);
    }
}
