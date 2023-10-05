/// Dijkstra's algorithm
use crate::{
    core::{entities::vertices::input_vertex::InputVertex, PropUnwrap},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

/// A state in the Dijkstra algorithm with a cost and a vertex name.
#[derive(Eq, PartialEq)]
struct State {
    cost: u64,
    vertex: String,
}

impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        Some(self.cmp(other))
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
) -> HashMap<String, (u64, Vec<String>)> {
    println!("A");
    let source_vertex = match graph.vertex(source) {
        Some(src) => src,
        None => return HashMap::new(),
    };
    println!("b");
    let target_nodes: Vec<String> = targets
        .iter()
        .filter_map(|p| match graph.has_vertex(p.clone()) {
            true => Some(graph.vertex(p.clone())?.name()),
            false => None,
        })
        .collect();
    println!("c");
    let mut heap = BinaryHeap::new();
    heap.push(State {
        cost: 0,
        vertex: source_vertex.name(),
    });
    println!("d");
    let mut dist: HashMap<String, u64> = HashMap::new();
    let mut predecessor: HashMap<String, String> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut paths: HashMap<String, (u64, Vec<String>)> = HashMap::new();
    println!("e");
    dist.insert(source_vertex.name(), 0);
    println!("f");
    while let Some(State {
        cost,
        vertex: vertex_name,
    }) = heap.pop()
    {
        println!("g");
        if target_nodes.contains(&vertex_name) && !paths.contains_key(&vertex_name) {
            let mut path = vec![vertex_name.clone()];
            let mut current_vertex_name = vertex_name.clone();
            while let Some(prev_vertex) = predecessor.get(&current_vertex_name) {
                path.push(prev_vertex.clone());
                current_vertex_name = prev_vertex.clone();
            }
            path.reverse();
            paths.insert(vertex_name.clone(), (cost, path));
        }
        println!("h");
        if !visited.insert(vertex_name.clone()) {
            continue;
        }
        println!("i");
        // Replace this loop with your actual logic to iterate over the outgoing edges
        for edge in graph.vertex(vertex_name.clone()).unwrap().out_edges() {
            println!("j");
            let next_vertex_name = edge.dst().name();
            let edge_val = match edge.properties().get(&weight) {
                Some(prop) => prop.unwrap_u64(),
                _ => 0,
            };
            let next_cost = cost + edge_val;
            println!("k");
            if next_cost < *dist.entry(next_vertex_name.clone()).or_insert(u64::MAX) {
                heap.push(State {
                    cost: next_cost,
                    vertex: next_vertex_name.clone(),
                });
                dist.insert(next_vertex_name.clone(), next_cost);
                predecessor.insert(next_vertex_name, vertex_name.clone());
            }
        }
    }
    paths
}

#[cfg(test)]
mod dijkstra_tests {
    use super::*;
    use crate::db::{api::mutation::AdditionOps, graph::graph::Graph};

    fn load_graph(edges: Vec<(i64, &str, &str, Vec<(&str, u64)>)>) -> Graph {
        let graph = Graph::new();

        for (t, src, dst, props) in edges {
            graph.add_edge(t, src, dst, props, None).unwrap();
        }
        graph
    }

    fn basic_graph() -> Graph {
        load_graph(vec![
            (0, "A", "B", vec![("weight", 4u64)]),
            (1, "A", "C", vec![("weight", 4u64)]),
            (2, "B", "C", vec![("weight", 2u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
            (4, "C", "E", vec![("weight", 1u64)]),
            (5, "C", "F", vec![("weight", 6u64)]),
            (6, "D", "F", vec![("weight", 2u64)]),
            (7, "E", "F", vec![("weight", 3u64)]),
        ])
    }

    #[test]
    fn test_dijkstra_multiple_targets() {
        let graph = basic_graph();

        let targets: Vec<&str> = vec!["D", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "A", targets, "weight".to_string());

        assert_eq!(results.get("D").unwrap().0, 7);
        assert_eq!(results.get("D").unwrap().1, vec!["A", "C", "D"]);

        assert_eq!(results.get("F").unwrap().0, 8);
        assert_eq!(results.get("F").unwrap().1, vec!["A", "C", "E", "F"]);

        let targets: Vec<&str> = vec!["D", "E", "F"];
        let results =
            dijkstra_single_source_shortest_paths(&graph, "B", targets, "weight".to_string());

        assert_eq!(results.get("D").unwrap().0, 5);
        assert_eq!(results.get("E").unwrap().0, 3);
        assert_eq!(results.get("F").unwrap().0, 6);
        assert_eq!(results.get("D").unwrap().1, vec!["B", "C", "D"]);
        assert_eq!(results.get("E").unwrap().1, vec!["B", "C", "E"]);
        assert_eq!(results.get("F").unwrap().1, vec!["B", "C", "E", "F"]);
    }
}
