use crate::{
    core::{entities::vertices::input_vertex::InputVertex, PropUnwrap},
    prelude::GraphViewOps,
};

use crate::prelude::{EdgeViewOps, VertexViewOps};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

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

pub fn dijkstra<G: GraphViewOps, T: InputVertex>(
    graph: &G,
    source: T,
    target: T,
    weight: String,
) -> Option<(u64, Vec<String>)> {
    // TODO: Change unwrap to something safer
    let source_vertex = graph.vertex(source).unwrap();
    let target_vertex = graph.vertex(target).unwrap();
    let mut heap = BinaryHeap::new();
    heap.push(State {
        cost: 0,
        vertex: source_vertex.clone().name(),
    });

    let mut dist: HashMap<String, u64> = HashMap::new();
    let mut predecessor: HashMap<String, String> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();

    dist.insert(source_vertex.name(), 0);

    while let Some(State {
        cost,
        vertex: vertex_name,
    }) = heap.pop()
    {
        if vertex_name == target_vertex.clone().name() {
            let mut path = vec![target_vertex.clone().name()];
            let mut current_vertex_name = target_vertex.name();
            while let Some(prev_vertex) = predecessor.get(&current_vertex_name) {
                path.push(prev_vertex.clone());
                current_vertex_name = prev_vertex.clone();
            }
            path.reverse();
            return Some((cost, path));
        }

        if !visited.insert(vertex_name.clone()) {
            continue;
        }

        for edge in graph.vertex(vertex_name.clone()).unwrap().out_edges() {
            let next_vertex_name = edge.dst().name();
            let next_cost = cost + edge.properties().get(&weight).unwrap().unwrap_u64();

            if next_cost < *dist.entry(next_vertex_name.clone()).or_insert(u64::MAX) as u64 {
                heap.push(State {
                    cost: next_cost,
                    vertex: next_vertex_name.clone(),
                });
                dist.insert(next_vertex_name.clone(), next_cost);
                predecessor.insert(next_vertex_name, vertex_name.clone());
            }
        }
    }

    None
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

    #[test]
    fn test_generic_taint_1() {
        let graph = load_graph(vec![
            (0, "A", "B", vec![("weight", 4u64)]),
            (1, "A", "C", vec![("weight", 4u64)]),
            (2, "B", "C", vec![("weight", 2u64)]),
            (3, "C", "D", vec![("weight", 3u64)]),
            (4, "C", "E", vec![("weight", 1u64)]),
            (5, "C", "F", vec![("weight", 6u64)]),
            (6, "D", "F", vec![("weight", 2u64)]),
            (7, "E", "F", vec![("weight", 3u64)]),
        ]);

        let results = dijkstra(&graph, "A", "B", "weight".to_string()).unwrap();
        println!("{:?}", results)
    }
}
