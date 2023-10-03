use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::entities::vertices::input_vertex::InputVertex, prelude::*,
};
use std::collections::HashMap;

pub fn single_source_shortest_path<G: GraphViewOps, T: InputVertex>(
    g: &G,
    source: T,
    cutoff: Option<usize>,
) -> AlgorithmResult<String, Vec<String>> {
    // if !g.has_vertex(source.clone()) {
    //     let h: Vec<(String, Vec<String>)> = Vec::new();
    //     return h;
    // }
    let source_node = g.vertex(source).unwrap();
    let mut level = 0;
    let mut nextlevel: HashMap<String, usize> = HashMap::new();
    nextlevel.insert(source_node.name(), 1);

    let mut paths: HashMap<String, Vec<String>> = HashMap::new();
    paths.insert(source_node.name(), vec![source_node.name()]);
    let results_type = std::any::type_name::<HashMap<String, Vec<String>>>();

    if let Some(0) = cutoff {
        return AlgorithmResult::new("Single Source Shortest Path", results_type, paths);
    }

    while !nextlevel.is_empty() {
        let thislevel: HashMap<String, usize> = nextlevel.clone();
        nextlevel.clear();

        for v in thislevel.keys() {
            for w in g.vertex(v.clone()).unwrap().neighbours() {
                if !paths.contains_key(&w.name()) {
                    let mut new_path = paths.get(v).unwrap().clone();
                    new_path.push(w.name());
                    paths.insert(w.name(), new_path);
                    nextlevel.insert(w.name(), 1);
                }
            }
        }
        level += 1;
        if let Some(c) = cutoff {
            if c <= level {
                break;
            }
        }
    }
    AlgorithmResult::new("Single Source Shortest Path", results_type, paths)
}

#[cfg(test)]
mod sssp_tests {
    use super::*;
    use crate::db::{api::mutation::AdditionOps, graph::graph::Graph};

    fn load_graph(edges: Vec<(i64, u64, u64)>) -> Graph {
        let graph = Graph::new();
        for (t, src, dst) in edges {
            graph.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }
        graph
    }

    #[test]
    fn test_sssp_1() {
        let graph = load_graph(vec![
            (0, 1, 2),
            (1, 1, 3),
            (2, 1, 4),
            (3, 2, 3),
            (4, 2, 4),
            (5, 3, 4),
            (6, 4, 4),
            (7, 4, 5),
            (8, 5, 6),
        ]);

        let binding = single_source_shortest_path(&graph, 1, Some(4));
        let results = binding.get_all();
        let expected = HashMap::from([
            ("1".to_string(), vec!["1".to_string()]),
            ("2".to_string(), vec!["1".to_string(), "2".to_string()]),
            ("3".to_string(), vec!["1".to_string(), "3".to_string()]),
            ("4".to_string(), vec!["1".to_string(), "4".to_string()]),
            (
                "5".to_string(),
                vec!["1".to_string(), "4".to_string(), "5".to_string()],
            ),
            (
                "6".to_string(),
                vec![
                    "1".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                ],
            ),
        ]);
        assert_eq!(results, &expected)
    }
}
