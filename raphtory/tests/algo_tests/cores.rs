#[cfg(test)]
mod k_core_test {
    use raphtory::{algorithms::cores::k_core::k_core_set, prelude::*};
    use std::collections::HashSet;

    use crate::test_storage;

    #[test]
    fn k_core_2() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        test_storage!(&graph, |graph| {
            let result = k_core_set(graph, 2, usize::MAX, None);
            let subgraph = graph.subgraph(result.clone());
            let actual = vec!["1", "3", "4", "5", "6", "8", "9", "10", "11"]
                .into_iter()
                .map(|k| k.to_string())
                .collect::<HashSet<String>>();

            assert_eq!(actual, subgraph.nodes().name().collect::<HashSet<String>>());
        });
    }
}
