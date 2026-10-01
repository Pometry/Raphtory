#[cfg(test)]
mod test {
    use raphtory::{
        db::api::{state::Index, view::internal::NodeList},
        prelude::*,
    };
    use itertools::Itertools;
    use rayon::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_indexed_nodes_respect_index() {
        let graph = Graph::new();
        for n in 0..6u64 {
            graph.add_node(0, n, NO_PROPS, None, None).unwrap();
        }
        graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        graph.add_edge(0, 2, 3, NO_PROPS, None).unwrap();

        for graph in [&graph.subgraph([0, 1, 2, 3, 4]), &graph.subgraph([0, 1, 2])] {
            let nodes = graph.nodes();
            let all = nodes.iter().map(|n| n.id()).collect_vec();

            // Restrict to every other node of the view.
            let picked = nodes.iter().step_by(2).map(|n| n.node).collect_vec();
            let expected = nodes
                .iter()
                .step_by(2)
                .map(|n| n.id())
                .collect::<BTreeSet<_>>();
            assert!(picked.len() < all.len(), "index should be a strict subset");

            let indexed = nodes.indexed(NodeList::from(Index::from_iter(picked)));
            assert_eq!(
                indexed.iter().map(|n| n.id()).collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(
                indexed.par_iter().map(|n| n.id()).collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(
                indexed
                    .collect()
                    .iter()
                    .map(|n| n.id())
                    .collect::<BTreeSet<_>>(),
                expected
            );
            assert_eq!(indexed.len(), expected.len());
        }
    }
}
