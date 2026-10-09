#[cfg(test)]
mod test {
    use itertools::Itertools;
    use raphtory::{algorithms::bipartite::max_weight_matching::max_weight_matching, prelude::*};

    #[test]
    fn test() {
        let g = Graph::new();
        let vs = vec![(1, 2, 5), (2, 3, 11), (3, 4, 5)];
        for (src, dst, weight) in &vs {
            g.add_edge(0, *src, *dst, [("weight", Prop::I64(*weight))], None)
                .unwrap();
        }

        // Run max weight matching with max cardinality set to false
        let res = max_weight_matching(&g, Some("weight"), false, true);
        // Run max weight matching with max cardinality set to true
        let maxc_res = max_weight_matching(&g, Some("weight"), true, true);

        let matching = res;
        let maxc_matching = maxc_res;
        // Check output
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(2, 3));
        assert_eq!(maxc_matching.len(), 2);
        assert!(maxc_matching.contains(1, 2));
        assert!(maxc_matching.contains(3, 4));

        assert_eq!(matching.src(3).unwrap().id(), 2);
        assert_eq!(matching.src(2), None);

        assert_eq!(matching.dst(2).unwrap().id(), 3);
        assert_eq!(matching.dst(3), None);

        assert_eq!(matching.edge_for_src(2).unwrap(), g.edge(2, 3).unwrap());
        assert_eq!(matching.edge_for_src(1), None);

        assert_eq!(matching.edge_for_dst(3).unwrap(), g.edge(2, 3).unwrap());
        assert_eq!(matching.edge_for_dst(2), None);

        assert_eq!(matching.edges().collect(), vec![g.edge(2, 3).unwrap()]);
        assert_eq!(
            matching.edges_iter().collect_vec(),
            vec![g.edge(2, 3).unwrap()]
        );
    }
}
