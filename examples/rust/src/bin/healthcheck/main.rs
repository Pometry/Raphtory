fn main() {}

#[cfg(test)]
mod test {
    use std::{
        fmt::Debug,
        path::{Path, PathBuf},
    };

    use itertools::Itertools;
    use raphtory::algorithms::connected_components::weakly_connected_components;
    use raphtory::core::Direction;
    use raphtory::db::{
        graph::Graph,
        view_api::*,
        view_api::{internal::GraphViewInternalOps, GraphViewOps},
    };
    use raphtory_io::graph_loader::source::csv_loader::CsvLoader;
    use serde::de::DeserializeOwned;

    trait TestEdge {
        fn src(&self) -> u64;
        fn dst(&self) -> u64;
        fn t(&self) -> i64;
    }

    fn load<REC: DeserializeOwned + TestEdge + Debug>(g1: &Graph, gn: &Graph, p: PathBuf) {
        CsvLoader::new(p)
            .set_delimiter(" ")
            .load_into_graph(&(g1, gn), |pair: REC, (g1, gn)| {
                g1.add_edge(pair.t(), pair.src(), pair.dst(), &vec![], None);
                gn.add_edge(pair.t(), pair.src(), pair.dst(), &vec![], None);
            })
            .expect("Failed to load graph from CSV files");
    }

    fn test_graph_sanity<P, REC: DeserializeOwned + TestEdge + Debug>(p: P, n_parts: usize)
    where
        P: Into<PathBuf>,
    {
        let path: PathBuf = p.into();
        let g1 = Graph::new(1);
        let gn = Graph::new(n_parts);

        load::<REC>(&g1, &gn, path);

        fn check_graphs<G: GraphViewOps>(g1: &G, gn: &G, n_parts: usize) {
            assert_eq!(g1.num_vertices(), gn.num_vertices());
            // NON-TEMPORAL TESTS HERE!

            let mut expect_1 = g1.vertices().id().collect::<Vec<_>>();
            let mut expect_n = gn.vertices().id().collect::<Vec<_>>();

            expect_1.sort();
            expect_n.sort();

            assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

            for v_ref in g1.vertices().id() {
                let v1 = g1.vertex(v_ref).unwrap().id();
                let vn = gn.vertex(v_ref).unwrap().id();
                assert_eq!(v1, vn, "Graphs are not equal {n_parts}");

                let v_id = v1;
                let v1 = g1.vertex(v_id).unwrap();
                let vn = gn.vertex(v_id).unwrap();
                let mut expect_1 = v1.neighbours().id().collect_vec();
                let mut expect_n = vn.neighbours().id().collect_vec();
                expect_1.sort();
                expect_n.sort();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                let mut expect_1 = v1.in_neighbours().id().collect_vec();
                let mut expect_n = vn.in_neighbours().id().collect_vec();
                expect_1.sort();
                expect_n.sort();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                let mut expect_1 = v1.out_neighbours().id().collect_vec();
                let mut expect_n = vn.out_neighbours().id().collect_vec();
                expect_1.sort();
                expect_n.sort();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                // now we test degrees
                let expect_1 = v1.degree();
                let expect_n = vn.degree();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                let expect_1 = v1.in_degree();
                let expect_n = vn.in_degree();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");

                let expect_1 = v1.out_degree();
                let expect_n = vn.out_degree();
                assert_eq!(expect_1, expect_n, "Graphs are not equal {n_parts}");
            }
        }

        check_graphs(&g1, &gn, n_parts);

        // TEMPORAL TESTS HERE!
        let t_start = 0;
        let t_end = 100;
        let g1_w = g1.window(t_start, t_end);
        let gn_w = gn.window(t_start, t_end);

        check_graphs(&g1_w, &gn_w, n_parts);
    }

    #[derive(serde::Deserialize, Debug)]
    struct Pair {
        src: u64,
        dst: u64,
        t: i64,
    }

    impl TestEdge for Pair {
        fn src(&self) -> u64 {
            self.src
        }

        fn dst(&self) -> u64 {
            self.dst
        }

        fn t(&self) -> i64 {
            self.t
        }
    }

    #[test]
    fn load_graph_from_cargo_path() {
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/", "test2.csv"]
            .iter()
            .collect();

        let p = Path::new(&csv_path);
        assert!(p.exists());

        for n_parts in 1..33 {
            test_graph_sanity::<&Path, Pair>(p, n_parts);
        }
    }

    #[derive(serde::Deserialize, Debug)]
    struct PairNoTime {
        src: u64,
        dst: u64,
    }

    impl TestEdge for PairNoTime {
        fn src(&self) -> u64 {
            self.src
        }

        fn dst(&self) -> u64 {
            self.dst
        }

        fn t(&self) -> i64 {
            1
        }
    }

    #[test]
    fn connected_components() {
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/", "test3.csv"]
            .iter()
            .collect();

        let p = Path::new(&csv_path);
        assert!(p.exists());
        let window = -100..100;

        for n_parts in 2..3 {
            let g1 = Graph::new(1);
            let gn = Graph::new(n_parts);
            load::<PairNoTime>(&g1, &gn, csv_path.clone());

            let iter_count = 50;
            let cc1 = weakly_connected_components(&g1, iter_count, None);
            let ccn = weakly_connected_components(&gn, iter_count, None);

            // get LCC
            let counts = cc1.iter().counts_by(|(_, cc)| cc);
            let max_1 = counts
                .into_iter()
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .rev()
                .take(1)
                .next();

            // get LCC
            let counts = ccn.iter().counts_by(|(_, cc)| cc);
            let max_n = counts
                .into_iter()
                .sorted_by(|l, r| l.1.cmp(&r.1))
                .rev()
                .take(1)
                .next();

            assert_eq!(max_1, Some((&6, 1039)));
            assert_eq!(max_1, max_n);
            println!("{:?}", max_1);
        }
    }
}
