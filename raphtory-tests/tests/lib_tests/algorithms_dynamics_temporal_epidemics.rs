#[cfg(test)]
mod test {
    use rand::{distr::Distribution, rngs::SmallRng, Rng, SeedableRng};
    use rand_distr::Exp;
    use raphtory::{
        algorithms::dynamics::temporal::epidemics::{temporal_SEIR, Number},
        prelude::*,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use rayon::prelude::*;
    use stats::{mean, stddev};
    use tracing::info;

    fn correct_res(x: f64) -> f64 {
        (1176. * x.powi(10)
            + 8540. * x.powi(9)
            + 26602. * x.powi(8)
            + 45169. * x.powi(7)
            + 46691. * x.powi(6)
            + 31573. * x.powi(5)
            + 14585. * x.powi(4)
            + 4637. * x.powi(3)
            + 977. * x.powi(2)
            + 123. * x
            + 7.)
            / (168. * x.powi(10)
                + 1316. * x.powi(9)
                + 4578. * x.powi(8)
                + 9303. * x.powi(7)
                + 12215. * x.powi(6)
                + 10815. * x.powi(5)
                + 6531. * x.powi(4)
                + 2653. * x.powi(3)
                + 693. * x.powi(2)
                + 105. * x
                + 7.)
    }

    fn generate_contact_times<R: Rng + ?Sized>(n: usize, rng: &mut R, r: f64) -> Vec<i64> {
        let dist = Exp::new(r).unwrap();
        let values: Vec<_> = (0..n)
            .scan(0, |v, _| {
                let new_v: f64 = dist.sample(rng);
                let floor_v = new_v.floor();
                let new_v = if rng.random_bool(new_v - floor_v) {
                    new_v.ceil() as i64
                } else {
                    floor_v as i64
                };
                *v += new_v;
                Some(*v)
            })
            .collect();
        values
    }

    fn generate_graph<R: Rng + ?Sized>(n: usize, r: f64, rng: &mut R) -> Graph {
        let g = Graph::new();
        let edges = [
            (1, 4),
            (1, 5),
            (1, 6),
            (2, 4),
            (2, 5),
            (3, 7),
            (4, 6),
            (5, 7),
            (6, 7),
        ];
        for (v1, v2) in edges {
            let times = generate_contact_times(n, rng, r);
            for t in times {
                g.add_edge(t, v1, v2, NO_PROPS, None).unwrap();
                g.add_edge(t, v2, v1, NO_PROPS, None).unwrap();
            }
        }
        g
    }

    fn inner_test(event_rate: f64, recovery_rate: f64, p: f64) {
        let num_tries = 100;
        let inner_tries = 100;
        let scaled_infection_rate = event_rate * p / recovery_rate;

        let actual: Vec<_> = (0..num_tries)
            .into_par_iter()
            .map(|i| {
                let mut rng = SmallRng::seed_from_u64(i);
                let g = generate_graph(1000, event_rate, &mut rng);
                mean((0..inner_tries).map(move |_| {
                    temporal_SEIR(&g, Some(recovery_rate), None, p, 0, Number(1), &mut rng)
                        .unwrap()
                        .len()
                }))
            })
            .collect();
        let mean = mean(actual.iter().copied());
        let dev = stddev(actual.iter().copied()) / (num_tries as f64).sqrt();
        let expected = correct_res(scaled_infection_rate);
        info!("mean: {mean}, expected: {expected}, dev: {dev},  infection rate: {scaled_infection_rate}");
        assert!((mean - expected).abs() < 2. * dev)
    }

    #[test]
    fn test_small_graph_medium() {
        global_info_logger();
        let event_rate = 0.00000001;
        let recovery_rate = 0.000000001;
        let p = 0.3;

        inner_test(event_rate, recovery_rate, p);
    }

    #[test]
    fn test_small_graph_high() {
        global_info_logger();
        let event_rate = 0.00000001;
        let recovery_rate = 0.000000001;
        let p = 0.7;

        inner_test(event_rate, recovery_rate, p);
    }

    #[test]
    fn test_small_graph_low() {
        global_info_logger();
        let event_rate = 0.00000001;
        let recovery_rate = 0.00000001;
        let p = 0.1;

        inner_test(event_rate, recovery_rate, p);
    }
}
