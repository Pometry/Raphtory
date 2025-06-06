use crate::{
    core::{
        entities::{nodes::node_ref::AsNodeRef, VID},
        utils::time::TryIntoTime,
    },
    db::api::{
        state::{Index, NodeState},
        view::StaticGraphViewOps,
    },
    prelude::*,
};
use indexmap::IndexSet;
use rand::{distributions::Bernoulli, seq::IteratorRandom, Rng};
use rand_distr::{Distribution, Exp};
use raphtory_core::utils::time::ParseTimeError;
use std::{
    cmp::Reverse,
    collections::{hash_map::Entry, BinaryHeap, HashMap},
    fmt::Debug,
};

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq)]
pub struct Probability(f64);

impl Probability {
    pub fn sample<R: Rng + ?Sized>(self, rng: &mut R) -> bool {
        rng.gen_bool(self.0)
    }
}

pub struct Number(pub usize);

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Infected {
    pub infected: i64,
    pub active: i64,
    pub recovered: i64,
}

#[derive(thiserror::Error, Debug)]
pub enum SeedError {
    #[error("Invalid seed fraction")]
    InvalidFraction {
        #[from]
        source: ProbabilityError,
    },
    #[error("Invalid node {0}")]
    InvalidNode(String),

    #[error("Requested {num_seeds} seeds for graph with {num_nodes} nodes")]
    TooManyNodes { num_seeds: usize, num_nodes: usize },

    #[error("Invalid recovery rate")]
    InvalidRecoveryRate {
        #[from]
        source: rand_distr::ExpError,
    },

    #[error("Invalid initial time")]
    InvalidTime {
        #[from]
        source: ParseTimeError,
    },
}
#[allow(unused)]
trait NotIterator {}

impl NotIterator for f64 {}

pub trait IntoSeeds {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError>;
}

impl<I: IntoIterator<Item = V>, V: AsNodeRef + Debug> IntoSeeds for I {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        _rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        self.into_iter()
            .map(|v| {
                let description = format!("{:?}", v);
                (&graph)
                    .node(v)
                    .map(|node| node.node)
                    .ok_or(SeedError::InvalidNode(description))
            })
            .collect()
    }
}

impl IntoSeeds for Probability {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        _rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        Ok(graph.nodes().iter().map(|node| node.node).collect())
    }
}

impl IntoSeeds for Number {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        let Number(num_seeds) = self;
        let num_nodes = graph.count_nodes();
        if num_nodes < num_seeds {
            Err(SeedError::TooManyNodes {
                num_nodes,
                num_seeds,
            })
        } else {
            Ok(graph
                .nodes()
                .iter()
                .map(|node| node.node)
                .choose_multiple(rng, num_seeds))
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Invalid probability {0}")]
pub struct ProbabilityError(f64);

impl TryFrom<f64> for Probability {
    type Error = ProbabilityError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if (0. ..=1.).contains(&value) {
            Ok(Probability(value))
        } else {
            Err(ProbabilityError(value))
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Infection {
    time: i64,
    node: VID,
}

/// Simulated SEIR dynamics on temporal network
///
/// This algorithm is based on <https://arxiv.org/abs/2007.14386>
///
/// # Arguments
///
/// - `graph` - the graph
/// - `recovery_rate` - Optional recovery rate (actual recovery times are sampled from an exponential
///                     distribution with this rate). If `None`, nodes never recover (i.e. SI model)
/// - `incubation_rate` - Optional incubation rate (the time nodes take to transition from exposed to infected
///                       is sampled from an exponential distribution with this rate). If `None`,
///                       the incubation time is `1`, i.e., an infected nodes becomes infectious at
///                       the next time step
/// - `initial_infection` - time stamp for the initial infection events
/// - `seeds` - Specify how to choose seeds, can be either a list of nodes, `Number(n: usize)` for
///             sampling a fixed number `n` of seed nodes, or `Probability(p: f64)` in which case a node is initially infected with probability `p`.
/// - `rng` - The random number generator to use
///
/// # Returns
///
/// A [Result] wrapping an [AlgorithmResult] which contains a mapping of each vertex to an [Infected] object, which contains the following structure:
/// - `infected`: the time stamp of the infection event
/// - `active`: the time stamp at which the node actively starts spreading the infection (i.e., the end of the incubation period)
/// - `recovered`: the time stamp at which the node recovered (i.e., stopped spreading the infection)
///
#[allow(non_snake_case)]
pub fn temporal_SEIR<
    G: StaticGraphViewOps,
    P: TryInto<Probability>,
    S: IntoSeeds,
    R: Rng + ?Sized,
    T: TryIntoTime,
>(
    g: &G,
    recovery_rate: Option<f64>,
    incubation_rate: Option<f64>,
    infection_prob: P,
    initial_infection: T,
    seeds: S,
    rng: &mut R,
) -> Result<NodeState<'static, Infected, G>, SeedError>
where
    SeedError: From<P::Error>,
{
    let infection_prob = infection_prob.try_into()?;
    let seeds = seeds.into_initial_list(g, rng)?;
    let recovery_dist = recovery_rate.map(Exp::new).transpose()?;
    let incubation_dist = incubation_rate.map(Exp::new).transpose()?;
    let infection_dist = Bernoulli::new(infection_prob.0).unwrap();
    let initial_infection = initial_infection.try_into_time()?;
    let mut states: HashMap<VID, Infected> = HashMap::default();
    let mut event_queue: BinaryHeap<Reverse<Infection>> = seeds
        .into_iter()
        .map(|v| {
            Reverse(Infection {
                time: initial_infection,
                node: v,
            })
        })
        .collect();
    while !event_queue.is_empty() {
        let Reverse(next_event) = event_queue.pop().unwrap();
        if let Entry::Vacant(e) = states.entry(next_event.node) {
            // node not yet infected
            let node = g.node(next_event.node).unwrap();
            let incubation_time = incubation_dist
                .map(|dist| dist.sample(rng) as i64)
                .unwrap_or(1);
            let recovery_time = recovery_dist
                .map(|dist| dist.sample(rng) as i64)
                .unwrap_or(i64::MAX);
            let start_t = next_event.time.saturating_add(incubation_time);
            let end_t = start_t.saturating_add(recovery_time);
            e.insert(Infected {
                infected: next_event.time,
                active: start_t,
                recovered: end_t,
            });
            for e in node.window(start_t, end_t).out_edges() {
                let neighbour = e.dst().node;
                if !states.contains_key(&neighbour) {
                    for ee in e.explode() {
                        if infection_dist.sample(rng) {
                            event_queue.push(Reverse(Infection {
                                node: neighbour,
                                time: ee.time().unwrap(),
                            }));
                            break;
                        }
                    }
                }
            }
        }
    }
    let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = states.into_iter().unzip();
    Ok(NodeState::new(
        g.clone(),
        g.clone(),
        values.into(),
        Some(Index::new(index)),
    ))
}

#[cfg(test)]
mod test {
    use crate::{
        algorithms::dynamics::temporal::epidemics::{temporal_SEIR, Number},
        prelude::*,
    };
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    use rand_distr::{Distribution, Exp};
    use raphtory_api::core::utils::logging::global_info_logger;
    use rayon::prelude::*;
    use stats::{mean, stddev};
    #[cfg(feature = "storage")]
    use tempfile::TempDir;
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
                let new_v = if rng.gen_bool(new_v - floor_v) {
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

    #[cfg(feature = "storage")]
    #[test]
    fn compare_disk_with_in_mem() {
        let event_rate = 0.00000001;
        let recovery_rate = 0.000000001;
        let p = 0.3;

        let mut rng = SmallRng::seed_from_u64(0);
        let g = generate_graph(1000, event_rate, &mut rng);
        let test_dir = TempDir::new().unwrap();
        let disk_graph = g.persist_as_disk_graph(test_dir.path()).unwrap();
        let mut rng = SmallRng::seed_from_u64(0);
        let res_arrow = temporal_SEIR(
            &disk_graph,
            Some(recovery_rate),
            None,
            p,
            0,
            Number(1),
            &mut rng,
        )
        .unwrap();

        let mut rng = SmallRng::seed_from_u64(0);
        let res_mem =
            temporal_SEIR(&g, Some(recovery_rate), None, p, 0, Number(1), &mut rng).unwrap();

        assert!(res_mem
            .iter()
            .all(|(key, val)| res_arrow.get_by_node(key.id()).unwrap() == val));
    }
}
