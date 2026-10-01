use crate::{
    core::entities::{nodes::node_ref::AsNodeRef, VID},
    db::api::{
        state::{GenericNodeState, TypedNodeState},
        view::StaticGraphViewOps,
    },
    prelude::*,
};
use rand::{
    distr::{Bernoulli, Distribution},
    seq::IteratorRandom,
    Rng,
};
use rand_distr::Exp;
use raphtory_api::core::{
    storage::timeindex::AsTime,
    utils::time::{ParseTimeError, TryIntoTime},
};
use serde::{Deserialize, Serialize};
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
        rng.random_bool(self.0)
    }
}

pub struct Number(pub usize);

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
    /// Seeds each node independently with this probability (Bernoulli sampling)
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        Ok(graph
            .nodes()
            .iter()
            .filter_map(|node| self.sample(rng).then_some(node.node))
            .collect())
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
) -> Result<TypedNodeState<'static, Infected, G>, SeedError>
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
                time: initial_infection.t(),
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
                                time: ee.time().unwrap().t(),
                            }));
                            break;
                        }
                    }
                }
            }
        }
    }
    //let (index, values): (IndexSet<_, ahash::RandomState>, Vec<_>) = states.into_iter().unzip();
    Ok(TypedNodeState::new(GenericNodeState::new_from_map(
        g.clone(),
        states,
        |value| value,
        None,
    )))
}
