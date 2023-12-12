use crate::{
    algorithms::algorithm_result::AlgorithmResult,
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::api::view::{
        internal::{CoreGraphOps, EdgeFilterOps, GraphOps, InternalLayerOps},
        StaticGraphViewOps,
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, TimeOps},
};
use rand::{distributions::Bernoulli, seq::IteratorRandom, Rng};
use rand_distr::{Distribution, Exp};
use std::{
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    ops::Range,
};

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq)]
pub struct Probability(pub f64);

impl Probability {
    pub fn sample<R: Rng + ?Sized>(self, rng: &mut R) -> bool {
        rng.gen_bool(self.0)
    }
}

pub struct Number(pub usize);

pub enum State {
    Susceptible,
    Infected {
        infected: i64,
        active: i64,
        recovered: i64,
    },
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
}

trait NotIterator {}

impl NotIterator for f64 {}

pub trait IntoSeeds {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError>;
}

impl<I: IntoIterator<Item = V>, V: Into<NodeRef> + Debug> IntoSeeds for I {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        _rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        self.into_iter()
            .map(|v| {
                let description = format!("{:?}", v);
                graph
                    .internal_node_ref(v.into(), &graph.layer_ids(), graph.edge_filter())
                    .ok_or_else(|| SeedError::InvalidNode(description))
            })
            .collect()
    }
}

impl IntoSeeds for Probability {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        Ok(graph
            .node_refs(graph.layer_ids(), graph.edge_filter())
            .filter(|_| self.sample(rng))
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
                .node_refs(graph.layer_ids(), graph.edge_filter())
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
        if 0. <= value && value <= 1. {
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

pub fn temporal_SEIR<
    G: StaticGraphViewOps,
    P: TryInto<Probability>,
    S: IntoSeeds,
    R: Rng + ?Sized,
>(
    graph: &G,
    recovery_rate: Option<f64>,
    incubation_rate: Option<f64>,
    infection_prob: P,
    initial_infection: i64,
    seeds: S,
    rng: &mut R,
) -> Result<HashMap<VID, State>, SeedError>
where
    SeedError: From<P::Error>,
{
    let infection_prob = infection_prob.try_into()?;
    let seeds = seeds.into_initial_list(graph, rng)?;
    let recovery_dist = recovery_rate.map(|r| Exp::new(r)).transpose()?;
    let incubation_dist = incubation_rate.map(|r| Exp::new(r)).transpose()?;
    let infection_dist = Bernoulli::new(infection_prob.0).unwrap();
    let mut states: HashMap<VID, State> = HashMap::default();
    let mut event_queue: BinaryHeap<Infection> = seeds
        .into_iter()
        .map(|v| Infection {
            time: initial_infection,
            node: v,
        })
        .collect();
    while !event_queue.is_empty() {
        let next_event = event_queue.pop().unwrap();
        if !states.contains_key(&next_event.node) {
            // node not yet infected
            let node = graph.node(next_event.node).unwrap();
            let incubation_time = incubation_dist
                .map(|dist| dist.sample(rng) as i64)
                .unwrap_or(1);
            let recovery_time = recovery_dist
                .map(|dist| dist.sample(rng) as i64)
                .unwrap_or(i64::MAX);
            let start_t = next_event.time.saturating_add(incubation_time);
            let end_t = start_t.saturating_add(recovery_time);
            states.insert(
                next_event.node,
                State::Infected {
                    infected: next_event.time,
                    active: start_t,
                    recovered: end_t,
                },
            );
            for e in node.window(start_t, end_t).out_edges() {
                let neighbour = e.dst().node;
                if !states.contains_key(&neighbour) {
                    for ee in e.explode() {
                        if infection_dist.sample(rng) {
                            event_queue.push(Infection {
                                node: neighbour,
                                time: ee.time().unwrap(),
                            });
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(states)
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
