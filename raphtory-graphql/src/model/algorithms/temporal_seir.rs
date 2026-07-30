use crate::model::{
    algorithms::GqlExecutableAlgorithm,
    graph::{node_id::GqlNodeId, node_state::GqlNodeState, timeindex::GqlTimeInput},
};
use dynamic_graphql::OneOfInput;
use raphtory::{
    algorithms::dynamics::temporal::epidemics::{
        temporal_SEIR, IntoSeeds, Number, Probability, SeedError,
    },
    core::entities::VID,
    db::api::view::{DynamicGraph, StaticGraphViewOps},
    errors::GraphError,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use raphtory_api::core::utils::time::IntoTime;

/// How the initially infected nodes are chosen.
#[derive(OneOfInput, Clone)]
#[graphql(name = "Seeds")]
pub(crate) enum GqlSeeds {
    /// Infect exactly these nodes.
    Nodes(Vec<GqlNodeId>),
    /// Infect this many randomly chosen nodes.
    Number(usize),
    /// Infect this fraction of the nodes, chosen at random.
    Probability(f64),
}

impl IntoSeeds for GqlSeeds {
    fn into_initial_list<G: StaticGraphViewOps, R: Rng + ?Sized>(
        self,
        graph: &G,
        rng: &mut R,
    ) -> Result<Vec<VID>, SeedError> {
        match self {
            GqlSeeds::Nodes(nodes) => nodes.into_initial_list(graph, rng),
            GqlSeeds::Number(number) => Number(number).into_initial_list(graph, rng),
            GqlSeeds::Probability(probability) => {
                Probability::try_from(probability)?.into_initial_list(graph, rng)
            }
        }
    }
}

/// Temporal SEIR epidemic simulation, see [`temporal_SEIR`].
pub(crate) struct GqlTemporalSeir;

pub(crate) struct GqlTemporalSeirArgs {
    pub(crate) seeds: GqlSeeds,
    pub(crate) infection_prob: f64,
    pub(crate) initial_infection: GqlTimeInput,
    pub(crate) recovery_rate: Option<f64>,
    pub(crate) incubation_rate: Option<f64>,
    pub(crate) rng_seed: Option<u64>,
}

impl GqlExecutableAlgorithm for GqlTemporalSeir {
    type Args = GqlTemporalSeirArgs;
    type Output = GqlNodeState;

    fn execute(graph: &DynamicGraph, args: Self::Args) -> Result<Self::Output, GraphError> {
        let mut rng = match args.rng_seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_os_rng(),
        };
        let state = temporal_SEIR(
            graph,
            args.recovery_rate,
            args.incubation_rate,
            args.infection_prob,
            args.initial_infection.into_time(),
            args.seeds,
            &mut rng,
        )?;
        Ok(state.into())
    }
}
