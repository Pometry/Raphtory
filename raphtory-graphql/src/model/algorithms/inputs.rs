//! Shared GraphQL argument types for algorithms, and their conversion into the
//! core types the algorithms take.

use crate::model::graph::node_id::GqlNodeId;
use dynamic_graphql::{Enum, OneOfInput};
use rand::Rng;
use raphtory::{
    algorithms::dynamics::temporal::epidemics::{IntoSeeds, Number, Probability, SeedError},
    db::api::view::StaticGraphViewOps,
};
use raphtory_api::core::{entities::VID, Direction};

/// Edge direction to follow during traversal.
#[derive(Enum, Copy, Clone)]
#[graphql(name = "Direction")]
pub enum GqlDirection {
    Out,
    In,
    Both,
}

impl From<GqlDirection> for Direction {
    fn from(direction: GqlDirection) -> Self {
        match direction {
            GqlDirection::Out => Direction::OUT,
            GqlDirection::In => Direction::IN,
            GqlDirection::Both => Direction::BOTH,
        }
    }
}

/// How the initially infected nodes are chosen.
#[derive(OneOfInput, Clone)]
#[graphql(name = "Seeds")]
pub enum GqlSeeds {
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
