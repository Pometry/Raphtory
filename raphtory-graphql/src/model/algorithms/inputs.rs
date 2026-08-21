//! Shared GraphQL argument types for algorithms, and their conversion into the
//! core types the algorithms take.

use crate::model::graph::node_id::GqlNodeId;
use dynamic_graphql::{Enum, InputObject, OneOfInput};
use rand::Rng;
use raphtory::{
    algorithms::{
        dynamics::temporal::epidemics::{IntoSeeds, Number, Probability, SeedError},
        pathing::scored_paths::{EntityScore, PropertyScore, ScoringMap},
    },
    db::api::view::StaticGraphViewOps,
};
use raphtory_api::core::{entities::VID, Direction};

/// Edge direction to follow during traversal.
#[derive(Enum, Copy, Clone)]
#[graphql(name = "Direction")]
pub(crate) enum GqlDirection {
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

/// Maps one value of a property to a score.
#[derive(InputObject, Clone)]
#[graphql(name = "CategoryScore")]
pub(crate) struct GqlCategoryScore {
    /// The property value to match, as a string.
    value: String,
    /// Score contributed when the property has this value.
    score: f64,
}

/// How one property contributes to the score of an edge or node.
#[derive(InputObject, Clone)]
#[graphql(name = "PropertyScore")]
pub(crate) struct GqlPropertyScore {
    /// Name of the property to read. Temporal properties are checked first, then metadata.
    name: String,
    /// Multiplier applied to a numeric property value. Defaults to 1. Ignored when `categories` is set.
    scale: Option<f64>,
    /// Scores per property value. When set, the property is scored by lookup rather than by its
    /// numeric value.
    categories: Option<Vec<GqlCategoryScore>>,
    /// Score contributed when the property is absent, is not numeric, or its value is not listed in
    /// `categories`. Defaults to 0.
    default: Option<f64>,
}

impl From<GqlPropertyScore> for PropertyScore {
    fn from(score: GqlPropertyScore) -> Self {
        Self {
            name: score.name,
            scale: score.scale.unwrap_or(1.0),
            categories: score.categories.map(|categories| {
                categories
                    .into_iter()
                    .map(|category| (category.value, category.score))
                    .collect()
            }),
            default: score.default.unwrap_or(0.0),
        }
    }
}

/// Scoring rules for one edge layer or one node type.
#[derive(InputObject, Clone)]
#[graphql(name = "EntityScore")]
pub(crate) struct GqlEntityScore {
    /// Name of the edge layer, or of the node type.
    name: String,
    /// Base score contributed by every edge in this layer, or every node of this type. Defaults to 0.
    weight: Option<f64>,
    /// Property contributions, summed on top of `weight`.
    properties: Option<Vec<GqlPropertyScore>>,
}

impl GqlEntityScore {
    fn into_entry(self) -> (String, EntityScore) {
        (
            self.name,
            EntityScore {
                weight: self.weight.unwrap_or(0.0),
                properties: self
                    .properties
                    .unwrap_or_default()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
    }
}

/// Rules applied to edge layers or node types with no entry of their own.
#[derive(InputObject, Clone)]
#[graphql(name = "DefaultEntityScore")]
pub(crate) struct GqlDefaultEntityScore {
    /// Base score. Defaults to 0.
    weight: Option<f64>,
    /// Property contributions, summed on top of `weight`.
    properties: Option<Vec<GqlPropertyScore>>,
}

impl From<GqlDefaultEntityScore> for EntityScore {
    fn from(score: GqlDefaultEntityScore) -> Self {
        Self {
            weight: score.weight.unwrap_or(0.0),
            properties: score
                .properties
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

/// How to score each edge layer and each node type when searching for scored paths.
#[derive(InputObject, Clone)]
#[graphql(name = "ScoringMap")]
pub(crate) struct GqlScoringMap {
    /// Scoring rules per edge layer.
    layers: Option<Vec<GqlEntityScore>>,
    /// Rules applied to layers with no entry in `layers`. Unlisted layers score 0 when unset.
    default_layer: Option<GqlDefaultEntityScore>,
    /// Scoring rules per node type.
    node_types: Option<Vec<GqlEntityScore>>,
    /// Rules applied to node types with no entry in `nodeTypes`. Unlisted types score 0 when unset.
    default_node_type: Option<GqlDefaultEntityScore>,
    /// Only traverse layers listed in `layers`. Defaults to false.
    skip_unscored_layers: Option<bool>,
    /// Only route through node types listed in `nodeTypes`. Defaults to false.
    skip_unscored_node_types: Option<bool>,
}

impl From<GqlScoringMap> for ScoringMap {
    fn from(scoring: GqlScoringMap) -> Self {
        Self {
            layers: scoring
                .layers
                .unwrap_or_default()
                .into_iter()
                .map(GqlEntityScore::into_entry)
                .collect(),
            default_layer: scoring.default_layer.map(Into::into),
            node_types: scoring
                .node_types
                .unwrap_or_default()
                .into_iter()
                .map(GqlEntityScore::into_entry)
                .collect(),
            default_node_type: scoring.default_node_type.map(Into::into),
            skip_unscored_layers: scoring.skip_unscored_layers.unwrap_or(false),
            skip_unscored_node_types: scoring.skip_unscored_node_types.unwrap_or(false),
        }
    }
}
