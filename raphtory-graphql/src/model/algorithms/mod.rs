//! Statically defined graph algorithms exposed through `Graph.algorithm`.

pub(crate) mod alternating_mask;
pub(crate) mod bipartite;
pub(crate) mod centrality;
pub(crate) mod community_detection;
pub(crate) mod components;
pub(crate) mod dynamics;
pub(crate) mod embeddings;
pub(crate) mod executable;
pub(crate) mod inputs;
pub(crate) mod layout;
pub(crate) mod metrics;
pub(crate) mod motifs;
pub(crate) mod pathing;
pub(crate) mod resolvers;

pub(crate) use executable::{GqlAlgorithms, GqlExecutableAlgorithm};
pub(crate) use inputs::{filtered_view, GqlDirection};
