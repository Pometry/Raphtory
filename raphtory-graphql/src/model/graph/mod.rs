use itertools::Itertools;
use raphtory::{
    prelude::{EdgeViewOps, NodeViewOps},
};

pub(crate) mod edge;
mod edges;
pub(crate) mod graph;
pub(crate) mod node;
mod nodes;
pub(crate) mod property;
pub(crate) mod vectorised_graph;

