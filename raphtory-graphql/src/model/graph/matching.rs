use crate::{
    model::graph::{edge::GqlEdge, edges::GqlEdges, node::GqlNode, node_id::GqlNodeId},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    algorithms::bipartite::max_weight_matching::Matching, db::api::view::DynamicGraph,
    prelude::NodeViewOps,
};
