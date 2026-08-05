//! Shared GraphQL argument types for algorithms, and their conversion into the
//! core types the algorithms take.

use crate::model::graph::filtering::GqlViewFilter;
use dynamic_graphql::Enum;
use raphtory::{
    db::{
        api::view::{DynamicGraph, Filter, IntoDynamic},
        graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, DynView,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::Direction;

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

/// Applies an optional composite filter, returning the filtered view (or the
/// graph unchanged if no filter is given).
pub(crate) fn filtered_view(
    graph: &DynamicGraph,
    filter: Option<GqlViewFilter>,
) -> Result<DynamicGraph, GraphError> {
    let Some(filter) = filter else {
        return Ok(graph.clone());
    };
    let mut graph = graph.clone();
    if let Some(nodes) = filter.nodes {
        let nodes: CompositeNodeFilter = nodes.try_into()?;
        graph = graph.filter(nodes)?.into_dynamic();
    }
    if let Some(edges) = filter.edges {
        let edges: CompositeEdgeFilter = edges.try_into()?;
        graph = graph.filter(edges)?.into_dynamic();
    }
    if let Some(view) = filter.graph {
        let view: DynView = view.try_into()?;
        graph = graph.filter(view)?.into_dynamic();
    }
    Ok(graph)
}
