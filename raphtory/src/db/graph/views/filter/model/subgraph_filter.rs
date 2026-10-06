use crate::{
    db::{
        api::{
            state::ops::filter::NodeTypeFilterOp,
            view::{
                internal::{GraphView, Static},
                GraphViewOps,
            },
        },
        graph::views::{
            filter::{model::CreateView, node_filtered_graph::NodeFilteredGraph},
            node_subgraph::NodeSubgraph,
            valid_graph::ValidGraph,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::GID;
use std::{fmt, fmt::Display};

/// Node ids as a comma-separated list.
pub(crate) fn id_list(ids: &[GID]) -> String {
    ids.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Every node but the named ones, as `exclude_nodes` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludeNodes<M> {
    pub nodes: Vec<GID>,
    pub inner: M,
}

impl<M> Static for ExcludeNodes<M> {}

impl<M: Display> Display for ExcludeNodes<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXCLUDE_NODES[{}]({})", id_list(&self.nodes), self.inner)
    }
}

impl<M> ExcludeNodes<M> {
    #[inline]
    pub fn new(nodes: Vec<GID>, entity: M) -> Self {
        Self {
            nodes,
            inner: entity,
        }
    }
}

/// The named nodes alone, as `subgraph` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subgraph<M> {
    pub nodes: Vec<GID>,
    pub inner: M,
}

impl<M> Static for Subgraph<M> {}

impl<M: Display> Display for Subgraph<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SUBGRAPH[{}]({})", id_list(&self.nodes), self.inner)
    }
}

impl<M> Subgraph<M> {
    #[inline]
    pub fn new(nodes: Vec<GID>, entity: M) -> Self {
        Self {
            nodes,
            inner: entity,
        }
    }
}

/// The nodes of the named types alone, as `subgraph_node_types` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubgraphNodeTypes<M> {
    pub node_types: Vec<String>,
    pub inner: M,
}

impl<M> Static for SubgraphNodeTypes<M> {}

impl<M: Display> Display for SubgraphNodeTypes<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SUBGRAPH_NODE_TYPES[{}]({})",
            self.node_types.join(", "),
            self.inner
        )
    }
}

impl<M> SubgraphNodeTypes<M> {
    #[inline]
    pub fn new(node_types: Vec<String>, entity: M) -> Self {
        Self {
            node_types,
            inner: entity,
        }
    }
}

/// The valid edges alone, as `valid` gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Valid<M> {
    pub inner: M,
}

impl<M> Static for Valid<M> {}

impl<M: Display> Display for Valid<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VALID({})", self.inner)
    }
}

impl<M> Valid<M> {
    #[inline]
    pub fn new(entity: M) -> Self {
        Self { inner: entity }
    }
}

// ── expr-layer view construction ──
//
// Each of these views can hide a node or an edge the incoming view shows, so
// they keep the default `narrows() == true`: a term read through one is `None`
// for an entity outside it.

impl<T: CreateView> CreateView for ExcludeNodes<T> {
    type View<'graph, G: GraphView + 'graph> = NodeSubgraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.exclude_nodes(&self.nodes))
    }
}

impl<T: CreateView> CreateView for Subgraph<T> {
    type View<'graph, G: GraphView + 'graph> = NodeSubgraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.subgraph(&self.nodes))
    }
}

impl<T: CreateView> CreateView for SubgraphNodeTypes<T> {
    type View<'graph, G: GraphView + 'graph> =
        NodeFilteredGraph<T::View<'graph, G>, NodeTypeFilterOp>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.subgraph_node_types(&self.node_types))
    }
}

impl<T: CreateView> CreateView for Valid<T> {
    type View<'graph, G: GraphView + 'graph> = ValidGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.valid())
    }
}
