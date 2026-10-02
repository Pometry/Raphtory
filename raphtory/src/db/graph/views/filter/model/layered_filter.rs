use crate::{
    db::{
        api::{
            state::ops::filter::NodeExistsOp,
            view::internal::{GraphView, Static},
        },
        graph::views::{
            filter::{
                model::{
                    edge_expr::ops::EdgeExistsOp, graph_filter::GraphFilterOps, ComposableFilter,
                    InternalViewWrapOps,
                },
                CreateFilter,
            },
            layer_graph::LayeredGraph,
        },
    },
    errors::GraphError,
    prelude::LayerOps,
};
use raphtory_api::core::{entities::Layer, storage::timeindex::EventTime};
use std::{fmt, fmt::Display};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layered<M> {
    pub layer: Layer,
    pub inner: M,
}

impl<M> Static for Layered<M> {}

impl<M: Display> Display for Layered<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LAYER[{}]({})", layer_label(&self.layer), self.inner)
    }
}

/// The layer selection as a reader would write it: the names themselves,
/// `*` for every layer, `none` for no layer.
pub(crate) fn layer_label(layer: &Layer) -> String {
    match layer {
        Layer::All => "*".to_string(),
        Layer::None => "none".to_string(),
        Layer::Default => "_default".to_string(),
        Layer::One(name) => name.to_string(),
        Layer::Multiple(names) => names
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    }
}

impl<M> Layered<M> {
    #[inline]
    pub fn new(layer: Layer, entity: M) -> Self {
        Self {
            layer,
            inner: entity,
        }
    }

    #[inline]
    pub fn from_layers<L: Into<Layer>>(layer: L, entity: M) -> Self {
        Self::new(layer.into(), entity)
    }
}

impl<T: InternalViewWrapOps> InternalViewWrapOps for Layered<T> {
    type Window = Layered<T::Window>;

    fn bounds(&self) -> (EventTime, EventTime) {
        self.inner.bounds()
    }

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Layered::new(self.layer, self.inner.build_window(start, end))
    }
}

/// A view wrapper applied as a filter: the inner filter's view is applied to the
/// graph and this view on top of it, in the order the chain was written. The nodes
/// and edges it selects are the ones that exist in the resulting view.
impl<T: GraphFilterOps> CreateFilter for Layered<T> {
    type FilteredGraph<'graph, G>
        = LayeredGraph<T::FilteredGraph<'graph, G>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NodeExistsOp<LayeredGraph<T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<LayeredGraph<T::FilteredGraph<'graph, G>>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(self
            .inner
            .create_graph_filter(graph)?
            .layers(self.layer.clone())?)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(self.create_graph_filter(graph)?))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(EdgeExistsOp::new(self.create_graph_filter(graph)?))
    }
}

impl<T: ComposableFilter> ComposableFilter for Layered<T> {}

// ── expr layer: the layer view scopes any inner expression (per-expression view) ──
// Nesting order of chained views is pinned by the view-semantics tests.
