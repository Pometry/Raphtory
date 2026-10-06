use crate::{
    db::{
        api::view::internal::{GraphView, Static},
        graph::views::{filter::model::CreateView, layer_graph::LayeredGraph},
    },
    errors::GraphError,
    prelude::LayerOps,
};
use raphtory_api::core::entities::Layer;
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

/// Every layer but the named ones, as `exclude_layers` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludeLayers<M> {
    pub layer: Layer,
    pub inner: M,
}

impl<M> Static for ExcludeLayers<M> {}

impl<M: Display> Display for ExcludeLayers<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EXCLUDE_LAYER[{}]({})",
            layer_label(&self.layer),
            self.inner
        )
    }
}

impl<M> ExcludeLayers<M> {
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

/// The default layer alone, as `default_layer` gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultLayer<M> {
    pub inner: M,
}

impl<M> Static for DefaultLayer<M> {}

impl<M: Display> Display for DefaultLayer<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DEFAULT_LAYER({})", self.inner)
    }
}

impl<M> DefaultLayer<M> {
    #[inline]
    pub fn new(entity: M) -> Self {
        Self { inner: entity }
    }
}

// ── expr-layer view construction ──

impl<T: CreateView> CreateView for ExcludeLayers<T> {
    type View<'graph, G: GraphView + 'graph> = LayeredGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        inner.exclude_layers(self.layer.clone())
    }
}

impl<T: CreateView> CreateView for DefaultLayer<T> {
    type View<'graph, G: GraphView + 'graph> = LayeredGraph<T::View<'graph, G>>;

    fn create_view<'graph, G: GraphView + 'graph>(
        &self,
        view: G,
    ) -> Result<Self::View<'graph, G>, GraphError> {
        let inner = self.inner.create_view(view)?;
        Ok(inner.default_layer())
    }
}
