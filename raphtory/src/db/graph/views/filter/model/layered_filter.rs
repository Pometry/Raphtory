use crate::db::api::view::internal::Static;
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
