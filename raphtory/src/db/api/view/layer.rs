use crate::core::ArcStr;
use std::sync::Arc;

/// Trait defining layer operations
pub trait LayerOps {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType {
        self.layer(Layer::Default).expect("Default layer not found")
    }

    /// Return a graph containing the layer `name`
    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType>;
}

#[derive(Debug, Clone)]
pub enum Layer {
    All,
    Default,
    One(ArcStr),
    Multiple(Arc<[String]>),
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<Option<&'a T>> for Layer {
    fn from(name: Option<&'a T>) -> Self {
        match name {
            Some(name) => Layer::One(name.to_owned().into()),
            None => Layer::All,
        }
    }
}

impl From<Option<String>> for Layer {
    fn from(value: Option<String>) -> Self {
        match value {
            Some(name) => Layer::One(name.into()),
            None => Layer::All,
        }
    }
}

impl From<ArcStr> for Layer {
    fn from(value: ArcStr) -> Self {
        Layer::One(value)
    }
}

impl From<String> for Layer {
    fn from(value: String) -> Self {
        Layer::One(value.into())
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<&'a T> for Layer {
    fn from(name: &'a T) -> Self {
        Layer::One(name.to_owned().into())
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<Vec<&'a T>> for Layer {
    fn from(names: Vec<&'a T>) -> Self {
        match names.len() {
            0 => Layer::All,
            1 => Layer::One(names[0].to_owned().into()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.to_owned())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

impl From<Vec<String>> for Layer {
    fn from(names: Vec<String>) -> Self {
        match names.len() {
            0 => Layer::All,
            1 => Layer::One(names.into_iter().next().expect("exists").into()),
            _ => Layer::Multiple(names.into()),
        }
    }
}
