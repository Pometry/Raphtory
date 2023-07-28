use std::sync::Arc;

/// Trait defining layer operations
pub trait LayerOps {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType;

    /// Return a graph containing the layer `name`
    fn layer<L: Into<Layer>>(&self, name: L) -> Option<Self::LayeredViewType>;
}

#[derive(Debug)]
pub enum Layer {
    All,
    Default,
    One(String),
    Multiple(Arc<[String]>),
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<Option<&'a T>> for Layer {
    fn from(name: Option<&'a T>) -> Self {
        match name {
            Some(name) => Layer::One(name.to_owned()),
            None => Layer::All,
        }
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<&'a T> for Layer {
    fn from(name: &'a T) -> Self {
        Layer::One(name.to_owned())
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> From<Vec<&'a T>> for Layer {
    fn from(names: Vec<&'a T>) -> Self {
        match names.len() {
            0 => Layer::All,
            1 => Layer::One(names[0].to_owned()),
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
            1 => Layer::One(names[0].clone()),
            _ => Layer::Multiple(names.into()),
        }
    }
}
