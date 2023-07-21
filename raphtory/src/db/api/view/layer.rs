use std::sync::Arc;

/// Trait defining layer operations
pub trait LayerOps {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType;

    /// Return a graph containing the layer `name`
    fn layer(&self, name: Layer) -> Option<Self::LayeredViewType>;
}

pub enum Layer {
    All,
    Default,
    One(String),
    Multiple(Arc<[String]>),
}

impl<'a> From<Option<&'a str>> for Layer {
    fn from(name: Option<&'a str>) -> Self {
        match name {
            Some(name) => Layer::One(name.to_string()),
            None => Layer::All,
        }
    }
}

impl<'a> From<&'a str> for Layer {
    fn from(name: &'a str) -> Self {
        Layer::One(name.to_string())
    }
}

impl<'a> From<Vec<&'a str>> for Layer {
    fn from(names: Vec<&'a str>) -> Self {
        match names.len() {
            0 => Layer::All,
            1 => Layer::One(names[0].to_string()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.to_string())
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
