use std::sync::Arc;

/// Trait defining layer operations
pub trait LayerOps {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType;

    /// Return a graph containing the layer `name`
    fn layer(&self, name: Layer) -> Option<Self::LayeredViewType>;
}


pub enum Layer<'a>{
    All,
    Default,
    One(&'a str),
    Multiple(Arc<[&'a str]>),
}

impl <'a> From<Option<&'a str>> for Layer<'a> {
    fn from(name: Option<&'a str>) -> Self {
        match name {
            Some(name) => Layer::One(name),
            None => Layer::All,
        }
    }
}

impl <'a> From<&'a str> for Layer<'a> {
    fn from(name: &'a str) -> Self {
        Layer::One(name)
    }
}

impl <'a> From<Vec<&'a str>> for Layer<'a> {
    fn from(names: Vec<&'a str>) -> Self {
        match names.len() {
            0 => Layer::All,
            1 => Layer::One(names[0]),
            _ => Layer::Multiple(names.into()),
        }
    }
}
