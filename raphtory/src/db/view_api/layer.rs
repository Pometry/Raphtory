/// Trait defining layer operations
pub trait LayerOps {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType;

    /// Return a graph containing the layer `name`
    fn layer(&self, name: &str) -> Option<Self::LayeredViewType>;
}
