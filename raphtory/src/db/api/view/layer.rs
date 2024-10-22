use crate::db::{
    api::view::internal::{InternalLayerOps, OneHopFilter},
    graph::views::layer_graph::LayeredGraph,
};
use raphtory_api::core::{entities::Layer, utils::errors::GraphError};

/// Trait defining layer operations
pub trait LayerOps<'graph> {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType {
        self.layers(Layer::Default)
            .expect("Default layer not found")
    }

    /// Return a graph containing the layers in `names`. Errors if one or more of the layers do not exists.
    fn layers<L: Into<Layer>>(&self, names: L) -> Result<Self::LayeredViewType, GraphError>;

    /// Return a graph containing the excluded layers in `names`. Errors if one or more of the layers do not exists.
    fn exclude_layers<L: Into<Layer>>(
        &self,
        layers: L,
    ) -> Result<Self::LayeredViewType, GraphError>;

    fn exclude_valid_layers<L: Into<Layer>>(&self, layers: L) -> Self::LayeredViewType;

    /// Check if `name` is a valid layer name
    fn has_layer(&self, name: &str) -> bool;

    /// Return a graph containing the layers in `names`. Any layers that do not exist are ignored.
    fn valid_layers<L: Into<Layer>>(&self, names: L) -> Self::LayeredViewType;
}

impl<'graph, V: OneHopFilter<'graph> + 'graph> LayerOps<'graph> for V {
    type LayeredViewType = V::Filtered<LayeredGraph<V::FilteredGraph>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        self.one_hop_filtered(LayeredGraph::new(self.current_filter().clone(), 0.into()))
    }

    fn layers<L: Into<Layer>>(&self, layers: L) -> Result<Self::LayeredViewType, GraphError> {
        let layers = layers.into();
        let ids = self.current_filter().layer_ids_from_names(layers)?;
        Ok(self.one_hop_filtered(LayeredGraph::new(self.current_filter().clone(), ids)))
    }

    fn exclude_layers<L: Into<Layer>>(
        &self,
        layers: L,
    ) -> Result<Self::LayeredViewType, GraphError> {
        let all_layer_ids = self.current_filter().layer_ids();
        let excluded_ids = self.current_filter().layer_ids_from_names(layers.into())?;
        let included_ids = all_layer_ids.diff(self.current_filter().clone(), &excluded_ids);

        Ok(self.one_hop_filtered(LayeredGraph::new(
            self.current_filter().clone(),
            included_ids,
        )))
    }

    fn exclude_valid_layers<L: Into<Layer>>(&self, layers: L) -> Self::LayeredViewType {
        let all_layer_ids = self.current_filter().layer_ids();
        let excluded_ids = self
            .current_filter()
            .valid_layer_ids_from_names(layers.into());
        let included_ids = all_layer_ids.diff(self.current_filter().clone(), &excluded_ids);

        self.one_hop_filtered(LayeredGraph::new(
            self.current_filter().clone(),
            included_ids,
        ))
    }

    fn has_layer(&self, name: &str) -> bool {
        !self
            .current_filter()
            .valid_layer_ids_from_names(name.into())
            .is_none()
    }

    fn valid_layers<L: Into<Layer>>(&self, names: L) -> Self::LayeredViewType {
        let layers = names.into();
        let ids = self.current_filter().valid_layer_ids_from_names(layers);
        self.one_hop_filtered(LayeredGraph::new(self.current_filter().clone(), ids))
    }
}
