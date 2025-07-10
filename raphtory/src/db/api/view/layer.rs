use crate::{
    db::{
        api::view::internal::{BaseFilter, InternalLayerOps},
        graph::views::layer_graph::LayeredGraph,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{Layer, LayerIds, SingleLayer};
use raphtory_storage::core_ops::CoreGraphOps;

/// Trait defining layer operations
pub trait LayerOps<'graph> {
    type LayeredViewType;

    /// Return a graph containing only the default edge layer
    fn default_layer(&self) -> Self::LayeredViewType {
        self.valid_layers(Layer::Default)
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
    fn has_layer<L: SingleLayer>(&self, name: L) -> bool;

    /// Return a graph containing the layers in `names`. Any layers that do not exist are ignored.
    fn valid_layers<L: Into<Layer>>(&self, names: L) -> Self::LayeredViewType;

    /// Returns the number of layers
    fn num_layers(&self) -> usize;
}

impl<'graph, V: BaseFilter<'graph> + 'graph> LayerOps<'graph> for V {
    type LayeredViewType = V::Filtered<LayeredGraph<V::Current>>;

    fn default_layer(&self) -> Self::LayeredViewType {
        let layers = match self.current_filtered_graph().get_default_layer_id() {
            None => LayerIds::None,
            Some(layer) => {
                if self.current_filtered_graph().layer_ids().contains(&layer) {
                    LayerIds::One(layer)
                } else {
                    LayerIds::None
                }
            }
        };
        self.apply_filter(LayeredGraph::new(
            self.current_filtered_graph().clone(),
            layers,
        ))
    }

    fn layers<L: Into<Layer>>(&self, layers: L) -> Result<Self::LayeredViewType, GraphError> {
        let layers = layers.into();
        let ids = self.current_filtered_graph().layer_ids_from_names(layers)?;
        Ok(self.apply_filter(LayeredGraph::new(
            self.current_filtered_graph().clone(),
            ids,
        )))
    }

    fn exclude_layers<L: Into<Layer>>(
        &self,
        layers: L,
    ) -> Result<Self::LayeredViewType, GraphError> {
        let all_layer_ids = self.current_filtered_graph().layer_ids();
        let excluded_ids = self
            .current_filtered_graph()
            .layer_ids_from_names(layers.into())?;
        let included_ids = diff(
            all_layer_ids,
            self.current_filtered_graph().clone(),
            &excluded_ids,
        );

        Ok(self.apply_filter(LayeredGraph::new(
            self.current_filtered_graph().clone(),
            included_ids,
        )))
    }

    fn exclude_valid_layers<L: Into<Layer>>(&self, layers: L) -> Self::LayeredViewType {
        let all_layer_ids = self.current_filtered_graph().layer_ids();
        let excluded_ids = self
            .current_filtered_graph()
            .valid_layer_ids_from_names(layers.into());
        let included_ids = diff(
            all_layer_ids,
            self.current_filtered_graph().clone(),
            &excluded_ids,
        );

        self.apply_filter(LayeredGraph::new(
            self.current_filtered_graph().clone(),
            included_ids,
        ))
    }

    fn has_layer<L: SingleLayer>(&self, name: L) -> bool {
        !self
            .current_filtered_graph()
            .valid_layer_ids_from_names(name.into())
            .is_none()
    }

    fn valid_layers<L: Into<Layer>>(&self, names: L) -> Self::LayeredViewType {
        let layers = names.into();
        let ids = self
            .current_filtered_graph()
            .valid_layer_ids_from_names(layers);
        self.apply_filter(LayeredGraph::new(
            self.current_filtered_graph().clone(),
            ids,
        ))
    }

    fn num_layers(&self) -> usize {
        self.current_filtered_graph().unique_layers().count()
    }
}

pub fn diff<'a>(
    left: &LayerIds,
    graph: impl GraphViewOps<'a>,
    other: &LayerIds,
) -> LayerIds {
    match (left, other) {
        (LayerIds::None, _) => LayerIds::None,
        (this, LayerIds::None) => this.clone(),
        (_, LayerIds::All) => LayerIds::None,
        (LayerIds::One(id), other) => {
            if other.contains(id) {
                LayerIds::None
            } else {
                LayerIds::One(*id)
            }
        }
        (LayerIds::Multiple(ids), other) => {
            let ids: Vec<usize> = ids.iter().filter(|id| !other.contains(id)).collect();
            match ids.len() {
                0 => LayerIds::None,
                1 => LayerIds::One(ids[0]),
                _ => LayerIds::Multiple(ids.into()),
            }
        }
        (LayerIds::All, other) => {
            let all_layer_ids: Vec<usize> = graph
                .unique_layers()
                .map(|name| graph.get_layer_id(name.as_ref()).unwrap())
                .filter(|id| !other.contains(id))
                .collect();
            match all_layer_ids.len() {
                0 => LayerIds::None,
                1 => LayerIds::One(all_layer_ids[0]),
                _ => LayerIds::Multiple(all_layer_ids.into()),
            }
        }
    }
}
