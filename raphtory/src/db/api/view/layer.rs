use crate::{
    core::utils::errors::GraphError,
    db::{
        api::view::internal::{InternalLayerOps, OneHopFilter},
        graph::views::layer_graph::LayeredGraph,
    },
};
use raphtory_api::core::{entities::LayerIds, storage::arc_str::ArcStr};
use std::sync::Arc;

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
        let included_ids = diff(all_layer_ids, self.current_filter().clone(), &excluded_ids);

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
        let included_ids = diff(all_layer_ids, self.current_filter().clone(), &excluded_ids);

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

#[derive(Debug, Clone)]
pub enum Layer {
    All,
    None,
    Default,
    One(ArcStr),
    Multiple(Arc<[ArcStr]>),
}

trait SingleLayer {
    fn name(self) -> ArcStr;
}

impl<T: SingleLayer> From<T> for Layer {
    fn from(value: T) -> Self {
        Layer::One(value.name())
    }
}

impl SingleLayer for ArcStr {
    fn name(self) -> ArcStr {
        self
    }
}

impl SingleLayer for String {
    fn name(self) -> ArcStr {
        self.into()
    }
}

impl<'a, T: ToOwned<Owned = String> + ?Sized> SingleLayer for &'a T {
    fn name(self) -> ArcStr {
        self.to_owned().into()
    }
}

impl<T: SingleLayer> From<Vec<T>> for Layer {
    fn from(names: Vec<T>) -> Self {
        match names.len() {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

impl<T: SingleLayer, const N: usize> From<[T; N]> for Layer {
    fn from(names: [T; N]) -> Self {
        match N {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

pub fn diff<'a>(
    left: &LayerIds,
    graph: impl crate::prelude::GraphViewOps<'a>,
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
