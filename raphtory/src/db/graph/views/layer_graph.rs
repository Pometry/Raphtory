use crate::{
    core::entities::{edges::edge_store::EdgeStore, LayerIds},
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{
                Base, EdgeFilter, EdgeFilterOps, Immutable, InheritCoreOps, InheritGraphOps,
                InheritMaterialize, InheritTimeSemantics, InternalLayerOps,
            },
            Layer,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct LayeredGraph<G: GraphViewOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,

    edge_filter: EdgeFilter,
}

impl<G: GraphViewOps> Immutable for LayeredGraph<G> {}

impl<G: GraphViewOps + Debug> Debug for LayeredGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredGraph")
            .field("graph", &self.graph)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<G: GraphViewOps> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<G: GraphViewOps> InheritTimeSemantics for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritCoreOps for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritMaterialize for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritPropertiesOps for LayeredGraph<G> {}

impl<G: GraphViewOps> InheritGraphOps for LayeredGraph<G> {}

impl<G: GraphViewOps> EdgeFilterOps for LayeredGraph<G> {
    #[inline]
    fn edge_filter(&self) -> Option<&EdgeFilter> {
        Some(&self.edge_filter)
    }
}

impl<G: GraphViewOps> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        let edge_filter: EdgeFilter = match graph.edge_filter().cloned() {
            None => Arc::new(|e, l| e.has_layer(l)),
            Some(f) => Arc::new(move |e, l| e.has_layer(l) && f(e, l)),
        };
        Self {
            graph,
            layers,
            edge_filter,
        }
    }

    /// Get the intersection between the previously requested layers and the layers of
    /// this view
    fn constrain(&self, layers: LayerIds) -> LayerIds {
        match layers {
            LayerIds::None => LayerIds::None,
            LayerIds::All => self.layers.clone(),
            _ => match &self.layers {
                LayerIds::All => layers,
                LayerIds::One(id) => match layers.find(*id) {
                    Some(layer) => LayerIds::One(layer),
                    None => LayerIds::None,
                },
                LayerIds::Multiple(ids) => {
                    // intersect the layers
                    let new_layers = ids.iter().filter_map(|id| layers.find(*id)).collect_vec();
                    match new_layers.len() {
                        0 => LayerIds::None,
                        1 => LayerIds::One(new_layers[0]),
                        _ => LayerIds::Multiple(new_layers.into()),
                    }
                }
                LayerIds::None => LayerIds::None,
            },
        }
    }
}

impl<G: GraphViewOps> InternalLayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> LayerIds {
        self.layers.clone()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.constrain(self.graph.layer_ids_from_names(key))
    }

    fn edge_layer_ids(&self, e: &EdgeStore) -> LayerIds {
        let layer_ids = self.graph.edge_layer_ids(e);
        self.constrain(layer_ids)
    }
}

#[cfg(test)]
mod test_layers {
    use crate::prelude::*;
    use itertools::Itertools;
    #[test]
    fn test_layer_vertex() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(0, 2, 3, NO_PROPS, Some("layer2")).unwrap();
        g.add_edge(3, 2, 4, NO_PROPS, Some("layer1")).unwrap();
        let neighbours = g
            .layer(vec!["layer1", "layer2"])
            .unwrap()
            .vertex(1)
            .unwrap()
            .neighbours()
            .into_iter()
            .collect_vec();
        assert_eq!(
            neighbours[0]
                .layer("layer2")
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(2, 3)]
        );
        assert_eq!(
            g.layer("layer2")
                .unwrap()
                .vertex(neighbours[0].name())
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(2, 3)]
        );
        let mut edges = g
            .layer("layer1")
            .unwrap()
            .vertex(neighbours[0].name())
            .unwrap()
            .edges()
            .id()
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = g.layer("layer1").unwrap().edges().id().collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = g
            .layer(vec!["layer1", "layer2"])
            .unwrap()
            .edges()
            .id()
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 3), (2, 4)]);
    }
}
