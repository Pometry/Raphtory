use std::fmt::{Debug, Formatter};

use itertools::Itertools;

use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds},
        utils::errors::GraphError,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{
                Base, EdgeFilterOps, Immutable, InheritCoreOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritTimeSemantics, InternalLayerOps, Static,
            },
            Layer,
        },
    },
    prelude::GraphViewOps,
};

#[derive(Clone)]
pub struct LayeredGraph<G> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layers: LayerIds,
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for LayeredGraph<G> {}

impl<G> Static for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph> + Debug> Debug for LayeredGraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredGraph")
            .field("graph", &self.graph)
            .field("layers", &self.layers)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for LayeredGraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritListOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for LayeredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for LayeredGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids) && edge.has_layer(&self.layers)
    }
}

impl<'graph, G: GraphViewOps<'graph>> LayeredGraph<G> {
    pub fn new(graph: G, layers: LayerIds) -> Self {
        Self { graph, layers }
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

impl<'graph, G: GraphViewOps<'graph>> InternalLayerOps for LayeredGraph<G> {
    fn layer_ids(&self) -> &LayerIds {
        &self.layers
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        Ok(self.constrain(self.graph.layer_ids_from_names(key)?))
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.constrain(self.graph.valid_layer_ids_from_names(key))
    }
}

#[cfg(test)]
mod test_layers {
    use itertools::Itertools;

    use crate::prelude::*;

    #[test]
    fn test_layer_node() {
        let g = Graph::new();

        g.add_edge(0, 1, 2, NO_PROPS, Some("layer1")).unwrap();
        g.add_edge(0, 2, 3, NO_PROPS, Some("layer2")).unwrap();
        g.add_edge(3, 2, 4, NO_PROPS, Some("layer1")).unwrap();
        let neighbours = g
            .layers(vec!["layer1", "layer2"])
            .unwrap()
            .node(1)
            .unwrap()
            .neighbours()
            .into_iter()
            .collect_vec();
        assert_eq!(
            neighbours[0]
                .layers("layer2")
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(2, 3)]
        );
        assert_eq!(
            g.layers("layer2")
                .unwrap()
                .node(neighbours[0].name())
                .unwrap()
                .edges()
                .id()
                .collect_vec(),
            vec![(2, 3)]
        );
        let mut edges = g
            .layers("layer1")
            .unwrap()
            .node(neighbours[0].name())
            .unwrap()
            .edges()
            .id()
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = g.layers("layer1").unwrap().edges().id().collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 4)]);
        let mut edges = g
            .layers(vec!["layer1", "layer2"])
            .unwrap()
            .edges()
            .id()
            .collect_vec();
        edges.sort();
        assert_eq!(edges, vec![(1, 2), (2, 3), (2, 4)]);
    }

    #[test]
    fn layering_tests() {
        let g = Graph::new();
        let e1 = g.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        g.add_edge(1, 1, 2, NO_PROPS, Some("2")).unwrap();
        let e = g.edge(1, 2).unwrap();

        // FIXME: this is weird, see issue #1458
        assert!(e1.has_layer("2"));
        assert!(e1.layers("2").unwrap().history().is_empty());

        // layers with non-existing layers errors
        assert!(e.layers(["1", "3"]).is_err());
        // valid_layers ignores non-existing layers
        assert_eq!(
            e.valid_layers(["1", "3"]).layer_names().collect_vec(),
            ["1"]
        );
        assert!(e.has_layer("1"));
        assert!(e.has_layer("2"));
        assert!(!e.has_layer("3"));
        assert!(e.valid_layers("1").has_layer("1"));
        assert!(!e.valid_layers("1").has_layer("2"));
    }
}
