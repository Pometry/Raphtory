use crate::{
    model::schema::{
        collect_no_variants, collect_variants, property_schema::PropertySchema, visible_ids_layer,
        MAX_DETAILED_SCHEMA_ENTITIES, MAX_VARIANTS,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::views::layer_graph::LayeredGraph},
    prelude::*,
};
use raphtory_api::core::entities::{LayerId, LayerIds};

/// Describes a single edge layer: its name and the edge property/metadata keys
/// present in it, with observed variants (property values) on small graphs
#[derive(ResolvedObject)]
pub(crate) struct LayerSchema<G: StaticGraphViewOps> {
    graph: G,
    layer_id: LayerId,
}

impl<G: StaticGraphViewOps> LayerSchema<G> {
    pub fn new(graph: G, layer_id: LayerId) -> Self {
        Self { graph, layer_id }
    }
}

#[ResolvedObjectFields]
impl<G: StaticGraphViewOps> LayerSchema<G> {
    /// Returns the name of the layer with this schema
    pub async fn name(&self) -> String {
        self.graph.get_layer_name(self.layer_id).to_string()
    }

    /// Returns the list of property schemas present on edges in this layer
    pub async fn properties(&self) -> Vec<PropertySchema> {
        let graph = self.graph.clone();
        let layer_id = self.layer_id;
        blocking_compute(move || {
            let mapper = graph.edge_meta().temporal_prop_mapper();
            let visible =
                visible_ids_layer(graph.edge_visible_temporal_prop_ids(), layer_id, mapper);

            if graph.unfiltered_num_edges(&LayerIds::One(layer_id)) > MAX_DETAILED_SCHEMA_ENTITIES {
                // too many edges to collect values: keys/types only, no variants
                collect_no_variants(mapper, &visible)
            } else {
                let layered = LayeredGraph::new(&graph, LayerIds::One(layer_id));
                collect_variants(
                    layered.edges().into_iter().map(|e| e.properties()),
                    mapper,
                    MAX_VARIANTS,
                    &visible,
                )
            }
        })
        .await
    }

    /// Returns the list of metadata schemas present on edges in this layer
    pub async fn metadata(&self) -> Vec<PropertySchema> {
        let graph = self.graph.clone();
        let layer_id = self.layer_id;
        blocking_compute(move || {
            let mapper = graph.edge_meta().metadata_mapper();
            let visible = visible_ids_layer(graph.edge_visible_metadata_ids(), layer_id, mapper);
            if graph.unfiltered_num_edges(&LayerIds::One(layer_id)) > MAX_DETAILED_SCHEMA_ENTITIES {
                collect_no_variants(mapper, &visible)
            } else {
                let layered = LayeredGraph::new(&graph, LayerIds::One(layer_id));
                collect_variants(
                    layered.edges().into_iter().map(|e| e.metadata()),
                    mapper,
                    MAX_VARIANTS,
                    &visible,
                )
            }
        })
        .await
    }
}
