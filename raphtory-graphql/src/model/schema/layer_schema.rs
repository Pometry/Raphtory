use crate::{
    model::schema::{
        cache::SchemaCache, collect_variants, property_schema::PropertySchema, MAX_EDGE_VARIANTS,
        MAX_DETAILED_SCHEMA_ENTITIES,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};
use raphtory_api::core::entities::{LayerId, LayerIds};
use std::sync::Arc;

/// Describes a single edge layer: its name and the edge property/metadata keys
/// present in it, with observed variants (property values) on small graphs
#[derive(Clone, ResolvedObject)]
pub(crate) struct LayerSchema<G: StaticGraphViewOps> {
    graph: G,
    layer_id: LayerId,
    // schema cache for the base graph, `None` for filtered/derived views
    cache: Option<Arc<SchemaCache>>,
}

impl<G: StaticGraphViewOps> LayerSchema<G> {
    pub fn new(graph: G, layer_id: LayerId, cache: Option<Arc<SchemaCache>>) -> Self {
        Self {
            graph,
            layer_id,
            cache,
        }
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
        if let Some(cache) = &self.cache {
            if let Some(hit) = cache.layer().get_properties(&self.layer_id) {
                return hit;
            }
        }
        let graph = self.graph.clone();
        let layer_id = self.layer_id;
        let result = blocking_compute(move || {
            if graph.unfiltered_num_edges(&LayerIds::One(layer_id)) > MAX_DETAILED_SCHEMA_ENTITIES {
                // too many edges to collect values: keys/types only, no variants
                return collect_layer_schema(&graph, layer_id, false);
            }
            let layer = graph.get_layer_name(layer_id);
            let layered = graph.valid_layers(layer);
            let mapper = graph.edge_meta().temporal_prop_mapper();
            collect_variants(
                layered.edges().into_iter().map(|e| e.properties()),
                mapper,
                MAX_EDGE_VARIANTS,
                |_| true,
            )
        })
        .await;
        if let Some(cache) = &self.cache {
            cache
                .layer()
                .store_properties(self.layer_id, result.clone());
        }
        result
    }

    /// Returns the list of metadata schemas present on edges in this layer
    pub async fn metadata(&self) -> Vec<PropertySchema> {
        if let Some(cache) = &self.cache {
            if let Some(hit) = cache.layer().get_metadata(&self.layer_id) {
                return hit;
            }
        }
        let graph = self.graph.clone();
        let layer_id = self.layer_id;
        let result = blocking_compute(move || {
            if graph.unfiltered_num_edges(&LayerIds::One(layer_id)) > MAX_DETAILED_SCHEMA_ENTITIES {
                return collect_layer_schema(&graph, layer_id, true);
            }
            let layer = graph.get_layer_name(layer_id);
            let layered = graph.valid_layers(layer);
            let mapper = graph.edge_meta().metadata_mapper();
            collect_variants(
                layered.edges().into_iter().map(|e| e.metadata()),
                mapper,
                MAX_EDGE_VARIANTS,
                |_| true,
            )
        })
        .await;
        if let Some(cache) = &self.cache {
            cache.layer().store_metadata(self.layer_id, result.clone());
        }
        result
    }
}

/// Get edge property/metadata keys and types using bitset without collecting values.
/// Redacted properties are handled by the GraphView (in edge_layer_has_*).
pub fn collect_layer_schema<G: StaticGraphViewOps>(
    graph: &G,
    layer_id: LayerId,
    metadata: bool,
) -> Vec<PropertySchema> {
    let meta = graph.edge_meta();
    let mapper = if metadata {
        meta.metadata_mapper()
    } else {
        meta.temporal_prop_mapper()
    };
    mapper
        .locked()
        .iter_ids_and_types()
        .filter(|(id, _, _)| {
            if metadata {
                graph.edge_layer_has_metadata(layer_id, *id)
            } else {
                graph.edge_layer_has_temporal_prop(layer_id, *id)
            }
        })
        .map(|(_, name, dtype)| PropertySchema::new(name.to_string(), dtype.clone(), vec![]))
        .collect()
}
