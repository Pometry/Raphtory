use crate::{
    model::schema::{
        cache::SchemaCache, property_schema::PropertySchema, ENUM_BOUNDARY,
        MAX_DETAILED_SCHEMA_ENTITIES,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};
use raphtory_api::core::entities::{properties::meta::PropMapper, LayerId, LayerIds};
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
            collect_variants(layered.edges().into_iter().map(|e| e.properties()), mapper)
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
            collect_variants(layered.edges().into_iter().map(|e| e.metadata()), mapper)
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

/// Collect the distinct values seen against each property id. Once a property
/// exceeds `ENUM_BOUNDARY` values we drop its set and report no variants for it.
pub fn collect_variants<P: PropertiesOps>(
    props_per_edge: impl Iterator<Item = P>,
    mapper: &PropMapper,
) -> Vec<PropertySchema> {
    // Vec is indexed by prop id; variants[0] returns the HashSet of variants for prop id 0
    // `None` = never seen, `Some(empty)` = seen but past ENUM_BOUNDARY
    let mut variants: Vec<Option<ahash::HashSet<String>>> = Vec::new();
    for props in props_per_edge {
        for id in props.ids() {
            let Some(value) = props.get_by_id(id) else {
                continue;
            };
            if variants.len() <= id {
                variants.resize_with(id + 1, || None);
            }
            match &mut variants[id] {
                slot @ None => *slot = Some(ahash::HashSet::from_iter([value.to_string()])),
                Some(seen) if !seen.is_empty() => {
                    seen.insert(value.to_string());
                    if seen.len() > ENUM_BOUNDARY {
                        seen.clear();
                    }
                }
                // already past the boundary, nothing left to record
                Some(_) => {}
            }
        }
    }

    // one read lock for every name and dtype, and prop id order for the output
    let locked = mapper.locked();
    locked
        .iter_ids_and_types()
        .filter_map(|(id, name, dtype)| {
            let seen = variants.get_mut(id).and_then(Option::take)?;
            let mut seen = Vec::from_iter(seen);
            seen.sort_unstable();
            Some(PropertySchema::new(name.to_string(), dtype.clone(), seen))
        })
        .collect()
}
