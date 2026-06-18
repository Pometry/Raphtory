use crate::{
    model::schema::{
        cache::SchemaCache, property_schema::PropertySchema, SchemaAggregate, ENUM_BOUNDARY,
        MAX_DETAILED_SCHEMA_ENTITIES,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::edge::EdgeView},
    prelude::*,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef, properties::meta::PropMapper, LayerIds,
};
use std::{
    collections::{hash_map::Entry, HashSet},
    sync::Arc,
};

/// Describes edges between a specific pair of node types — the property and
/// metadata keys seen on such edges, along with their observed value types.
/// One `EdgeSchema` per `(srcType, dstType)` pair per layer.
#[derive(Clone, ResolvedObject)]
pub(crate) struct EdgeSchema<G: StaticGraphViewOps> {
    graph: G,
    layer: String,
    src_type: String,
    dst_type: String,
    // scan once and remember edges matching the (srcNodeType, dstNodeType)
    edges: Arc<[EdgeRef]>,
    // schema cache for the base graph, `None` for filtered/derived views
    cache: Option<Arc<SchemaCache>>,
}

impl<G: StaticGraphViewOps> EdgeSchema<G> {
    pub fn new(
        graph: G,
        layer: String,
        src_type: String,
        dst_type: String,
        edges: Vec<EdgeRef>,
        cache: Option<Arc<SchemaCache>>,
    ) -> Self {
        Self {
            graph,
            layer,
            src_type,
            dst_type,
            edges: edges.into(),
            cache,
        }
    }

    fn edges(&self) -> impl Iterator<Item = EdgeView<&G>> + '_ {
        self.edges.iter().map(|e| EdgeView::new(&self.graph, *e))
    }

    fn cache_key(&self) -> (String, String, String) {
        (
            self.layer.clone(),
            self.src_type.clone(),
            self.dst_type.clone(),
        )
    }
}

#[ResolvedObjectFields]
impl<G: StaticGraphViewOps> EdgeSchema<G> {
    /// Returns the type of source for these edges
    async fn src_type(&self) -> String {
        self.src_type.clone()
    }

    /// Returns the type of destination for these edges
    async fn dst_type(&self) -> String {
        self.dst_type.clone()
    }

    /// Returns the list of property schemas for edges matching these `(src_node_type, dst_node_type)`
    async fn properties(&self) -> Vec<PropertySchema> {
        if let Some(cache) = &self.cache {
            if let Some(hit) = cache.get_edge_properties(&self.cache_key()) {
                return hit.as_ref().clone();
            }
        }
        // not found in cache, so calculate it and cache it
        let cloned = self.clone();
        let result = blocking_compute(move || {
            if cloned.graph.unfiltered_num_edges(&LayerIds::All) > MAX_DETAILED_SCHEMA_ENTITIES {
                // large graph, do not collect detailed schema as it is expensive
                let visible: HashSet<usize> =
                    cloned.graph.edge_visible_temporal_prop_ids().collect();
                cloned
                    .graph
                    .edge_meta()
                    .temporal_prop_mapper()
                    .locked()
                    .iter_ids_and_types()
                    .filter(|(id, _, _)| visible.contains(id))
                    .map(|(_, name, dtype)| {
                        PropertySchema::new(name.to_string(), dtype.to_string(), vec![])
                    })
                    .collect()
            } else {
                let meta = cloned.graph.edge_meta();
                collect_schema(
                    cloned.edges().map(|edge| edge.properties()),
                    meta.temporal_prop_mapper(),
                )
            }
        })
        .await;
        if let Some(cache) = &self.cache {
            cache.store_edge_properties(self.cache_key(), Arc::new(result.clone()));
        }
        result
    }
    /// Returns the list of metadata schemas for edges matching these `(src_node_type, dst_node_type)`
    async fn metadata(&self) -> Vec<PropertySchema> {
        if let Some(cache) = &self.cache {
            if let Some(hit) = cache.get_edge_metadata(&self.cache_key()) {
                return hit.as_ref().clone();
            }
        }
        // not found in cache, so calculate it and cache it
        let cloned = self.clone();
        let result = blocking_compute(move || {
            if cloned.graph.unfiltered_num_edges(&LayerIds::All) > MAX_DETAILED_SCHEMA_ENTITIES {
                // large graph, do not collect detailed schema as it is expensive
                let visible: HashSet<usize> = cloned.graph.edge_visible_metadata_ids().collect();
                cloned
                    .graph
                    .edge_meta()
                    .metadata_mapper()
                    .locked()
                    .iter_ids_and_types()
                    .filter(|(id, _, _)| visible.contains(id))
                    .map(|(_, name, dtype)| {
                        PropertySchema::new(name.to_string(), dtype.to_string(), vec![])
                    })
                    .collect()
            } else {
                let meta = cloned.graph.edge_meta();
                collect_schema(
                    cloned.edges().map(|edge| edge.metadata()),
                    meta.metadata_mapper(),
                )
            }
        })
        .await;
        if let Some(cache) = &self.cache {
            cache.store_edge_metadata(self.cache_key(), Arc::new(result.clone()));
        }
        result
    }
}

/// Aggregate `(key, dtype) -> distinct values` across all edges, capping each value set at `ENUM_BOUNDARY`
fn collect_schema<P: PropertiesOps>(
    props_per_edge: impl Iterator<Item = P>,
    mapper: &PropMapper,
) -> Vec<PropertySchema> {
    let mut schema = SchemaAggregate::default();
    for props in props_per_edge {
        for ((key, value), id) in props.iter().zip(props.ids()) {
            let Some(value) = value else { continue };
            let key_with_prop_type = (
                key.to_string(),
                mapper
                    .get_dtype(id)
                    .expect("type for internal id should always exist")
                    .to_string(),
            );
            match schema.entry(key_with_prop_type) {
                Entry::Vacant(entry) => {
                    entry.insert(HashSet::from([value.to_string()]));
                }
                Entry::Occupied(mut entry) => {
                    let variants = entry.get_mut();
                    // An empty set means "too many variants" so we skip.
                    // Otherwise, there should always be at least 1 value in the set.
                    if !variants.is_empty() {
                        variants.insert(value.to_string());
                        if variants.len() > ENUM_BOUNDARY {
                            variants.clear();
                        }
                    }
                }
            }
        }
    }
    schema.into_iter().map(|prop| prop.into()).collect()
}
