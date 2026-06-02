use crate::{
    model::schema::{
        get_node_type, merge_schemas, property_schema::PropertySchema, SchemaAggregate,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::{properties::internal::EdgePropertySchemaOps, view::StaticGraphViewOps},
        graph::edge::EdgeView,
    },
    prelude::*,
};
use raphtory_api::core::entities::properties::{layer_schema::LayerPropSchema, meta::PropMapper};
use raphtory_storage::layer_ops::InternalLayerOps;
use std::collections::HashSet;

/// Describes edges between a specific pair of node types — the property and
/// metadata keys seen on such edges, along with their observed value types.
/// One `EdgeSchema` per `(srcType, dstType)` pair per layer.
#[derive(Clone, ResolvedObject)]
pub(crate) struct EdgeSchema<G: StaticGraphViewOps> {
    graph: G,
    src_type: String,
    dst_type: String,
}

impl<G: StaticGraphViewOps> EdgeSchema<G> {
    pub fn new(graph: G, src_type: String, dst_type: String) -> Self {
        Self {
            graph,
            src_type,
            dst_type,
        }
    }

    fn edges(&self) -> impl Iterator<Item = EdgeView<&G>> {
        (&&self.graph).edges().into_iter().filter(|&edge| {
            let src_type = get_node_type(edge.src());
            let dst_type = get_node_type(edge.dst());
            src_type == self.src_type && dst_type == self.dst_type
        })
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

    /// Returns the list of property schemas for edges connecting these types of nodes.
    ///
    /// Edges are filtered by `(src_type, dst_type)` and their temporal
    /// properties are aggregated into per-key value-variant sets (preserved
    /// up to `ENUM_BOUNDARY` distinct values per key, see `merge_schemas`).
    /// The resulting key set is intersected with the per-layer property
    /// bitset so anything not actually present in this layer is dropped.
    async fn properties(&self) -> Vec<PropertySchema> {
        let cloned = self.clone();
        blocking_compute(move || {
            let mapper = cloned.graph.edge_meta().temporal_prop_mapper();
            let layers = cloned.graph.layer_ids().clone();
            let layer_schema = cloned.graph.edge_layer_prop_schema(&layers);
            let aggregate: SchemaAggregate = cloned
                .edges()
                .map(collect_edge_property_schema)
                .reduce(merge_schemas)
                .unwrap_or_default();
            aggregate_to_property_list(aggregate, mapper, &layer_schema, PropKind::Temporal)
        })
        .await
    }
    /// Returns the list of metadata schemas for edges connecting these types of nodes.
    /// Same shape as `properties` but over metadata fields rather than
    /// temporal properties.
    async fn metadata(&self) -> Vec<PropertySchema> {
        let cloned = self.clone();
        blocking_compute(move || {
            let mapper = cloned.graph.edge_meta().metadata_mapper();
            let layers = cloned.graph.layer_ids().clone();
            let layer_schema = cloned.graph.edge_layer_prop_schema(&layers);
            let aggregate: SchemaAggregate = cloned
                .edges()
                .map(collect_edge_metadata_schema)
                .reduce(merge_schemas)
                .unwrap_or_default();
            aggregate_to_property_list(aggregate, mapper, &layer_schema, PropKind::Metadata)
        })
        .await
    }
}

#[derive(Copy, Clone)]
enum PropKind {
    Temporal,
    Metadata,
}

fn collect_schema<P: PropertiesOps>(props: P, mapper: &PropMapper) -> SchemaAggregate {
    props
        .iter()
        .zip(props.ids())
        .filter_map(|((key, value), id)| {
            let value = value?;
            let key_with_prop_type = (
                key.to_string(),
                mapper
                    .get_dtype(id)
                    .expect("type for internal id should always exist")
                    .to_string(),
            );
            Some((key_with_prop_type, HashSet::from([value.to_string()])))
        })
        .collect()
}

fn collect_edge_property_schema<'graph, G: GraphViewOps<'graph>>(
    edge: EdgeView<G>,
) -> SchemaAggregate {
    let props = edge.properties();
    let mapper = edge.graph.edge_meta().temporal_prop_mapper();
    collect_schema(props, mapper)
}

fn collect_edge_metadata_schema<'graph, G: GraphViewOps<'graph>>(
    edge: EdgeView<G>,
) -> SchemaAggregate {
    let props = edge.metadata();
    let mapper = edge.graph.edge_meta().metadata_mapper();
    collect_schema(props, mapper)
}

/// Convert an aggregate into the final `PropertySchema` list, dropping any
/// keys that aren't in the per-layer bitset. The intersection is the bitset
/// adaptation: edges of this `(src_type, dst_type)` may have surfaced
/// properties whose ids the layer bitset disagrees with (e.g. through a
/// filtered or redacted view) — those are removed here.
fn aggregate_to_property_list(
    aggregate: SchemaAggregate,
    mapper: &PropMapper,
    layer_schema: &LayerPropSchema,
    kind: PropKind,
) -> Vec<PropertySchema> {
    aggregate
        .into_iter()
        .filter(|((key, _dtype), _values)| {
            mapper
                .get_id(key)
                .map(|id| match kind {
                    PropKind::Temporal => layer_schema.contains_temporal(id),
                    PropKind::Metadata => layer_schema.contains_metadata(id),
                })
                .unwrap_or(false)
        })
        .map(|prop| prop.into())
        .collect_vec()
}
