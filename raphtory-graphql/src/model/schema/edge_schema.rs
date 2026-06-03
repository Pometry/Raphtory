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

    /// Returns the list of property schemas for edges matching these `(src_node_type, dst_node_type)`
    async fn properties(&self) -> Vec<PropertySchema> {
        let cloned = self.clone();
        blocking_compute(move || {
            let layers = cloned.graph.layer_ids().clone();
            let layer_schema = cloned.graph.edge_layer_prop_schema(&layers);
            let schema: SchemaAggregate = cloned
                .edges()
                .map(|e| collect_edge_property_schema(e, &layer_schema, false))
                .reduce(merge_schemas)
                .unwrap_or_default();
            schema.into_iter().map(|prop| prop.into()).collect_vec()
        })
        .await
    }
    /// Returns the list of metadata schemas for edges matching these `(src_node_type, dst_node_type)`
    async fn metadata(&self) -> Vec<PropertySchema> {
        let cloned = self.clone();
        blocking_compute(move || {
            let layers = cloned.graph.layer_ids().clone();
            let layer_schema = cloned.graph.edge_layer_prop_schema(&layers);
            let schema: SchemaAggregate = cloned
                .edges()
                .map(|e| collect_edge_metadata_schema(e, &layer_schema, true))
                .reduce(merge_schemas)
                .unwrap_or_default();
            schema.into_iter().map(|prop| prop.into()).collect_vec()
        })
        .await
    }
}

fn collect_schema<P: PropertiesOps>(
    props: P,
    mapper: &PropMapper,
    layer_schema: &LayerPropSchema,
    is_metadata: bool,
) -> SchemaAggregate {
    props
        .iter()
        .zip(props.ids())
        .filter_map(|((key, value), id)| {
            // skip properties not in the layer schema
            // FIXME: Is this even necessary?
            // `edge.properties()` should only return properties present somewhere in the graph (by definition)
            if (is_metadata && !layer_schema.contains_metadata(id))
                || (!is_metadata && !layer_schema.contains_temporal(id))
            {
                return None;
            }

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
    layer_schema: &LayerPropSchema,
    is_metadata: bool,
) -> SchemaAggregate {
    let props = edge.properties();
    let mapper = edge.graph.edge_meta().temporal_prop_mapper();
    collect_schema(props, mapper, layer_schema, is_metadata)
}

fn collect_edge_metadata_schema<'graph, G: GraphViewOps<'graph>>(
    edge: EdgeView<G>,
    layer_schema: &LayerPropSchema,
    is_metadata: bool,
) -> SchemaAggregate {
    let props = edge.metadata();
    let mapper = edge.graph.edge_meta().metadata_mapper();
    collect_schema(props, mapper, layer_schema, is_metadata)
}
