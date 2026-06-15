use crate::{
    model::schema::{
        merge_schemas, property_schema::PropertySchema, SchemaAggregate,
        MAX_DETAILED_SCHEMA_ENTITIES,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::edge::EdgeView},
    prelude::*,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef, properties::meta::PropMapper, LayerIds,
};
use std::{collections::HashSet, sync::Arc};

/// Describes edges between a specific pair of node types — the property and
/// metadata keys seen on such edges, along with their observed value types.
/// One `EdgeSchema` per `(srcType, dstType)` pair per layer.
#[derive(Clone, ResolvedObject)]
pub(crate) struct EdgeSchema<G: StaticGraphViewOps> {
    graph: G,
    src_type: String,
    dst_type: String,
    // scan once and remember edges matching the (srcType, dstType)
    edges: Arc<[EdgeRef]>,
}

impl<G: StaticGraphViewOps> EdgeSchema<G> {
    pub fn new(graph: G, src_type: String, dst_type: String, edges: Vec<EdgeRef>) -> Self {
        Self {
            graph,
            src_type,
            dst_type,
            edges: edges.into(),
        }
    }

    fn edges(&self) -> impl Iterator<Item = EdgeView<&G>> + '_ {
        self.edges.iter().map(|e| EdgeView::new(&self.graph, *e))
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
                let schema: SchemaAggregate = cloned
                    .edges()
                    .map(collect_edge_property_schema)
                    .reduce(merge_schemas)
                    .unwrap_or_default();
                schema.into_iter().map(|prop| prop.into()).collect_vec()
            }
        })
        .await
    }
    /// Returns the list of metadata schemas for edges matching these `(src_node_type, dst_node_type)`
    async fn metadata(&self) -> Vec<PropertySchema> {
        let cloned = self.clone();
        blocking_compute(move || {
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
                let schema: SchemaAggregate = cloned
                    .edges()
                    .map(collect_edge_metadata_schema)
                    .reduce(merge_schemas) // FIXME: Stop scanning all properties, take the first (or last) 20
                    .unwrap_or_default();
                schema.into_iter().map(|prop| prop.into()).collect_vec()
            }
        })
        .await
    }
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
