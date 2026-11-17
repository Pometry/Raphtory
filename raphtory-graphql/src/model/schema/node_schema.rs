use crate::{
    model::schema::{
        collect_no_variants, collect_variants, property_schema::PropertySchema,
        visible_ids_any_layer, DEFAULT_NODE_TYPE, MAX_DETAILED_SCHEMA_ENTITIES, MAX_VARIANTS,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::{
            properties::internal::NodePropertySchemaOps, state::ops::filter::NodeTypeFilterOp,
            view::DynamicGraph,
        },
        graph::views::filter::node_filtered_graph::NodeFilteredGraph,
    },
    prelude::*,
};
use raphtory_api::core::entities::LayerIds;
use raphtory_storage::core_ops::CoreGraphOps;

/// Describes nodes of a specific type in a graph — its property keys and
/// observed value types (and, for string-valued properties, the set of
/// distinct values seen). One `NodeSchema` per node type.
#[derive(Clone, ResolvedObject)]
pub(crate) struct NodeSchema {
    pub(crate) type_id: usize,
    graph: DynamicGraph,
}

impl NodeSchema {
    pub fn new(node_type: usize, graph: DynamicGraph) -> Self {
        Self {
            type_id: node_type,
            graph,
        }
    }
}

#[ResolvedObjectFields]
impl NodeSchema {
    /// The node type this schema describes (e.g. `"person"`, `"org"`).
    /// Falls back to the default node type for untyped nodes.
    pub async fn type_name(&self) -> String {
        self.type_name_inner()
    }

    /// Property schemas seen on nodes of this type — one entry per property key
    /// ever set on a node of this type, with its observed `PropertyType` and (for
    /// string-valued properties) the set of distinct values.
    pub async fn properties(&self) -> Vec<PropertySchema> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.properties_inner()).await
    }

    /// Metadata schemas seen on nodes of this type — like `properties`, but
    /// covering metadata fields rather than temporal properties.
    pub async fn metadata(&self) -> Vec<PropertySchema> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.metadata_inner()).await
    }
}

impl NodeSchema {
    fn type_name_inner(&self) -> String {
        self.graph
            .node_meta()
            .get_node_type_name_by_id(self.type_id)
            .map(|type_name| type_name.to_string())
            .unwrap_or_else(|| DEFAULT_NODE_TYPE.to_string())
    }
    /// Filter only keeps the nodes carrying this schema's node type. Built once per resolution.
    fn nodes_of_type(&self) -> NodeFilteredGraph<DynamicGraph, NodeTypeFilterOp> {
        let mut node_types_mask =
            vec![false; self.graph.node_meta().node_type_meta().num_all_fields()];
        node_types_mask[self.type_id] = true;
        let filter = NodeTypeFilterOp::from_mask(node_types_mask.into(), self.graph.clone());
        NodeFilteredGraph::new(self.graph.clone(), filter)
    }

    fn properties_inner(&self) -> Vec<PropertySchema> {
        let mapper = self.graph.node_meta().temporal_prop_mapper();
        let visible = visible_ids_any_layer(self.graph.node_visible_temporal_prop_ids(), mapper);

        if self.graph.unfiltered_num_nodes(&LayerIds::All) > MAX_DETAILED_SCHEMA_ENTITIES {
            collect_no_variants(mapper, &visible)
        } else {
            // one pass over this type's nodes
            collect_variants(
                self.nodes_of_type().nodes().properties().into_iter_values(),
                mapper,
                MAX_VARIANTS,
                &visible,
            )
        }
    }

    fn metadata_inner(&self) -> Vec<PropertySchema> {
        let mapper = self.graph.node_meta().metadata_mapper();
        let visible = visible_ids_any_layer(self.graph.node_visible_metadata_ids(), mapper);

        if self.graph.unfiltered_num_nodes(&LayerIds::All) > MAX_DETAILED_SCHEMA_ENTITIES {
            collect_no_variants(mapper, &visible)
        } else {
            collect_variants(
                self.nodes_of_type().nodes().metadata().into_iter_values(),
                mapper,
                MAX_VARIANTS,
                &visible,
            )
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use raphtory::{db::api::view::IntoDynamic, prelude::*};

    use crate::model::schema::{
        graph_schema::GraphSchema, node_schema::PropertySchema, MAX_DETAILED_SCHEMA_ENTITIES,
    };
    use pretty_assertions::assert_eq;
    use raphtory::errors::GraphError;
    use raphtory_api::core::entities::{properties::prop::PropType, LayerIds};
    use raphtory_storage::core_ops::CoreGraphOps;

    #[test]
    fn aggregate_schema() -> Result<(), GraphError> {
        let g = Graph::new();

        g.add_node(
            0,
            1,
            [("t", Prop::str("wallet")), ("cost", Prop::F64(99.5))],
            Some("a"),
            None,
        )?;
        g.add_node(1, 2, [("t", Prop::str("person"))], None, None)?;
        g.add_node(
            6,
            3,
            [
                (
                    "list_prop",
                    Prop::list([Prop::F64(1.1), Prop::F64(2.2), Prop::F64(3.3)]).unwrap(),
                ),
                (
                    "map_prop",
                    Prop::map([("a", Prop::F64(1.0)), ("b", Prop::F64(2.0))]),
                ),
                ("cost_b", Prop::F64(76.0)),
            ],
            Some("b"),
            None,
        )?;
        g.add_node(
            7,
            4,
            [
                ("str_prop", Prop::str("hello")),
                ("bool_prop", Prop::Bool(true)),
            ],
            Some("b"),
            None,
        )?;

        let node = g.node(1).unwrap();
        node.add_metadata([("lol", Prop::str("smile"))])?;

        check_schema(&g);

        Ok(())
    }

    fn check_schema(g: &Graph) {
        let gs = GraphSchema::new(&g.clone().into_dynamic());

        let mut actual: Vec<(String, Vec<PropertySchema>, Vec<PropertySchema>)> = gs
            .nodes
            .iter()
            .map(|ns| {
                (
                    ns.type_name_inner(),
                    ns.properties_inner(),
                    ns.metadata_inner(),
                )
            })
            .collect_vec();

        actual.sort_by(|(k1, _, _), (k2, _, _)| k1.cmp(k2));

        actual.iter_mut().for_each(|(_, v1, v2)| {
            v1.sort();
            v2.sort();
        });

        let expected = vec![
            (
                "None".to_string(),
                vec![(("t", PropType::Str), ["person"]).into()],
                vec![],
            ),
            (
                "a".to_string(),
                vec![
                    (("cost", PropType::F64), ["99.5"]).into(),
                    (("t", PropType::Str), ["wallet"]).into(),
                ],
                vec![(("lol", PropType::Str), ["smile"]).into()],
            ),
            (
                "b".to_string(),
                vec![
                    (("bool_prop", PropType::Bool), ["true"]).into(),
                    (("cost_b", PropType::F64), ["76"]).into(),
                    (
                        ("list_prop", PropType::List(Box::new(PropType::F64))),
                        ["[1.1, 2.2, 3.3]"],
                    )
                        .into(),
                    (
                        (
                            "map_prop",
                            PropType::map([("a", PropType::F64), ("b", PropType::F64)]),
                        ),
                        ["{\"a\": 1, \"b\": 2}"],
                    )
                        .into(),
                    (("str_prop", PropType::Str), ["hello"]).into(),
                ],
                vec![],
            ),
        ];
        assert_eq!(actual, expected);
    }

    /// Materializing a filtered view keeps the source's prop mappers, so ids and
    /// dtypes for keys with no values in the result stay registered. The schema
    /// must not report those - the presence bitset is the only thing
    /// separating "registered" from "actually has values here".
    #[test]
    fn filtered_out_props_are_not_reported_on_large_graphs() -> Result<(), GraphError> {
        let g = Graph::new();
        // over the detailed-schema threshold, so the keys/types-only branch runs
        let num_nodes = MAX_DETAILED_SCHEMA_ENTITIES + 1;
        for id in 0..num_nodes {
            g.add_node(0, id as u64, [("kept", Prop::I64(1))], Some("t"), None)?;
        }
        // only ever set outside the window below
        let late = g.add_node(100, u64::MAX, [("dropped", Prop::I64(2))], Some("t"), None)?;
        late.add_metadata([("dropped_meta", Prop::I64(3))])?;

        let mat = g.window(0, 50).materialize()?;
        assert!(mat.unfiltered_num_nodes(&LayerIds::All) > MAX_DETAILED_SCHEMA_ENTITIES);

        // the mappers still know the filtered-out keys...
        let node_meta = mat.node_meta();
        assert!(node_meta.temporal_prop_mapper().get_id("dropped").is_some());
        assert!(node_meta.metadata_mapper().get_id("dropped_meta").is_some());

        // ...but only the key with values inside the window is reported
        let gs = GraphSchema::new(&mat.into_dynamic());
        let t = gs
            .nodes
            .iter()
            .find(|n| n.type_name_inner() == "t")
            .expect("node type t");
        assert_eq!(
            t.properties_inner(),
            vec![PropertySchema::new(
                "kept".to_string(),
                PropType::I64,
                vec![]
            )]
        );
        assert_eq!(t.metadata_inner(), vec![]);
        Ok(())
    }
}
