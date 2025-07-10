use crate::model::schema::{property_schema::PropertySchema, DEFAULT_NODE_TYPE};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::view::DynamicGraph,
        graph::views::filter::node_type_filtered_graph::NodeTypeFilteredGraph,
    },
    prelude::{GraphViewOps, NodeStateOps, NodeViewOps},
};
use raphtory_storage::core_ops::CoreGraphOps;
use rayon::prelude::*;

#[derive(ResolvedObject)]
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
    async fn type_name(&self) -> String {
        self.type_name_inner()
    }

    /// Returns the list of property schemas for this node
    async fn properties(&self) -> Vec<PropertySchema> {
        self.properties_inner()
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
    fn properties_inner(&self) -> Vec<PropertySchema> {
        let mut keys: Vec<String> = self
            .graph
            .node_meta()
            .temporal_prop_meta()
            .get_keys()
            .into_iter()
            .map(|k| k.to_string())
            .collect();
        let mut property_types: Vec<String> = self
            .graph
            .node_meta()
            .temporal_prop_meta()
            .dtypes()
            .iter()
            .map(|dtype| dtype.to_string())
            .collect();
        for const_key in self.graph.node_meta().const_prop_meta().get_keys() {
            if self
                .graph
                .node_meta()
                .get_prop_id(&const_key, false)
                .is_none()
            {
                keys.push(const_key.to_string());
                let id = self
                    .graph
                    .node_meta()
                    .get_prop_id(&const_key, true)
                    .unwrap();
                property_types.push(
                    self.graph
                        .node_meta()
                        .const_prop_meta()
                        .get_dtype(id)
                        .unwrap()
                        .to_string(),
                );
            }
        }

        if self.graph.unfiltered_num_nodes() > 1000 {
            // large graph, do not collect detailed schema as it is expensive
            keys.into_iter()
                .zip(property_types)
                .map(|(key, dtype)| PropertySchema::new(key, dtype, vec![]))
                .collect()
        } else {
            keys.into_par_iter()
                .zip(property_types)
                .filter_map(|(key, dtype)| {
                    let mut node_types_filter =
                        vec![false; self.graph.node_meta().node_type_meta().len()];
                    node_types_filter[self.type_id] = true;
                    let unique_values: ahash::HashSet<_> =
                        NodeTypeFilteredGraph::new(self.graph.clone(), node_types_filter.into())
                            .nodes()
                            .properties()
                            .into_iter_values()
                            .filter_map(|props| props.get(&key).map(|v| v.to_string()))
                            .collect();
                    if unique_values.is_empty() {
                        None
                    } else {
                        let mut variants = if unique_values.len() <= 100 {
                            Vec::from_iter(unique_values)
                        } else {
                            vec![]
                        };
                        variants.sort();
                        Some(PropertySchema::new(key, dtype, variants))
                    }
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use raphtory::{db::api::view::IntoDynamic, prelude::*};

    use crate::model::schema::{graph_schema::GraphSchema, node_schema::PropertySchema};
    use pretty_assertions::assert_eq;
    use raphtory::errors::GraphError;

    #[test]
    fn aggregate_schema() -> Result<(), GraphError> {
        let g = Graph::new();

        g.add_node(
            0,
            1,
            [("t", Prop::str("wallet")), ("cost", Prop::F64(99.5))],
            Some("a"),
        )?;
        g.add_node(1, 2, [("t", Prop::str("person"))], None)?;
        g.add_node(
            6,
            3,
            [
                (
                    "list_prop",
                    Prop::List(vec![Prop::F64(1.1), Prop::F64(2.2), Prop::F64(3.3)].into()),
                ),
                (
                    "map_prop",
                    Prop::map([("a", Prop::F64(1.0)), ("b", Prop::F64(2.0))]),
                ),
                ("cost_b", Prop::F64(76.0)),
            ],
            Some("b"),
        )?;
        g.add_node(
            7,
            4,
            [
                ("str_prop", Prop::str("hello")),
                ("bool_prop", Prop::Bool(true)),
            ],
            Some("b"),
        )?;

        let node = g.node(1).unwrap();
        node.add_constant_properties([("lol", Prop::str("smile"))])?;

        check_schema(&g);

        Ok(())
    }

    fn check_schema(g: &Graph) {
        let gs = GraphSchema::new(&g.clone().into_dynamic());

        let mut actual: Vec<(String, Vec<PropertySchema>)> = gs
            .nodes
            .iter()
            .map(|ns| (ns.type_name_inner(), ns.properties_inner()))
            .collect_vec();

        actual.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        actual.iter_mut().for_each(|(_, v)| v.sort());

        let expected = vec![
            ("None".to_string(), vec![(("t", "Str"), ["person"]).into()]),
            (
                "a".to_string(),
                vec![
                    (("cost", "F64"), ["99.5"]).into(),
                    (("lol", "Str"), ["smile"]).into(),
                    (("t", "Str"), ["wallet"]).into(),
                ],
            ),
            (
                "b".to_string(),
                vec![
                    (("bool_prop", "Bool"), ["true"]).into(),
                    (("cost_b", "F64"), ["76"]).into(),
                    (("list_prop", "List<F64>"), ["[1.1, 2.2, 3.3]"]).into(),
                    (
                        ("map_prop", "Map{ a: F64, b: F64 }"),
                        ["{\"a\": 1, \"b\": 2}"],
                    )
                        .into(),
                    (("str_prop", "Str"), ["hello"]).into(),
                ],
            ),
        ];
        assert_eq!(actual, expected);
    }
}
