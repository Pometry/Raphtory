use crate::model::schema::{
    merge_schemas, property_schema::PropertySchema, SchemaAggregate, DEFAULT_NODE_TYPE,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{internal::CoreGraphOps, DynamicGraph},
        graph::node::NodeView,
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use rustc_hash::FxHashMap;
use std::collections::HashSet;

#[derive(ResolvedObject)]
pub(crate) struct NodeSchema {
    pub(crate) type_name: String,
    graph: DynamicGraph,
}

impl NodeSchema {
    pub fn new(node_type: String, graph: DynamicGraph) -> Self {
        Self {
            type_name: node_type,
            graph,
        }
    }
}

#[ResolvedObjectFields]
impl NodeSchema {
    async fn type_name(&self) -> String {
        self.type_name.clone()
    }

    /// Returns the list of property schemas for this node
    async fn properties(&self) -> Vec<PropertySchema> {
        self.properties_inner()
    }
}

impl NodeSchema {
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

        keys.into_iter()
            .zip(property_types)
            .map(|(key, dtype)| PropertySchema::new(key, dtype, vec![]))
            .collect()
    }
}

fn collect_node_schema(node: NodeView<DynamicGraph>) -> SchemaAggregate {
    let mut stable_hash = FxHashMap::default();
    let properties = node.properties();
    let iter = properties.iter().filter_map(|(key, value)| {
        let value = value?;
        let temporal_prop = node
            .base_graph
            .node_meta()
            .get_prop_id(&key.to_string(), false);
        let constant_prop = node
            .base_graph
            .node_meta()
            .get_prop_id(&key.to_string(), true);

        let key_with_prop_type = if temporal_prop.is_some() {
            let p_type = node
                .base_graph
                .node_meta()
                .temporal_prop_meta()
                .get_dtype(temporal_prop.unwrap());
            (key.to_string(), p_type.unwrap().to_string())
        } else if constant_prop.is_some() {
            let p_type = node
                .base_graph
                .node_meta()
                .const_prop_meta()
                .get_dtype(constant_prop.unwrap());
            (key.to_string(), p_type.unwrap().to_string())
        } else {
            (key.to_string(), "NONE".to_string())
        };
        Some((key_with_prop_type, HashSet::from([value.to_string()])))
    });

    for (key, value) in iter {
        stable_hash.insert(key, value);
    }

    stable_hash
}

#[cfg(test)]
mod test {

    use itertools::Itertools;
    use raphtory::{core::utils::errors::GraphError, db::api::view::IntoDynamic, prelude::*};

    use crate::model::schema::{graph_schema::GraphSchema, node_schema::PropertySchema};
    use pretty_assertions::assert_eq;

    #[test]
    fn aggregate_schema() -> Result<(), GraphError> {
        let g = Graph::new_with_shards(2);

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

        let actual: Vec<(String, Vec<PropertySchema>)> = gs
            .nodes
            .iter()
            .map(|ns| ((&ns.type_name).to_string(), ns.properties_inner()))
            .collect_vec();

        let expected = vec![
            (
                "a".to_string(),
                vec![
                    (("t", "Str"), ["wallet"]).into(),
                    (("lol", "Str"), ["smile"]).into(),
                    (("cost", "F64"), ["99.5"]).into(),
                ],
            ),
            ("None".to_string(), vec![(("t", "Str"), ["person"]).into()]),
            (
                "b".to_string(),
                vec![
                    (("list_prop", "List<F64>"), ["[1.1, 2.2, 3.3]"]).into(),
                    (("cost_b", "F64"), ["76"]).into(),
                    (("str_prop", "Str"), ["hello"]).into(),
                    (("bool_prop", "Bool"), ["true"]).into(),
                    (
                        ("map_prop", "Map{ a: F64, b: F64 }"),
                        ["{\"a\": 1, \"b\": 2}"],
                    )
                        .into(),
                ],
            ),
        ];
        assert_eq!(actual, expected);
    }
}
