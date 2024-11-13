use crate::model::graph::{node::Node, property::GqlPropValue, FilterCondition, Operator};
use dynamic_graphql::{Enum, InputObject, ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::errors::GraphError,
    db::{api::view::DynamicGraph, graph::nodes::Nodes},
    prelude::{GraphViewOps, *},
};

#[derive(ResolvedObject)]
pub(crate) struct GqlNodes {
    pub(crate) nn: Nodes<'static, DynamicGraph>,
}

impl GqlNodes {
    fn update<N: Into<Nodes<'static, DynamicGraph>>>(&self, nodes: N) -> Self {
        GqlNodes::new(nodes)
    }
}

impl GqlNodes {
    pub(crate) fn new<N: Into<Nodes<'static, DynamicGraph>>>(nodes: N) -> Self {
        Self { nn: nodes.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Node> + '_> {
        let iter = self.nn.iter_owned().map(Node::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlNodes {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.valid_layers(names))
    }

    async fn exclude_layers(&self, names: Vec<String>) -> Self {
        self.update(self.nn.exclude_valid_layers(names))
    }

    async fn layer(&self, name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    async fn exclude_layer(&self, name: String) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    async fn window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.window(start, end))
    }

    async fn at(&self, time: i64) -> Self {
        self.update(self.nn.at(time))
    }

    async fn latest(&self) -> Self {
        self.update(self.nn.latest())
    }

    async fn snapshot_at(&self, time: i64) -> Self {
        self.update(self.nn.snapshot_at(time))
    }

    async fn snapshot_latest(&self) -> Self {
        self.update(self.nn.snapshot_latest())
    }

    async fn before(&self, time: i64) -> Self {
        self.update(self.nn.before(time))
    }

    async fn after(&self, time: i64) -> Self {
        self.update(self.nn.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.update(self.nn.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.update(self.nn.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.update(self.nn.shrink_end(end))
    }

    async fn type_filter(&self, node_types: Vec<String>) -> Self {
        self.update(self.nn.type_filter(&node_types))
    }

    async fn node_filter(
        &self,
        property: String,
        condition: FilterCondition,
    ) -> Result<Self, GraphError> {
        match condition.operator {
            Operator::Equal => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::eq(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "Equal".into(),
                    ))
                }
            }
            Operator::NotEqual => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::ne(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "NotEqual".into(),
                    ))
                }
            }
            Operator::GreaterThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::ge(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThanOrEqual".into(),
                    ))
                }
            }
            Operator::LessThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::le(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThanOrEqual".into(),
                    ))
                }
            }
            Operator::GreaterThan => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::gt(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThan".into(),
                    ))
                }
            }
            Operator::LessThan => {
                if let Some(value) = condition.value {
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::lt(property, value.0))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThan".into(),
                    ))
                }
            }
            Operator::IsNone => {
                let filtered_nodes = self.nn.filter_nodes(PropertyFilter::is_none(property))?;
                Ok(self.update(filtered_nodes))
            }
            Operator::IsSome => {
                let filtered_nodes = self.nn.filter_nodes(PropertyFilter::is_some(property))?;
                Ok(self.update(filtered_nodes))
            }
            Operator::Any => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::any(property, prop_values))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "Any".into(),
                    ))
                }
            }
            Operator::NotAny => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_nodes = self
                        .nn
                        .filter_nodes(PropertyFilter::not_any(property, prop_values))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "NotAny".into(),
                    ))
                }
            }
        }
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn start(&self) -> Option<i64> {
        self.nn.start()
    }

    async fn end(&self) -> Option<i64> {
        self.nn.end()
    }

    /////////////////
    //// List ///////
    /////////////////

    async fn count(&self) -> usize {
        self.iter().count()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Node> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    async fn list(&self) -> Vec<Node> {
        self.iter().collect()
    }

    async fn ids(&self) -> Vec<String> {
        self.nn.name().collect()
    }
}
