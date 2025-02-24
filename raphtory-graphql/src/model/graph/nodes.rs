use crate::model::{
    graph::{node::Node, FilterCondition, Operator},
    sorting::{NodeSortBy, SortByTime},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::{state::Index, view::DynamicGraph},
        graph::nodes::Nodes,
    },
    prelude::*,
};
use raphtory_api::core::entities::VID;
use std::cmp::Ordering;

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
                        .filter_nodes(PropertyFilter::includes(property, prop_values))?;
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
                        .filter_nodes(PropertyFilter::excludes(property, prop_values))?;
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

    /////////////////
    //// Sorting ////
    /////////////////

    async fn sorted(&self, sort_bys: Vec<NodeSortBy>) -> Self {
        let sorted: Index<VID> = self
            .nn
            .iter()
            .sorted_by(|first_node, second_node| {
                sort_bys
                    .iter()
                    .fold(Ordering::Equal, |current_ordering, sort_by| {
                        current_ordering.then_with(|| {
                            let ordering = if sort_by.id == Some(true) {
                                first_node.id().partial_cmp(&second_node.id())
                            } else if let Some(sort_by_time) = sort_by.time.as_ref() {
                                let (first_time, second_time) = match sort_by_time {
                                    SortByTime::Latest => {
                                        (first_node.latest_time(), second_node.latest_time())
                                    }
                                    SortByTime::Earliest => {
                                        (first_node.earliest_time(), second_node.earliest_time())
                                    }
                                };
                                first_time.partial_cmp(&second_time)
                            } else if let Some(sort_by_property) = sort_by.property.as_ref() {
                                let first_prop_maybe =
                                    first_node.properties().get(sort_by_property);
                                let second_prop_maybe =
                                    second_node.properties().get(sort_by_property);
                                first_prop_maybe.partial_cmp(&second_prop_maybe)
                            } else {
                                None
                            };
                            if let Some(ordering) = ordering {
                                if sort_by.reverse == Some(true) {
                                    ordering.reverse()
                                } else {
                                    ordering
                                }
                            } else {
                                Ordering::Equal
                            }
                        })
                    })
            })
            .map(|node_view| node_view.node)
            .collect();
        GqlNodes::new(self.nn.indexed(sorted))
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
