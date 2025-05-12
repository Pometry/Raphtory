use crate::model::{
    graph::{
        filtering::{FilterCondition, NodesViewCollection, Operator},
        node::GqlNode,
        windowset::GqlNodesWindowSet,
    },
    sorting::{NodeSortBy, SortByTime},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::errors::{
        GraphError,
        GraphError::{MismatchedIntervalTypes, NoIntervalProvided, WrongNumOfArgs},
    },
    db::{
        api::{state::Index, view::DynamicGraph},
        graph::{nodes::Nodes, views::property_filter::PropertyRef},
    },
    prelude::*,
};
use raphtory_api::core::entities::VID;
use std::cmp::Ordering;

#[derive(ResolvedObject)]
#[graphql(name = "Nodes")]
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

    fn iter(&self) -> Box<dyn Iterator<Item = GqlNode> + '_> {
        let iter = self.nn.iter_owned().map(GqlNode::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlNodes {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////
    async fn default_layer(&self) -> Self {
        self.update(self.nn.default_layer())
    }

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

    fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlNodesWindowSet, GraphError> {
        match (window_str, window_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "window_str".to_string(),
                "window_int".to_string(),
            )),
            (None, Some(window_int)) => {
                if step_str.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlNodesWindowSet::new(
                    self.nn.rolling(window_int, step_int)?,
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlNodesWindowSet::new(
                    self.nn.rolling(window_str, step_str)?,
                ))
            }
            (None, None) => return Err(NoIntervalProvided),
        }
    }

    fn expanding(
        &self,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlNodesWindowSet, GraphError> {
        match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => Ok(GqlNodesWindowSet::new(self.nn.expanding(step_int)?)),
            (Some(step_str), None) => Ok(GqlNodesWindowSet::new(self.nn.expanding(step_str)?)),
            (None, None) => return Err(NoIntervalProvided),
        }
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
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::eq(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "Equal".into(),
                    ))
                }
            }
            Operator::NotEqual => {
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::ne(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "NotEqual".into(),
                    ))
                }
            }
            Operator::GreaterThanOrEqual => {
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::ge(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThanOrEqual".into(),
                    ))
                }
            }
            Operator::LessThanOrEqual => {
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::le(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThanOrEqual".into(),
                    ))
                }
            }
            Operator::GreaterThan => {
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::gt(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThan".into(),
                    ))
                }
            }
            Operator::LessThan => {
                if let Some(v) = condition.value {
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::lt(
                        PropertyRef::Property(property),
                        Prop::try_from(v)?,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThan".into(),
                    ))
                }
            }
            Operator::IsNone => {
                let filtered_nodes = self
                    .nn
                    .filter_nodes(PropertyFilter::is_none(PropertyRef::Property(property)))?;
                Ok(self.update(filtered_nodes))
            }
            Operator::IsSome => {
                let filtered_nodes = self
                    .nn
                    .filter_nodes(PropertyFilter::is_some(PropertyRef::Property(property)))?;
                Ok(self.update(filtered_nodes))
            }
            Operator::Any => {
                if let Some(Prop::List(list)) = condition.value.and_then(|v| Prop::try_from(v).ok())
                {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::includes(
                        PropertyRef::Property(property),
                        prop_values,
                    ))?;
                    Ok(self.update(filtered_nodes))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "Any".into(),
                    ))
                }
            }
            Operator::NotAny => {
                if let Some(Prop::List(list)) = condition.value.and_then(|v| Prop::try_from(v).ok())
                {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_nodes = self.nn.filter_nodes(PropertyFilter::excludes(
                        PropertyRef::Property(property),
                        prop_values,
                    ))?;
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

    async fn apply_views(&self, views: Vec<NodesViewCollection>) -> Result<GqlNodes, GraphError> {
        let mut return_view: GqlNodes = GqlNodes::new(self.nn.clone());

        for view in views {
            let mut count = 0;
            if let Some(_) = view.default_layer {
                count += 1;
                return_view = return_view.default_layer().await;
            }
            if let Some(layers) = view.layers {
                count += 1;
                return_view = return_view.layers(layers).await;
            }
            if let Some(layers) = view.exclude_layers {
                count += 1;
                return_view = return_view.exclude_layers(layers).await;
            }
            if let Some(layer) = view.layer {
                count += 1;
                return_view = return_view.layer(layer).await;
            }
            if let Some(layer) = view.exclude_layer {
                count += 1;
                return_view = return_view.exclude_layer(layer).await;
            }
            if let Some(window) = view.window {
                count += 1;
                return_view = return_view.window(window.start, window.end).await;
            }
            if let Some(time) = view.at {
                count += 1;
                return_view = return_view.at(time).await;
            }
            if let Some(_) = view.latest {
                count += 1;
                return_view = return_view.latest().await;
            }
            if let Some(time) = view.snapshot_at {
                count += 1;
                return_view = return_view.snapshot_at(time).await;
            }
            if let Some(_) = view.snapshot_latest {
                count += 1;
                return_view = return_view.snapshot_latest().await;
            }
            if let Some(time) = view.before {
                count += 1;
                return_view = return_view.before(time).await;
            }
            if let Some(time) = view.after {
                count += 1;
                return_view = return_view.after(time).await;
            }
            if let Some(window) = view.shrink_window {
                count += 1;
                return_view = return_view.shrink_window(window.start, window.end).await;
            }
            if let Some(time) = view.shrink_start {
                count += 1;
                return_view = return_view.shrink_start(time).await;
            }
            if let Some(time) = view.shrink_end {
                count += 1;
                return_view = return_view.shrink_end(time).await;
            }
            if let Some(types) = view.type_filter {
                count += 1;
                return_view = return_view.type_filter(types).await;
            }
            if let Some(node_filter) = view.node_filter {
                count += 1;
                return_view = return_view
                    .node_filter(node_filter.property, node_filter.condition)
                    .await?;
            }

            if count > 1 {
                return Err(GraphError::TooManyViewsSet);
            }
        }

        Ok(return_view)
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

    async fn page(&self, limit: usize, offset: usize) -> Vec<GqlNode> {
        let start = offset * limit;
        self.iter().skip(start).take(limit).collect()
    }

    async fn list(&self) -> Vec<GqlNode> {
        self.iter().collect()
    }

    async fn ids(&self) -> Vec<String> {
        self.nn.name().collect()
    }
}
