use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                edge_filter::CompositeEdgeFilter,
                property_filter::{PropertyFilter, PropertyFilterBuilder},
                AndFilter, AsNodeFilter, Filter, NotFilter, OrFilter, PropertyFilterFactory,
                TryAsEdgeFilter, TryAsNodeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{fmt, fmt::Display, ops::Deref, sync::Arc};

#[derive(Debug, Clone)]
pub struct NodeNameFilter(pub Filter);

impl Display for NodeNameFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeNameFilter {
    fn from(filter: Filter) -> Self {
        NodeNameFilter(filter)
    }
}

#[derive(Debug, Clone)]
pub struct NodeTypeFilter(pub Filter);

impl Display for NodeTypeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeTypeFilter {
    fn from(filter: Filter) -> Self {
        NodeTypeFilter(filter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter<NodeFilter>),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Not(Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
        }
    }
}

impl CreateFilter for CompositeNodeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeNodeFilter::Node(i) => match i.field_name.as_str() {
                "node_name" => Ok(Arc::new(NodeNameFilter(i).create_filter(graph)?)),
                "node_type" => Ok(Arc::new(NodeTypeFilter(i).create_filter(graph)?)),
                _ => {
                    unreachable!()
                }
            },
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeNodeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeNodeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeNodeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }
}

impl AsNodeFilter for CompositeNodeFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        self.clone()
    }
}

impl TryAsNodeFilter for CompositeNodeFilter {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.clone())
    }
}

impl TryAsEdgeFilter for CompositeNodeFilter {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

pub trait InternalNodeFilterBuilderOps: Send + Sync {
    type NodeFilterType: From<Filter>
        + CreateFilter
        + AsNodeFilter
        + TryAsNodeFilter
        + TryAsEdgeFilter
        + Clone
        + 'static;

    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeFilterBuilderOps> InternalNodeFilterBuilderOps for Arc<T> {
    type NodeFilterType = T::NodeFilterType;

    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait NodeFilterBuilderOps: InternalNodeFilterBuilderOps {
    fn eq(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::eq(self.field_name(), value).into()
    }

    fn ne(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::ne(self.field_name(), value).into()
    }

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::is_in(self.field_name(), values).into()
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::is_not_in(self.field_name(), values).into()
    }

    fn contains(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::contains(self.field_name(), value).into()
    }

    fn not_contains(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::not_contains(self.field_name(), value.into()).into()
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::NodeFilterType {
        Filter::fuzzy_search(self.field_name(), value, levenshtein_distance, prefix_match).into()
    }
}

impl<T: InternalNodeFilterBuilderOps + ?Sized> NodeFilterBuilderOps for T {}

pub struct NodeNameFilterBuilder;

impl InternalNodeFilterBuilderOps for NodeNameFilterBuilder {
    type NodeFilterType = NodeNameFilter;

    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

pub struct NodeTypeFilterBuilder;

impl InternalNodeFilterBuilderOps for NodeTypeFilterBuilder {
    type NodeFilterType = NodeTypeFilter;

    fn field_name(&self) -> &'static str {
        "node_type"
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl NodeFilter {
    pub fn name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }
}

impl PropertyFilterFactory<NodeFilter> for NodeFilter {
    fn property(name: impl Into<String>) -> PropertyFilterBuilder<NodeFilter> {
        PropertyFilterBuilder::new(name)
    }
}

impl AsNodeFilter for NodeNameFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Node(self.0.clone())
    }
}

impl TryAsNodeFilter for NodeNameFilter {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.as_node_filter())
    }
}

impl TryAsEdgeFilter for NodeNameFilter {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl AsNodeFilter for NodeTypeFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Node(self.0.clone())
    }
}

impl TryAsNodeFilter for NodeTypeFilter {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.as_node_filter())
    }
}

impl TryAsEdgeFilter for NodeTypeFilter {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
