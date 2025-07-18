use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                node_filter::CompositeNodeFilter,
                property_filter::{PropertyFilter, PropertyFilterBuilder},
                AndFilter, AsEdgeFilter, Filter, NotFilter, OrFilter, PropertyFilterFactory,
                TryAsEdgeFilter, TryAsNodeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use std::{fmt, fmt::Display, ops::Deref, sync::Arc};

#[derive(Debug, Clone)]
pub struct EdgeFieldFilter(pub Filter);

impl Display for EdgeFieldFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeEdgeFilter {
    Edge(Filter),
    Property(PropertyFilter<EdgeFilter>),
    And(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
    Or(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
    Not(Box<CompositeEdgeFilter>),
}

impl Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Edge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeEdgeFilter::Edge(i) => Ok(Arc::new(EdgeFieldFilter(i).create_filter(graph)?)),
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeEdgeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeEdgeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeEdgeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }
}

impl TryAsNodeFilter for CompositeEdgeFilter {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl AsEdgeFilter for CompositeEdgeFilter {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        self.clone()
    }
}

impl TryAsEdgeFilter for CompositeEdgeFilter {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(self.clone())
    }
}

pub trait InternalEdgeFilterBuilderOps: Send + Sync {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalEdgeFilterBuilderOps> InternalEdgeFilterBuilderOps for Arc<T> {
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait EdgeFilterOps {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn contains(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn not_contains(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter;
}

impl<T: ?Sized + InternalEdgeFilterBuilderOps> EdgeFilterOps for T {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::eq(self.field_name(), value))
    }

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ne(self.field_name(), value))
    }

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::is_in(self.field_name(), values))
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::is_not_in(self.field_name(), values))
    }

    fn contains(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::contains(self.field_name(), value.into()))
    }

    fn not_contains(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::not_contains(self.field_name(), value.into()))
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::fuzzy_search(
            self.field_name(),
            value,
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct EdgeSourceFilterBuilder;

impl InternalEdgeFilterBuilderOps for EdgeSourceFilterBuilder {
    fn field_name(&self) -> &'static str {
        "src"
    }
}

pub struct EdgeDestinationFilterBuilder;

impl InternalEdgeFilterBuilderOps for EdgeDestinationFilterBuilder {
    fn field_name(&self) -> &'static str {
        "dst"
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct EdgeFilter;

#[derive(Clone)]
pub enum EdgeEndpointFilter {
    Src,
    Dst,
}

impl EdgeEndpointFilter {
    pub fn name(&self) -> Arc<dyn InternalEdgeFilterBuilderOps> {
        match self {
            EdgeEndpointFilter::Src => Arc::new(EdgeSourceFilterBuilder),
            EdgeEndpointFilter::Dst => Arc::new(EdgeDestinationFilterBuilder),
        }
    }
}

impl EdgeFilter {
    pub fn src() -> EdgeEndpointFilter {
        EdgeEndpointFilter::Src
    }
    pub fn dst() -> EdgeEndpointFilter {
        EdgeEndpointFilter::Dst
    }
}

impl PropertyFilterFactory<EdgeFilter> for EdgeFilter {
    fn property(name: impl Into<String>) -> PropertyFilterBuilder<EdgeFilter> {
        PropertyFilterBuilder::new(name)
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl PropertyFilterFactory<ExplodedEdgeFilter> for ExplodedEdgeFilter {
    fn property(name: impl Into<String>) -> PropertyFilterBuilder<ExplodedEdgeFilter> {
        PropertyFilterBuilder::new(name)
    }
}

impl AsEdgeFilter for EdgeFieldFilter {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Edge(self.0.clone())
    }
}

impl TryAsNodeFilter for EdgeFieldFilter {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl TryAsEdgeFilter for EdgeFieldFilter {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Edge(self.0.clone()))
    }
}
