use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                node_filter::CompositeNodeFilter, property_filter::PropertyFilter, AndFilter,
                Filter, NotFilter, OrFilter, PropertyFilterFactory, TryAsCompositeFilter, Windowed,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::GID;
use raphtory_core::utils::time::IntoTime;
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
            CompositeEdgeFilter::Edge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
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

impl TryAsCompositeFilter for CompositeEdgeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(self.clone())
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeExplodedEdgeFilter {
    Property(PropertyFilter<ExplodedEdgeFilter>),
    And(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Or(
        Box<CompositeExplodedEdgeFilter>,
        Box<CompositeExplodedEdgeFilter>,
    ),
    Not(Box<CompositeExplodedEdgeFilter>),
}

impl Display for CompositeExplodedEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeExplodedEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeExplodedEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeExplodedEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeExplodedEdgeFilter::Not(filter) => write!(f, "(NOT {})", filter),
        }
    }
}

impl CreateFilter for CompositeExplodedEdgeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeExplodedEdgeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeExplodedEdgeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeExplodedEdgeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeExplodedEdgeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }
}

impl TryAsCompositeFilter for CompositeExplodedEdgeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
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

    fn starts_with(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn ends_with(&self, value: impl Into<String>) -> EdgeFieldFilter;

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

    fn starts_with(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::starts_with(self.field_name(), value.into()))
    }

    fn ends_with(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ends_with(self.field_name(), value.into()))
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

#[derive(Debug, Clone)]
pub struct EdgeIdFilterBuilder {
    field: &'static str,
}

impl EdgeIdFilterBuilder {
    #[inline]
    fn field_name(&self) -> &'static str {
        self.field
    }

    pub fn eq<V: Into<GID>>(&self, v: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::eq_id(self.field_name(), v))
    }

    pub fn ne<V: Into<GID>>(&self, v: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ne_id(self.field_name(), v))
    }

    pub fn is_in<I, V>(&self, vals: I) -> EdgeFieldFilter
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        EdgeFieldFilter(Filter::is_in_id(self.field_name(), vals))
    }

    pub fn is_not_in<I, V>(&self, vals: I) -> EdgeFieldFilter
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        EdgeFieldFilter(Filter::is_not_in_id(self.field_name(), vals))
    }

    pub fn lt<V: Into<GID>>(&self, value: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::lt(self.field_name(), value))
    }

    pub fn le<V: Into<GID>>(&self, value: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::le(self.field_name(), value).into())
    }

    pub fn gt<V: Into<GID>>(&self, value: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::gt(self.field_name(), value))
    }

    pub fn ge<V: Into<GID>>(&self, value: V) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ge(self.field_name(), value))
    }

    pub fn starts_with<S: Into<String>>(&self, s: S) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::starts_with(self.field_name(), s.into()))
    }

    pub fn ends_with<S: Into<String>>(&self, s: S) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ends_with(self.field_name(), s.into()))
    }

    pub fn contains<S: Into<String>>(&self, s: S) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::contains(self.field_name(), s.into()))
    }

    pub fn not_contains<S: Into<String>>(&self, s: S) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::not_contains(self.field_name(), s.into()))
    }

    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter {
        EdgeFieldFilter(
            Filter::fuzzy_search(self.field_name(), s, levenshtein_distance, prefix_match).into(),
        )
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
    pub fn id(&self) -> EdgeIdFilterBuilder {
        let field = match self {
            EdgeEndpointFilter::Src => "src",
            EdgeEndpointFilter::Dst => "dst",
        };
        EdgeIdFilterBuilder { field }
    }

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

    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<EdgeFilter> {
        Windowed::from_times(start, end)
    }
}

impl IsPropertyFilterFactory<EdgeFilter> for EdgeFilter {}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl PropertyFilterFactory<ExplodedEdgeFilter> for ExplodedEdgeFilter {}

impl ExplodedEdgeFilter {
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<ExplodedEdgeFilter> {
        Windowed::from_times(start, end)
    }
}

impl TryAsCompositeFilter for EdgeFieldFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Edge(self.0.clone()))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
