use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            exploded_edge_property_filter::ExplodedEdgePropertyFilteredGraph,
            internal::CreateFilter,
            model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
                property_filter::PropertyFilter, AndFilter, NotFilter, OrFilter,
                PropertyFilterFactory, TryAsCompositeFilter, Windowed,
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, LayerOps},
};
use raphtory_core::utils::time::IntoTime;
use std::{fmt, fmt::Display, ops::Deref, sync::Arc};

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

impl CreateFilter for PropertyFilter<ExplodedEdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = ExplodedEdgePropertyFilteredGraph<G>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let prop_id = self.resolve_prop_id(graph.edge_meta(), graph.num_layers() > 1)?;
        Ok(ExplodedEdgePropertyFilteredGraph::new(
            graph.clone(),
            prop_id,
            self,
        ))
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

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl PropertyFilterFactory<ExplodedEdgeFilter> for ExplodedEdgeFilter {}

impl ExplodedEdgeFilter {
    #[inline]
    pub fn window<S: IntoTime, E: IntoTime>(start: S, end: E) -> Windowed<ExplodedEdgeFilter> {
        Windowed::from_times(start, end)
    }
}
