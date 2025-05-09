use crate::{
    core::utils::errors::GraphError,
    db::{
        api::{storage::graph::edges::edge_ref::EdgeStorageRef, view::BoxableGraphView},
        graph::views::filter::{
            internal::InternalEdgeFilterOps,
            model::{property_filter::PropertyFilter, AndFilter, Filter, NotFilter, OrFilter},
        },
    },
    prelude::GraphViewOps,
};
use std::{collections::HashMap, fmt, fmt::Display, ops::Deref, sync::Arc};

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
    Property(PropertyFilter),
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

impl CompositeEdgeFilter {
    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_ids: &HashMap<String, usize>,
        c_prop_ids: &HashMap<String, usize>,
        edge: EdgeStorageRef,
    ) -> bool {
        match self {
            CompositeEdgeFilter::Edge(edge_filter) => edge_filter.matches_edge::<G>(graph, edge),
            CompositeEdgeFilter::Property(property_filter) => {
                let prop_name = property_filter.prop_ref.name();
                let t_prop_id = t_prop_ids.get(prop_name).copied();
                let c_prop_id = c_prop_ids.get(prop_name).copied();
                property_filter.matches_edge(graph, t_prop_id, c_prop_id, edge)
            }
            CompositeEdgeFilter::And(left, right) => {
                left.matches_edge(graph, t_prop_ids, c_prop_ids, edge)
                    && right.matches_edge(graph, t_prop_ids, c_prop_ids, edge)
            }
            CompositeEdgeFilter::Or(left, right) => {
                left.matches_edge(graph, t_prop_ids, c_prop_ids, edge)
                    || right.matches_edge(graph, t_prop_ids, c_prop_ids, edge)
            }
            CompositeEdgeFilter::Not(filter) => {
                !filter.matches_edge(graph, t_prop_ids, c_prop_ids, edge)
            }
        }
    }
}

impl InternalEdgeFilterOps for CompositeEdgeFilter {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        match self {
            CompositeEdgeFilter::Edge(i) => {
                Ok(Arc::new(EdgeFieldFilter(i).create_edge_filter(graph)?))
            }
            CompositeEdgeFilter::Property(i) => Ok(Arc::new(i.create_edge_filter(graph)?)),
            CompositeEdgeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_edge_filter(graph)?,
            )),
            CompositeEdgeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_edge_filter(graph)?,
            )),
            CompositeEdgeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_edge_filter(graph)?))
            }
        }
    }
}
