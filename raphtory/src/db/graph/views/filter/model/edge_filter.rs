use crate::{
    db::{
        api::storage::graph::edges::edge_ref::EdgeStorageRef,
        graph::views::filter::model::{property_filter::PropertyFilter, Filter},
    },
    prelude::GraphViewOps,
};
use std::{collections::HashMap, fmt, fmt::Display};

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
}

impl Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Edge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
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
        }
    }
}
