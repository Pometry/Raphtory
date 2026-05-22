use std::collections::HashSet;
use std::sync::Arc;

use raphtory_api::core::entities::properties::prop::PropType;
use raphtory_api::core::{Direction, entities::properties::prop::Prop};
use raphtory_core::entities::{VID};
use crate::db::api::state::ops::GraphView;
use crate::db::api::state::ops::filter::NodeDegreeFilterOp;
use crate::db::graph::views::filter::CreateFilter;
use crate::db::graph::views::filter::model::property_filter::{Op, PropertyFilterInput, PropertyRef};
use crate::db::graph::views::filter::model::property_filter::builders::{PropertyExprBuilder, PropertyExprBuilderInput};
use crate::db::graph::views::filter::model::{CombinedFilter, EntityMarker, InternalPropertyFilterBuilder, TryAsCompositeFilter};
use crate::db::graph::views::filter::model;
use crate::db::graph::views::filter::node_filtered_graph::NodeFilteredGraph;
use crate::db::{api::view::{GraphViewOps, NodeViewOps}, graph::views::filter::model::{FilterOperator, property_filter::PropertyFilterValue}};
use crate::errors::GraphError;


#[derive(Clone)]
pub struct DegreeFilterBuilder<M> {
    direction: Direction,
    entity: M
}

impl<M> DegreeFilterBuilder<M> {
    pub fn new(direction: Direction, entity: M) -> Self {
        Self {
            direction,
            entity
        }
    }
}

#[derive(Debug, Clone)]
pub struct DegreeFilter {
    pub direction: Direction,
    pub operator: FilterOperator,
    pub value: PropertyFilterValue
}

impl DegreeFilter {
    pub fn matches<'graph, G: GraphViewOps<'graph>>(&self, graph: &G, node: VID) -> bool {
        let node_view = graph.node(node).unwrap();
        let node_degree = match self.direction {
            Direction::IN => {
                node_view.in_degree()
            },          
            Direction::OUT => {
                node_view.out_degree()
            }, 
            Direction::BOTH => {
                node_view.in_degree() + node_view.out_degree()
            }
        };
        let node_degree_prop =  Prop::U64(node_degree as u64);
        self.operator.apply_to_property(&self.value, Some(&node_degree_prop))
    }
}

impl CreateFilter for DegreeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, NodeDegreeFilterOp<G>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NodeDegreeFilterOp<G>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeDegreeFilterOp::new(graph, self))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl TryAsCompositeFilter for DegreeFilter {
    fn try_as_composite_edge_filter(&self) -> Result<model::edge_filter::CompositeEdgeFilter, GraphError> {
         Err(GraphError::NotSupported)
    }
    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<model::CompositeExplodedEdgeFilter, GraphError>
    {
       Err(GraphError::NotSupported) 
    } 
    fn try_as_composite_node_filter(&self) -> Result<model::CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}  

impl<M> InternalPropertyFilterBuilder for DegreeFilterBuilder<M>
where
    M: Into<EntityMarker> + Send + Sync + Clone + 'static,
    DegreeFilter: CombinedFilter,
    PropertyExprBuilder<M>: InternalPropertyFilterBuilder,
{
    type Filter = DegreeFilter;
    type ExprBuilder = PropertyExprBuilder<M>;
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property("degree".to_string())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.entity.clone()
    }

    fn filter(&self, filter: PropertyFilterInput) -> Self::Filter {
        let value = match filter.prop_value {
           PropertyFilterValue::Single(ref prop_val) => PropertyFilterValue::Single(prop_val.clone().try_cast(PropType::U64).unwrap_or(prop_val.clone())),
           PropertyFilterValue::Set(ref prop_vals) => PropertyFilterValue::Set(Arc::new(prop_vals.iter().map(|val| val.clone().try_cast(PropType::U64).unwrap_or(val.clone())).collect::<HashSet<Prop>>())),
           PropertyFilterValue::None => PropertyFilterValue::None
        };  
        DegreeFilter {
             value,
             direction: self.direction,
             operator: filter.operator 
        }
    }

    fn with_expr_builder(&self, builder: PropertyExprBuilderInput) -> Self::ExprBuilder {
        builder.with_entity(self.entity())
    }
}

pub trait DegreeFilterFactory {
    type Entity: Clone + Send + Sync + Into<EntityMarker> + 'static;

    fn degree(&self, direction: Direction) -> DegreeFilterBuilder<Self::Entity>; 
}
