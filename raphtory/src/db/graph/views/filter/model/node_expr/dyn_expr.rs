//! Type-erased forms of the expression traits, so a filter compiled from a
//! tree can hold any entity expression behind one `Arc<dyn …>` and still
//! report its static type and whether it can be missing.

use crate::{
    db::{
        api::{
            state::{ops::GraphView, NodeOp},
            view::BoxableGraphView,
        },
        graph::views::filter::model::{
            edge_expr::EdgeOp,
            edge_filter::EdgeEndpointWrapper,
            filter_operator::ElemQual,
            node_expr::{
                AvgExpr, CreateOp, EntityAggOps, EntityExpr, FirstExpr, LastExpr, LenExpr, MaxExpr,
                MinExpr, PredicateLhs, SumExpr,
            },
            CreateView, EntityMarker, PropertyExpr,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{ops::Deref, sync::Arc};

pub trait DynEntityExpr: Send + Sync + 'static {
    fn dyn_entity(&self) -> EntityMarker;
    fn dyn_prop_type(&self) -> PropType;
    fn dyn_nullable(&self) -> bool;
}

impl<E: EntityExpr<Marker: Into<EntityMarker>>> DynEntityExpr for E {
    fn dyn_entity(&self) -> EntityMarker {
        self.entity().into()
    }

    fn dyn_prop_type(&self) -> PropType {
        self.prop_type()
    }

    fn dyn_nullable(&self) -> bool {
        self.nullable()
    }
}

pub trait DynTemporal: DynCreateOp {
    fn temporal(&self) -> Arc<dyn DynCreateOp>;
}

impl<E: EntityExpr + CreateView + Send + Sync + 'static> DynTemporal for PropertyExpr<E> {
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.temporal())
    }
}

impl<E> DynTemporal for EdgeEndpointWrapper<PropertyExpr<E>>
where
    E: EntityExpr + CreateView + Clone + Send + Sync + 'static,
    Self: DynCreateOp,
{
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.temporal())
    }
}

/// An endpoint read built from an erased node value: switching to the history
/// happens on the node side, and the result is read through the same endpoint.
impl DynTemporal for EdgeEndpointWrapper<Arc<dyn DynTemporal>> {
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(EdgeEndpointWrapper::new(
            self.inner.temporal(),
            self.endpoint(),
        ))
    }
}

pub trait DynCreateOp: DynEntityExpr {
    fn dyn_selects_node_id(&self) -> bool;

    fn dyn_create_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError>;

    fn dyn_create_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError>;

    fn dyn_create_qualified_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>;

    fn dyn_create_qualified_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError>;
}

impl<E: CreateOp> DynCreateOp for E {
    fn dyn_selects_node_id(&self) -> bool {
        self.selects_node_id()
    }

    fn dyn_create_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.create_node_op(graph)
    }

    fn dyn_create_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.create_edge_op(graph)
    }

    fn dyn_create_qualified_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        self.create_qualified_node_op(graph)
    }

    fn dyn_create_qualified_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        self.create_qualified_edge_op(graph)
    }
}

impl<T: DynEntityExpr + ?Sized> EntityExpr for Arc<T> {
    type Marker = EntityMarker;

    fn entity(&self) -> Self::Marker {
        self.deref().dyn_entity()
    }

    fn prop_type(&self) -> PropType {
        self.deref().dyn_prop_type()
    }

    fn nullable(&self) -> bool {
        self.deref().dyn_nullable()
    }
}

impl<T: DynEntityExpr + ?Sized> PredicateLhs for Arc<T> {}

impl<T: DynCreateOp + ?Sized> CreateOp for Arc<T> {
    fn selects_node_id(&self) -> bool {
        self.as_ref().dyn_selects_node_id()
    }

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.deref().dyn_create_node_op(Arc::new(graph))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.deref().dyn_create_edge_op(Arc::new(graph))
    }

    fn create_qualified_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        self.deref().dyn_create_qualified_node_op(Arc::new(graph))
    }

    fn create_qualified_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        self.deref().dyn_create_qualified_edge_op(Arc::new(graph))
    }
}

impl<T: DynCreateOp + ?Sized> EntityAggOps for Arc<T> {
    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }
    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }
    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }
    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }
    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }
    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }
    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}
