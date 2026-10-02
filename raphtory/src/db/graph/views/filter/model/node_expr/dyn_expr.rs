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
            expr::{DynCreateHistory, ValueTest},
            node_expr::{CreateOp, EntityAggOps, EntityExpr, IndexQuery, IndexTerm, Pushdown},
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
    fn dyn_constant(&self) -> Option<Prop>;
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

    fn dyn_constant(&self) -> Option<Prop> {
        self.constant()
    }
}

pub trait DynTemporal: DynCreateOp {
    /// The history as one list value.
    fn temporal(&self) -> Arc<dyn DynCreateOp>;

    /// The history as a stream, for a consumer that walks it.
    fn history(&self) -> Arc<dyn DynCreateHistory>;
}

impl<E: EntityExpr + CreateView + Send + Sync + 'static> DynTemporal for PropertyExpr<E> {
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(self.temporal())
    }

    fn history(&self) -> Arc<dyn DynCreateHistory> {
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

    fn history(&self) -> Arc<dyn DynCreateHistory> {
        Arc::new(EdgeEndpointWrapper::new(
            self.inner.temporal(),
            self.endpoint(),
        ))
    }
}

/// An endpoint term built from an erased node value: switching to the history
/// happens on the node side, and the result is read through the same endpoint.
impl DynTemporal for EdgeEndpointWrapper<Arc<dyn DynTemporal>> {
    fn temporal(&self) -> Arc<dyn DynCreateOp> {
        Arc::new(EdgeEndpointWrapper::new(
            self.inner.temporal(),
            self.endpoint(),
        ))
    }

    fn history(&self) -> Arc<dyn DynCreateHistory> {
        Arc::new(EdgeEndpointWrapper::new(
            DynTemporal::history(self.inner.as_ref()),
            self.endpoint(),
        ))
    }
}

pub trait DynCreateOp: DynEntityExpr {
    fn dyn_create_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError>;

    fn dyn_create_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError>;

    fn dyn_history(&self) -> Option<Arc<dyn DynCreateHistory>>;
    fn dyn_index_term(&self) -> Option<IndexTerm>;
    fn dyn_index_query(&self) -> Option<IndexQuery>;
    fn dyn_pushdown(&self) -> Option<Pushdown>;
    fn dyn_value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)>;
}

impl<E: CreateOp> DynCreateOp for E {
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

    fn dyn_history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        self.history()
    }

    fn dyn_index_term(&self) -> Option<IndexTerm> {
        self.index_term()
    }

    fn dyn_index_query(&self) -> Option<IndexQuery> {
        self.index_query()
    }

    fn dyn_pushdown(&self) -> Option<Pushdown> {
        self.pushdown()
    }

    fn dyn_value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        self.value_test()
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

    fn constant(&self) -> Option<Prop> {
        self.deref().dyn_constant()
    }
}

impl<T: DynCreateOp + ?Sized> CreateOp for Arc<T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.deref().dyn_create_node_op(graph.into_dyn_graph_arc())
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.deref().dyn_create_edge_op(graph.into_dyn_graph_arc())
    }

    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        self.deref().dyn_history()
    }

    fn index_term(&self) -> Option<IndexTerm> {
        self.deref().dyn_index_term()
    }

    fn index_query(&self) -> Option<IndexQuery> {
        self.deref().dyn_index_query()
    }

    fn pushdown(&self) -> Option<Pushdown> {
        self.deref().dyn_pushdown()
    }

    fn value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        self.deref().dyn_value_test()
    }
}

impl<T: DynCreateOp + ?Sized> EntityAggOps for Arc<T> {}
