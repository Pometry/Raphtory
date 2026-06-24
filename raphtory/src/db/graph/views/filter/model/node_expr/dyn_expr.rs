//! Dyn-dispatch traits for property/aggregator/quantifier expression chains.
//!
//! `DynPropertyExpr` is the type-erased equivalent of the chained
//! `EntityExpr` → `EntityAggOps` → `EntityExprFilterOps` API. Each method either:
//!   - terminates the chain by producing an `Arc<dyn DynCreateFilter>`
//!     (comparators / string ops / set ops / unary), or
//!   - extends the chain by producing another `Arc<dyn DynPropertyExpr>`
//!     (selectors / aggregators / quantifiers).
//!
//! Used by the Python `PyPropertyExprBuilder` and `PyPropertyFilterBuilder`
//! wrappers to dispatch chain calls at runtime through typed expressions.
//!
//! ## Chain methods are currently panic stubs
//!
//! Implementing the chain methods (`dyn_first`, `dyn_sum`, …) properly
//! requires distinguishing node-side vs edge-side expressions at the type
//! level. The blanket impl below only bounds `E: EntityExpr`, which is not
//! enough — `BinaryCmpExpr<_, _, M>: CreateFilter` requires `L: NodeExpr`
//! (or `L: EdgeExpr`) depending on `M`. Splitting the blanket into Node /
//! Edge versions creates coherence overlap because primitive types (`Prop`,
//! `u32`, …) impl both `NodeExpr` and `EdgeExpr`.
//!
//! Chain methods are left as panicking default impls until a working
//! resolution is in place. The Python `.sum()`, `.first()`, `.any()`, etc.
//! calls will panic at runtime.

use crate::{
    db::{
        api::{
            state::{ops::GraphView, NodeOp},
            view::BoxableGraphView,
        },
        graph::views::filter::model::{
            edge_expr::EdgeOp,
            node_expr::{CreateOp, EntityExpr},
            EntityMarker,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{ops::Deref, sync::Arc};

trait DynEntityExpr: Send + Sync + 'static {
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

impl EntityExpr for Arc<dyn DynEntityExpr> {
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

pub trait DynCreateOp: DynEntityExpr {
    fn dyn_create_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError>;

    fn dyn_create_edge_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError>;
}

impl<E: CreateOp<Marker: Into<EntityMarker>>> DynCreateOp for E {
    fn dyn_create_node_op<'g>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.create_node_op(graph)
    }

    fn dyn_create_edge_op<'g>(
        &self,
        graph:  Arc<dyn BoxableGraphView + 'g>,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.create_edge_op(graph)
    }
}

impl EntityExpr for Arc<dyn DynCreateOp> {
    type Marker = EntityMarker;

    fn entity(&self) -> Self::Marker {
        self.deref().dyn_entity()
    }
}

impl CreateOp for Arc<dyn DynCreateOp> {
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
}
