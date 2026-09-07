//! Boolean composition of edge filters, as a per-edge boolean rather than nested graphs.
//!
//! A filter expression can be lowered two ways. Composing *graphs* — one wrapper
//! per operand, each narrowing the last — is how the entity path works, and it
//! cannot express `or` or `not`: a wrapper inherits its time semantics from the
//! graph it wraps, so a view operand's restriction is simply absent from the
//! composite, and each wrapper answers several hooks (`internal_filter_edge`,
//! `internal_filter_edge_layer`, `internal_nodes_filtered`, the exploded ones)
//! which then have to agree with each other for every combination of operand
//! kinds. They do not.
//!
//! Composing *booleans* has neither problem. Each operand is asked the one
//! question that already has a correct answer — "is this edge in your view?",
//! via the fully composed [`FilterOps::filter_edge`] — and `&`, `|` and `~`
//! combine those answers. This is what `nodes[...]` does with node ops, and why
//! it is the only path that obeys set algebra today.
//!
//! The trade, which is inherent rather than incidental: a composite decides
//! **membership** only. `window(1, 2) | property > 5` admits an edge that is in
//! the window and an edge that is not, so there is no single time semantics the
//! result could carry — the events of the edges it admits come from the base
//! graph. A single view is unaffected and still clips history as it does today.

use crate::db::api::{
    properties::internal::{
        InheritEdgePropertySchemaOps, InheritNodePropertySchemaOps, InheritPropertiesOps,
    },
    state::ops::GraphView,
    view::internal::{
        FilterOps, Immutable, InheritEdgeHistoryFilter, InheritEdgeLayerFilterOps,
        InheritExplodedEdgeFilterOps, InheritListOps, InheritMaterialize, InheritNodeFilterOps,
        InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics, InternalEdgeFilterOps,
        Static,
    },
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, layer_ops::InheritLayerOps};
use storage::EdgeEntryRef;

/// A per-edge predicate that a filter expression lowers to.
pub trait EdgeFilterOp: Send + Sync {
    /// Whether this edge passes.
    fn test(&self, edge: EdgeEntryRef) -> bool;

    /// Whether this test can reject anything. `false` lets the engine skip it,
    /// and must only be returned when [`EdgeFilterOp::test`] is true for every edge.
    fn is_filtered(&self) -> bool;
}

/// The leaf: an edge passes if the operand's own view contains it.
///
/// `filter_edge` is the composed predicate — node filters, window, layers and
/// exploded filters included — so a windowed operand answers with its window
/// applied. That is precisely what nesting the wrapper graphs fails to do.
pub struct EdgeExistsOp<G> {
    graph: G,
}

impl<G: GraphView> EdgeExistsOp<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G: GraphView> EdgeFilterOp for EdgeExistsOp<G> {
    #[inline]
    fn test(&self, edge: EdgeEntryRef) -> bool {
        self.graph.filter_edge(edge)
    }

    #[inline]
    fn is_filtered(&self) -> bool {
        self.graph.filtered()
    }
}

pub struct AndTest<L, R> {
    left: L,
    right: R,
}

impl<L: EdgeFilterOp, R: EdgeFilterOp> EdgeFilterOp for AndTest<L, R> {
    #[inline]
    fn test(&self, edge: EdgeEntryRef) -> bool {
        self.left.test(edge) && self.right.test(edge)
    }

    #[inline]
    fn is_filtered(&self) -> bool {
        self.left.is_filtered() || self.right.is_filtered()
    }
}

pub struct OrTest<L, R> {
    left: L,
    right: R,
}

impl<L: EdgeFilterOp, R: EdgeFilterOp> EdgeFilterOp for OrTest<L, R> {
    #[inline]
    fn test(&self, edge: EdgeEntryRef) -> bool {
        self.left.test(edge) || self.right.test(edge)
    }

    #[inline]
    fn is_filtered(&self) -> bool {
        // An `or` can only reject an edge both sides reject, so it is
        // unfiltered as soon as either side admits everything.
        self.left.is_filtered() && self.right.is_filtered()
    }
}

pub struct NotTest<T> {
    inner: T,
}

impl<T: EdgeFilterOp> EdgeFilterOp for NotTest<T> {
    #[inline]
    fn test(&self, edge: EdgeEntryRef) -> bool {
        !self.inner.test(edge)
    }

    #[inline]
    fn is_filtered(&self) -> bool {
        // The complement of an unfiltered test is empty, which is still a
        // restriction, so a negation always filters.
        true
    }
}

/// Combinators, so a lowering reads as the expression it came from.
pub trait EdgeFilterOpExt: EdgeFilterOp + Sized {
    fn and<T: EdgeFilterOp>(self, other: T) -> AndTest<Self, T> {
        AndTest {
            left: self,
            right: other,
        }
    }

    fn or<T: EdgeFilterOp>(self, other: T) -> OrTest<Self, T> {
        OrTest {
            left: self,
            right: other,
        }
    }

    fn negate(self) -> NotTest<Self> {
        NotTest { inner: self }
    }
}

impl<T: EdgeFilterOp + Sized> EdgeFilterOpExt for T {}

/// The single wrapper graph a lowered expression produces.
///
/// It answers `internal_filter_edge` from the test and inherits every other
/// filter hook, so there are no sibling hooks left to contradict it — the
/// failure mode that makes nested `And`/`Or`/`Not` graphs inconsistent.
#[derive(Debug, Clone)]
pub struct EdgeOpFilteredGraph<G, T> {
    base: G,
    test: T,
}

impl<G: GraphView, T: EdgeFilterOp> EdgeOpFilteredGraph<G, T> {
    pub fn new(base: G, test: T) -> Self {
        Self { base, test }
    }
}

impl<G, T> Base for EdgeOpFilteredGraph<G, T> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.base
    }
}

impl<G, T> Static for EdgeOpFilteredGraph<G, T> {}
impl<G, T> Immutable for EdgeOpFilteredGraph<G, T> {}

impl<G: GraphView, T: EdgeFilterOp> InheritCoreGraphOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritStorageOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritLayerOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritListOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritMaterialize for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritNodeFilterOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritPropertiesOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritNodePropertySchemaOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritEdgePropertySchemaOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritTimeSemantics for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritNodeHistoryFilter for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritEdgeHistoryFilter for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritEdgeLayerFilterOps for EdgeOpFilteredGraph<G, T> {}
impl<G: GraphView, T: EdgeFilterOp> InheritExplodedEdgeFilterOps for EdgeOpFilteredGraph<G, T> {}

impl<G: GraphView, T: EdgeFilterOp> InternalEdgeFilterOps for EdgeOpFilteredGraph<G, T> {
    #[inline]
    fn internal_edge_filtered(&self) -> bool {
        self.test.is_filtered() || self.base.internal_edge_filtered()
    }

    #[inline]
    fn internal_edge_list_trusted(&self) -> bool {
        false
    }

    #[inline]
    fn internal_filter_edge(&self, edge: EdgeEntryRef, layer_ids: &LayerIds) -> bool {
        self.base.internal_filter_edge(edge, layer_ids) && self.test.test(edge)
    }
}

impl<T: EdgeFilterOp + ?Sized> EdgeFilterOp for std::sync::Arc<T> {
    #[inline]
    fn test(&self, edge: EdgeEntryRef) -> bool {
        (**self).test(edge)
    }

    #[inline]
    fn is_filtered(&self) -> bool {
        (**self).is_filtered()
    }
}
