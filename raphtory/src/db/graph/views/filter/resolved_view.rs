//! What a filter expression restricts the graph to, as a value: a set of time
//! ranges and a set of layers. Composites combine these algebraically and the
//! result is applied to the base graph once, so a conjunction of two windows
//! narrows to their overlap, a disjunction of two windows becomes a
//! [`MultiWindowedGraph`] over both, and a negated window becomes the two
//! ranges either side of it — each with the time semantics to match, which is
//! what a stack of wrapper graphs could never give.
//!
//! A predicate restricts nothing here: it contributes [`ResolvedView::all`],
//! the identity, and does its work through the node and edge tests instead.
//! That is the whole distinction between `Graph.window(0, 5)`, whose window is
//! the filter and lands in this value, and `Node.window(0, 3).property(..)`,
//! whose window is the scope its predicate is read in and stays private to it.
//!
//! One shape is not representable: a union or complement that restricts time
//! *and* layers at once. `(T1, L1) ∪ (T2, L2)` is a single (time, layers) pair
//! only when the two agree on one dimension, and the complement of `(T, L)` is
//! one pair only when `T` or `L` is unrestricted. Those combinations are
//! refused rather than approximated — the hull would admit events in neither
//! operand.

use crate::{
    db::{
        api::view::{
            diff,
            internal::{time_semantics::TimeRanges, DynGraphArc, GraphView},
        },
        graph::views::{
            layer_graph::LayeredGraph, multi_window_graph::MultiWindowedGraph,
            window_graph::WindowedGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{entities::LayerIds, storage::timeindex::EventTime};
use std::sync::Arc;

/// The time ranges and layers a view expression restricts to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedView {
    pub time: TimeRanges,
    pub layers: LayerIds,
}

impl ResolvedView {
    /// No restriction: every time, every layer. What a predicate contributes.
    pub fn all() -> Self {
        Self {
            time: TimeRanges::all(),
            layers: LayerIds::All,
        }
    }

    pub fn is_all(&self) -> bool {
        self.time.is_all() && self.layers.is_all()
    }

    fn restricts_time(&self) -> bool {
        !self.time.is_all()
    }

    fn restricts_layers(&self) -> bool {
        !self.layers.is_all()
    }

    /// Both restrictions: the overlap of the times and the layers in common.
    /// Always representable, since intersection distributes over the product.
    pub fn and(&self, other: &Self) -> Self {
        Self {
            time: self.time.intersect(&other.time),
            layers: self.layers.intersect(&other.layers),
        }
    }

    /// Either restriction. Representable as one (time, layers) pair only when
    /// the operands agree on one dimension; otherwise the pair
    /// `(T1 ∪ T2, L1 ∪ L2)` would admit `T1`-events on `L2` layers that neither
    /// operand admits.
    pub fn or(&self, other: &Self) -> Result<Self, GraphError> {
        if self.time == other.time || self.layers == other.layers {
            return Ok(Self {
                time: self.time.union(&other.time),
                layers: self.layers.union(&other.layers),
            });
        }
        // One side unrestricted in a dimension absorbs the other in that
        // dimension: (T, All) ∪ (All, L) is not a product, but (T, All) ∪
        // (T2, All) is, and so is (All, All) ∪ anything.
        if self.is_all() || other.is_all() {
            return Ok(Self::all());
        }
        // A rectangle cannot hold this union; the list of rectangles that
        // could needs per-layer time semantics, see #2776.
        Err(GraphError::InvalidGqlFilter(
            "a union of views that restrict both time and layers differently is not \
             representable as one view; combine the time views and the layer views separately"
                .to_string(),
        ))
    }

    /// Neither the times nor the layers. Representable when at most one
    /// dimension is restricted; the complement of a time-and-layer view is a
    /// union across dimensions, which is refused for the reason `or` gives.
    pub fn not<'graph, G: GraphViewOps<'graph>>(&self, graph: G) -> Result<Self, GraphError> {
        match (self.restricts_time(), self.restricts_layers()) {
            (false, false) => Ok(Self {
                time: TimeRanges::empty(),
                layers: LayerIds::All,
            }),
            (true, false) => Ok(Self {
                time: self.time.complement(),
                layers: LayerIds::All,
            }),
            (false, true) => Ok(Self {
                time: TimeRanges::all(),
                layers: diff(&LayerIds::All, graph, &self.layers),
            }),
            (true, true) => Err(GraphError::InvalidGqlFilter(
                "negating a view that restricts both time and layers is not representable as \
                 one view; negate the time view and the layer view separately"
                    .to_string(),
            )),
        }
    }

    /// The graph restricted to these layers and times. Layers first, then the
    /// time set by its shape: none is an empty window, one is a
    /// `WindowedGraph`, more is a `MultiWindowedGraph` — the three
    /// `TimeSemantics` variants.
    pub fn apply<'graph>(self, graph: DynGraphArc<'graph>) -> DynGraphArc<'graph> {
        let graph: DynGraphArc<'graph> = if self.layers.is_all() {
            graph
        } else {
            Arc::new(LayeredGraph::new(graph, self.layers))
        };
        match self.time.as_slice() {
            _ if self.time.is_all() => graph,
            [] => Arc::new(WindowedGraph::new(
                graph,
                Some(EventTime::MIN),
                Some(EventTime::MIN),
            )),
            [w] => Arc::new(WindowedGraph::new(graph, Some(w.start), Some(w.end))),
            _ => Arc::new(MultiWindowedGraph::new(graph, self.time)),
        }
    }
}

/// The view a graph already carries: the ranges its time semantics are
/// bounded to and its layer set. A views_only view resolves by applying its chain
/// to a graph and reading the result back, which is the only way `latest()`
/// and `snapshot_at` can resolve since they depend on the graph's events.
pub fn read_view<G: GraphView>(graph: &G) -> ResolvedView {
    ResolvedView {
        time: graph.node_time_semantics().ranges(),
        layers: graph.layer_ids().clone(),
    }
}

/// The complement of `view` inside `graph`'s own view, or `None` when it is
/// not representable as one view.
fn complement_within<'graph, G: GraphViewOps<'graph>>(
    view: &ResolvedView,
    graph: &G,
) -> Option<ResolvedView> {
    view.not(graph.clone())
        .ok()
        .map(|c| c.and(&read_view(graph)))
}

fn or_within(left: &Option<ResolvedView>, right: &Option<ResolvedView>) -> Option<ResolvedView> {
    left.as_ref()?.or(right.as_ref()?).ok()
}

fn and_within(left: &Option<ResolvedView>, right: &Option<ResolvedView>) -> Option<ResolvedView> {
    Some(left.as_ref()?.and(right.as_ref()?))
}

/// What a filter expression restricts the result to.
///
/// A tree of views alone selects exactly one view, and its negation is that
/// view's complement. Once a predicate is in the tree a view only says where
/// to look — the predicate decides what is kept — and the negation's view can
/// no longer be derived from the result's: `~(window & P)` can hold anywhere,
/// while `~(window | P)` holds only outside the window. So a tree with
/// predicates carries both views, and `not` swaps them instead of normalising
/// the tree.
///
/// `None` in `WithPredicates` means that side is not one (time, layers) pair
/// (a union or complement across both axes); it is an error only if that side
/// is actually applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewBounds {
    /// Nothing but views in the tree: exactly what the expression selects.
    ViewOnly(ResolvedView),
    /// A predicate is in the tree, so a view only says where to look:
    /// `result` is where the result lives, `negated` where its negation lives.
    WithPredicates {
        result: Option<ResolvedView>,
        negated: Option<ResolvedView>,
    },
}

impl ViewBounds {
    /// A predicate: restricts nothing, and neither does its negation.
    pub fn predicate() -> Self {
        Self::WithPredicates {
            result: Some(ResolvedView::all()),
            negated: Some(ResolvedView::all()),
        }
    }

    pub fn is_view_only(&self) -> bool {
        matches!(self, Self::ViewOnly(_))
    }

    /// Both sides as bounds, so a view can combine with a tree that has
    /// predicates in it.
    fn sides<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
    ) -> (Option<ResolvedView>, Option<ResolvedView>) {
        match self {
            Self::ViewOnly(view) => (Some(view.clone()), complement_within(view, graph)),
            Self::WithPredicates { result, negated } => (result.clone(), negated.clone()),
        }
    }

    /// A conjunction: both restrictions. `right` must have been resolved
    /// against the graph `left` produces, since `latest()` and `snapshot_at`
    /// depend on the graph they are applied to; given that, intersecting the
    /// two is the same as applying them in sequence.
    pub fn and<'graph, G: GraphViewOps<'graph>>(left: &Self, right: &Self, graph: &G) -> Self {
        if let (Self::ViewOnly(a), Self::ViewOnly(b)) = (left, right) {
            return Self::ViewOnly(a.and(b));
        }
        let (left_result, left_negated) = left.sides(graph);
        let (right_result, right_negated) = right.sides(graph);
        Self::WithPredicates {
            result: and_within(&left_result, &right_result),
            negated: or_within(&left_negated, &right_negated),
        }
    }

    /// A disjunction: `L ∪ R`, negated `~L ∩ ~R`. Two views that restrict
    /// time and layers differently have no single union and are refused.
    pub fn or<'graph, G: GraphViewOps<'graph>>(
        left: &Self,
        right: &Self,
        graph: &G,
    ) -> Result<Self, GraphError> {
        if let (Self::ViewOnly(a), Self::ViewOnly(b)) = (left, right) {
            return Ok(Self::ViewOnly(a.or(b)?));
        }
        let (left_result, left_negated) = left.sides(graph);
        let (right_result, right_negated) = right.sides(graph);
        Ok(Self::WithPredicates {
            result: or_within(&left_result, &right_result),
            negated: and_within(&left_negated, &right_negated),
        })
    }

    /// The complement of a view, or the swapped sides of a tree with
    /// predicates.
    pub fn not<'graph, G: GraphViewOps<'graph>>(self, graph: G) -> Result<Self, GraphError> {
        Ok(match self {
            Self::ViewOnly(view) => Self::ViewOnly(view.not(graph)?),
            Self::WithPredicates { result, negated } => Self::WithPredicates {
                result: negated,
                negated: result,
            },
        })
    }

    /// `graph` restricted to the view the result lives in.
    pub fn apply<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        Ok(self.resolved()?.apply(Arc::new(graph)))
    }

    /// The view the result lives in, or the error for an unrepresentable one.
    pub fn resolved(self) -> Result<ResolvedView, GraphError> {
        match self {
            Self::ViewOnly(view) => Ok(view),
            Self::WithPredicates { result, .. } => result.ok_or_else(|| {
                GraphError::InvalidGqlFilter(
                    "a union or negation of views that restrict both time and layers is not \
                     representable as one view; combine or negate the time views and the layer \
                     views separately"
                        .to_string(),
                )
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use raphtory_api::core::{entities::LayerId, storage::timeindex::AsTime};
    use raphtory_storage::core_ops::CoreGraphOps;
    use std::ops::Range;

    fn time(ranges: &[Range<i64>]) -> TimeRanges {
        TimeRanges::new(ranges.iter().map(|r| EventTime::range(r.clone())).collect())
    }

    fn view(time_ranges: &[Range<i64>], layers: LayerIds) -> ResolvedView {
        ResolvedView {
            time: time(time_ranges),
            layers,
        }
    }

    #[test]
    fn a_predicate_contributes_the_identity() {
        let w = view(&[0..5], LayerIds::All);
        assert_eq!(w.and(&ResolvedView::all()), w);
        assert_eq!(ResolvedView::all().and(&w), w);
        assert_eq!(w.or(&ResolvedView::all()).unwrap(), ResolvedView::all());
    }

    #[test]
    fn two_windows_and_to_their_overlap_and_or_to_both() {
        let a = view(&[0..5], LayerIds::All);
        let b = view(&[3..8], LayerIds::All);
        assert_eq!(a.and(&b).time, time(&[3..5]));
        assert_eq!(a.or(&b).unwrap().time, time(&[0..8]));
        let far = view(&[6..10], LayerIds::All);
        assert_eq!(a.or(&far).unwrap().time.len(), 2);
    }

    // Layer ids are not a contiguous range from zero — a graph reserves ids
    // of its own — so the complement is taken over the graph's actual layers.
    #[test]
    fn not_of_a_window_is_the_two_sides_and_of_a_layer_the_other_layers() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("x")).unwrap();
        g.add_edge(2, "c", "d", NO_PROPS, Some("y")).unwrap();

        let w = view(&[3..5], LayerIds::All).not(g.clone()).unwrap();
        assert_eq!(w.time.len(), 2);
        assert!(w.layers.is_all());

        let x_only = read_view(&g.layers("x").unwrap());
        let not_x = x_only.not(g.clone()).unwrap();
        assert!(not_x.time.is_all());
        assert_eq!(not_x.layers, read_view(&g.layers("y").unwrap()).layers);
    }

    #[test]
    fn mixed_time_and_layer_unions_and_negations_are_refused() {
        let w = view(&[0..5], LayerIds::All);
        let l = ResolvedView {
            time: TimeRanges::all(),
            layers: LayerIds::One(LayerId(1)),
        };
        assert!(w.or(&l).is_err());
        assert!(w.and(&l).not(Graph::new()).is_err());
        // Agreeing on one dimension is fine.
        let wl1 = w.and(&l);
        let wl2 = view(&[6..9], LayerIds::One(LayerId(1)));
        assert!(wl1.or(&wl2).is_ok());
    }

    #[test]
    fn not_of_the_identity_is_empty_and_back() {
        let g = Graph::new();
        let none = ResolvedView::all().not(g.clone()).unwrap();
        assert!(none.time.is_empty());
        assert_eq!(none.not(g).unwrap(), ResolvedView::all());
    }

    #[test]
    fn reading_a_view_back_from_a_graph() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, Some("x")).unwrap();
        g.add_edge(7, "a", "b", NO_PROPS, Some("y")).unwrap();
        assert_eq!(read_view(&g), ResolvedView::all());
        assert_eq!(read_view(&g.window(0, 5)), view(&[0..5], LayerIds::All));
        let layered = g.layers("y").unwrap();
        assert_eq!(
            read_view(&layered).layers,
            LayerIds::One(g.get_layer_id("y").unwrap())
        );
        assert!(read_view(&layered).time.is_all());
        // A resolved view applied and read back round-trips, gaps included.
        let two = view(&[0..5, 6..10], LayerIds::All);
        let applied = two.clone().apply(Arc::new(g.clone()));
        assert_eq!(read_view(&applied), two);
    }

    #[test]
    fn bounds_of_negations_follow_the_tree_without_normalising_it() {
        let g = Graph::new();
        g.add_edge(1, "a", "b", NO_PROPS, None).unwrap();
        let window = ViewBounds::ViewOnly(view(&[0..5], LayerIds::All));
        let predicate = ViewBounds::predicate();

        // ~(window & P): nodes outside the window or failing P — anywhere.
        let and = ViewBounds::and(&window, &predicate, &g);
        assert!(!and.is_view_only());
        assert_eq!(
            and.clone().not(g.clone()).unwrap().resolved().unwrap(),
            ResolvedView::all()
        );

        // ~(window | P): nodes with no event in the window and failing P — so
        // every event they have is outside the window.
        let or = ViewBounds::or(&window, &predicate, &g).unwrap();
        assert_eq!(or.clone().resolved().unwrap(), ResolvedView::all());
        assert_eq!(
            or.not(g.clone()).unwrap().resolved().unwrap().time,
            time(&[0..5]).complement()
        );

        // Views alone stay exact through a negation, and come back.
        assert!(window.is_view_only());
        let back = window.clone().not(g.clone()).unwrap().not(g).unwrap();
        assert_eq!(back, window);
    }
}
