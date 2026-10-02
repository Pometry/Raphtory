//! From expression data to a filter the engine can apply.
//!
//! Terms and views replay onto the erased factories exactly as the typed API
//! would build them, and every other node of a value expression becomes the
//! typed expression of the rust API over those erased terms: the tree is data,
//! and the typed expressions are the one compiler. A `Bool`-typed value is a
//! filter on its entity through [`Predicate`]; this module adds what only a
//! whole filter knows: which entities each leg answers for, how `and`, `or`
//! and `not` combine those answers, and where a view applies.

use super::{Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, ViewOp};
use crate::{
    db::{
        api::{
            state::{
                ops::{filter::NodeExistsOp, NodeFilterOp},
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                and_filter::AndFilter,
                answer::{all_of, any_of, combine, compose, Answer, FilterAnswer, Question},
                dyn_factory::{DynEdgeFilterFactory, DynNodeFilterFactory},
                edge_expr::ops::{AndEdgeOp, EdgeExistsOp},
                edge_filter::{EdgeEndpointWrapper, EdgeFilter, Endpoint},
                exploded_edge_filter::ExplodedEdgeFilter,
                filter_operator::{SetOp, UnaryOp},
                graph_filter::GraphFilter,
                is_active_edge_filter::IsActiveEdge,
                is_active_node_filter::IsActiveNode,
                is_deleted_filter::IsDeletedEdge,
                is_self_loop_filter::IsSelfLoopEdge,
                is_valid_filter::IsValidEdge,
                node_expr::{
                    AllExpr, AndExpr, AnyExpr, BinaryCmpExpr, DynCreateOp, NotExpr, OrExpr,
                    Predicate, PropValueSetExpr, Scoped, StringExpr, UnaryExpr,
                },
                node_filter::NodeFilter,
                DynCreateFilter, DynView, EntityMarker, ViewWrapOps,
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
    prelude::{EntityAggOps, Layer},
};
use raphtory_api::core::Direction;
use std::{fmt::Debug, sync::Arc};

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

// ── leaves ───────────────────────────────────────────────────────────────────

/// The terms an entity offers. Implemented by the leaf enum of each entity.
pub trait Leaf: Clone + Debug + PartialEq + Send + Sync + 'static {
    /// The entity every expression over this leaf type belongs to.
    const ENTITY: EntityMarker;

    /// The erased value this term produces.
    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError>;

    /// The latest value of a property, or its history when `temporal`, seen
    /// through `views`.
    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self;

    /// A metadata entry seen through `views`.
    fn metadata(views: Vec<ViewOp>, name: String) -> Self;

    /// Whether the entity is active inside `views`.
    fn is_active(views: Vec<ViewOp>) -> Self;

    /// A view applied around the term: it scopes the term.
    fn push_view(&mut self, op: ViewOp);

    /// The filter a yes/no expression over this leaf type is.
    fn filter(expr: Expr<Self>) -> FilterExpr;
}

impl<L: Leaf> Expr<L> {
    /// Scope every term in this expression by one more view, applied after
    /// the views the terms already carry.
    pub fn push_view(&mut self, op: ViewOp) {
        match self {
            Expr::Const(_) => {}
            Expr::Term(leaf) => leaf.push_view(op),
            Expr::Agg(_, e)
            | Expr::IsSome(e)
            | Expr::IsNone(e)
            | Expr::Any(e)
            | Expr::All(e)
            | Expr::Not(e) => e.push_view(op),
            Expr::In { expr, .. } => expr.push_view(op),
            Expr::Cmp(_, l, r) | Expr::Str(_, l, r) => {
                l.push_view(op.clone());
                r.push_view(op);
            }
            Expr::And(items) | Expr::Or(items) => {
                for item in items {
                    item.push_view(op.clone());
                }
            }
        }
    }
}

fn node_factory(views: &[ViewOp]) -> Arc<dyn DynNodeFilterFactory> {
    let mut f: Arc<dyn DynNodeFilterFactory> = Arc::new(NodeFilter);
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.window(*start, *end),
            ViewOp::At(t) => f.at(*t),
            ViewOp::After(t) => f.after(*t),
            ViewOp::Before(t) => f.before(*t),
            ViewOp::Latest => Arc::new(f.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(f.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(f.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(f.layer(names.clone())),
        };
    }
    f
}

fn edge_factory(exploded: bool, views: &[ViewOp]) -> Arc<dyn DynEdgeFilterFactory> {
    let mut f: Arc<dyn DynEdgeFilterFactory> = if exploded {
        Arc::new(ExplodedEdgeFilter)
    } else {
        Arc::new(EdgeFilter)
    };
    for op in views {
        f = match op {
            ViewOp::Window { start, end } => f.dyn_window(*start, *end),
            ViewOp::At(t) => f.dyn_at(*t),
            ViewOp::After(t) => f.dyn_after(*t),
            ViewOp::Before(t) => f.dyn_before(*t),
            ViewOp::Latest => f.dyn_latest(),
            ViewOp::SnapshotAt(t) => f.dyn_snapshot_at(*t),
            ViewOp::SnapshotLatest => f.dyn_snapshot_latest(),
            ViewOp::Layers(names) => f.dyn_layer(names.clone()),
        };
    }
    f
}

impl Leaf for NodeLeaf {
    const ENTITY: EntityMarker = EntityMarker::Node;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        Ok(match self {
            NodeLeaf::Field { views, field } => {
                let f = node_factory(views);
                match field {
                    Field::Id => f.dyn_id(),
                    Field::Name => f.dyn_name(),
                    Field::NodeType => f.dyn_node_type(),
                }
            }
            NodeLeaf::Degree { views, direction } => {
                let f = node_factory(views);
                match direction {
                    Direction::BOTH => f.dyn_degree(),
                    Direction::IN => f.dyn_in_degree(),
                    Direction::OUT => f.dyn_out_degree(),
                }
            }
            NodeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = node_factory(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            NodeLeaf::Metadata { views, name } => node_factory(views).dyn_metadata(name.clone()),
            NodeLeaf::IsActive { views } => Arc::new(Scoped {
                view: node_factory(views),
                inner: IsActiveNode,
            }),
        })
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        NodeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        NodeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        NodeLeaf::IsActive { views }
    }

    fn push_view(&mut self, op: ViewOp) {
        self.views_mut().push(op);
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Node(expr)
    }
}

impl NodeLeaf {
    fn views_mut(&mut self) -> &mut Vec<ViewOp> {
        match self {
            NodeLeaf::Field { views, .. }
            | NodeLeaf::Degree { views, .. }
            | NodeLeaf::Property { views, .. }
            | NodeLeaf::Metadata { views, .. }
            | NodeLeaf::IsActive { views } => views,
        }
    }
}

impl Leaf for EdgeLeaf {
    const ENTITY: EntityMarker = EntityMarker::Edge;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let f = |views| edge_factory(false, views);
        Ok(match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = f(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            EdgeLeaf::Metadata { views, name } => f(views).dyn_metadata(name.clone()),
            EdgeLeaf::IsActive { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsActiveEdge,
            }),
            EdgeLeaf::IsValid { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsValidEdge,
            }),
            EdgeLeaf::IsDeleted { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsDeletedEdge,
            }),
            EdgeLeaf::IsSelfLoop { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsSelfLoopEdge,
            }),
            // An endpoint term is a node expression the edge evaluates on the
            // node at that end; its views scope that node term.
            EdgeLeaf::Src(inner) => Arc::new(EdgeEndpointWrapper::new(
                inner.compile_value()?,
                Endpoint::Src,
            )),
            EdgeLeaf::Dst(inner) => Arc::new(EdgeEndpointWrapper::new(
                inner.compile_value()?,
                Endpoint::Dst,
            )),
        })
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        EdgeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        EdgeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        EdgeLeaf::IsActive { views }
    }

    /// A view around an endpoint term scopes the node term at that end.
    fn push_view(&mut self, op: ViewOp) {
        match self {
            EdgeLeaf::Property { views, .. }
            | EdgeLeaf::Metadata { views, .. }
            | EdgeLeaf::IsActive { views }
            | EdgeLeaf::IsValid { views }
            | EdgeLeaf::IsDeleted { views }
            | EdgeLeaf::IsSelfLoop { views } => views.push(op),
            EdgeLeaf::Src(inner) | EdgeLeaf::Dst(inner) => inner.push_view(op),
        }
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::Edge(expr)
    }
}

impl Leaf for ExplodedEdgeLeaf {
    const ENTITY: EntityMarker = EntityMarker::ExplodedEdge;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let f = |views| edge_factory(true, views);
        Ok(match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => {
                let prop = f(views).dyn_property(name.clone());
                if *temporal {
                    prop.temporal()
                } else {
                    prop
                }
            }
            ExplodedEdgeLeaf::Metadata { views, name } => f(views).dyn_metadata(name.clone()),
            ExplodedEdgeLeaf::IsActive { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsActiveEdge,
            }),
            ExplodedEdgeLeaf::IsValid { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsValidEdge,
            }),
            ExplodedEdgeLeaf::IsDeleted { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsDeletedEdge,
            }),
            ExplodedEdgeLeaf::IsSelfLoop { views } => Arc::new(Scoped {
                view: f(views),
                inner: IsSelfLoopEdge,
            }),
        })
    }

    fn property(views: Vec<ViewOp>, name: String, temporal: bool) -> Self {
        ExplodedEdgeLeaf::Property {
            views,
            name,
            temporal,
        }
    }

    fn metadata(views: Vec<ViewOp>, name: String) -> Self {
        ExplodedEdgeLeaf::Metadata { views, name }
    }

    fn is_active(views: Vec<ViewOp>) -> Self {
        ExplodedEdgeLeaf::IsActive { views }
    }

    fn push_view(&mut self, op: ViewOp) {
        self.views_mut().push(op);
    }

    fn filter(expr: Expr<Self>) -> FilterExpr {
        FilterExpr::ExplodedEdge(expr)
    }
}

impl ExplodedEdgeLeaf {
    fn views_mut(&mut self) -> &mut Vec<ViewOp> {
        match self {
            ExplodedEdgeLeaf::Property { views, .. }
            | ExplodedEdgeLeaf::Metadata { views, .. }
            | ExplodedEdgeLeaf::IsActive { views }
            | ExplodedEdgeLeaf::IsValid { views }
            | ExplodedEdgeLeaf::IsDeleted { views }
            | ExplodedEdgeLeaf::IsSelfLoop { views } => views,
        }
    }
}

// ── values ───────────────────────────────────────────────────────────────────

impl<L: Leaf> Expr<L> {
    /// The typed expression this tree stands for, over erased terms. Each
    /// node of the tree is one constructor call; the typed expression decides
    /// its result type, narrows through an index and streams a history the
    /// same way whether rust or the tree built it.
    pub fn compile_value(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        let entity = L::ENTITY;
        Ok(match self {
            Expr::Const(value) => Arc::new(value.clone()),
            Expr::Term(leaf) => leaf.compile()?,
            Expr::Agg(agg, inner) => {
                let op = inner.compile_value()?;
                match agg {
                    Agg::Sum => Arc::new(op.sum()),
                    Agg::Avg => Arc::new(op.avg()),
                    Agg::Min => Arc::new(op.min()),
                    Agg::Max => Arc::new(op.max()),
                    Agg::First => Arc::new(op.first()),
                    Agg::Last => Arc::new(op.last()),
                    Agg::Len => Arc::new(op.len()),
                    Agg::Earliest => Arc::new(op.earliest()),
                    Agg::Latest => Arc::new(op.latest()),
                }
            }
            Expr::Cmp(op, lhs, rhs) => Arc::new(BinaryCmpExpr::new(
                lhs.compile_value()?,
                *op,
                rhs.compile_value()?,
                entity,
            )),
            Expr::Str(op, lhs, rhs) => Arc::new(StringExpr::new(
                lhs.compile_value()?,
                *op,
                rhs.compile_value()?,
                entity,
            )),
            Expr::In {
                expr,
                values,
                negated,
            } => Arc::new(PropValueSetExpr {
                expr: expr.compile_value()?,
                values: values.clone(),
                op: if *negated {
                    SetOp::IsNotIn
                } else {
                    SetOp::IsIn
                },
                entity,
            }),
            Expr::IsSome(inner) => Arc::new(UnaryExpr {
                expr: inner.compile_value()?,
                op: UnaryOp::IsSome,
                entity,
            }),
            Expr::IsNone(inner) => Arc::new(UnaryExpr {
                expr: inner.compile_value()?,
                op: UnaryOp::IsNone,
                entity,
            }),
            Expr::Any(inner) => Arc::new(AnyExpr(inner.compile_value()?)),
            Expr::All(inner) => Arc::new(AllExpr(inner.compile_value()?)),
            Expr::And(items) => Arc::new(AndExpr {
                items: items
                    .iter()
                    .map(Self::compile_value)
                    .collect::<Result<Vec<_>, _>>()?,
                entity,
            }),
            Expr::Or(items) => Arc::new(OrExpr {
                items: items
                    .iter()
                    .map(Self::compile_value)
                    .collect::<Result<Vec<_>, _>>()?,
                entity,
            }),
            Expr::Not(inner) => Arc::new(NotExpr(inner.compile_value()?)),
        })
    }
}

// ── filters ──────────────────────────────────────────────────────────────────

impl FilterExpr {
    /// The erased, applicable form of this filter.
    ///
    /// A view (`View`) applies first: the graph is seen through it and the other
    /// legs run inside it, terms included, the way `graph.window(..).filter(expr)`
    /// does. A view therefore stands alone or is a leg of the top-level `and`
    /// (nested `and`s count as top level); under `or` or `not` it has no meaning the
    /// engine can give it and is refused.
    pub fn compile(&self) -> Result<Arc<dyn DynCreateFilter>, GraphError> {
        let (views, predicates, saw_view) = self.split_top_views();
        if saw_view && views.is_empty() {
            return Err(invalid("a view filter needs at least one view"));
        }
        if views.is_empty() {
            return compose(self);
        }
        let inner: Arc<dyn DynCreateFilter> = if predicates.is_empty() {
            Arc::new(GraphFilter)
        } else {
            combine(
                predicates.iter().map(|p| compose(*p)),
                "and",
                |left, right| Arc::new(AndFilter { left, right }),
            )?
        };
        Ok(Arc::new(Viewed { views, inner }))
    }

    /// The view ops at the top of the filter, in order, and the predicates beside
    /// them. `and` nests flatten; anything else is a predicate. The flag says whether
    /// a `View` node was seen at all, so an empty one can be told from none.
    fn split_top_views(&self) -> (Vec<ViewOp>, Vec<&FilterExpr>, bool) {
        fn walk<'a>(
            filter: &'a FilterExpr,
            views: &mut Vec<ViewOp>,
            predicates: &mut Vec<&'a FilterExpr>,
            saw_view: &mut bool,
        ) {
            match filter {
                FilterExpr::View(ops) => {
                    *saw_view = true;
                    views.extend(ops.iter().cloned());
                }
                FilterExpr::And(items) => {
                    for item in items {
                        walk(item, views, predicates, saw_view);
                    }
                }
                other => predicates.push(other),
            }
        }
        let (mut views, mut predicates, mut saw_view) = (Vec::new(), Vec::new(), false);
        walk(self, &mut views, &mut predicates, &mut saw_view);
        (views, predicates, saw_view)
    }
}

fn view_below_top_level() -> GraphError {
    invalid(
        "a view applies to the whole filter: use it alone or as a leg of the top-level `and`, \
         not under `or` or `not`",
    )
}

/// The graph-level view a chain of view ops describes, applied in order.
fn compile_view(views: &[ViewOp]) -> DynView {
    let mut v: DynView = Arc::new(GraphFilter);
    for op in views {
        v = match op {
            ViewOp::Window { start, end } => v.window(*start, *end),
            ViewOp::At(t) => v.at(*t),
            ViewOp::After(t) => v.after(*t),
            ViewOp::Before(t) => v.before(*t),
            ViewOp::Latest => Arc::new(v.latest()),
            ViewOp::SnapshotAt(t) => Arc::new(v.snapshot_at(*t)),
            ViewOp::SnapshotLatest => Arc::new(v.snapshot_latest()),
            ViewOp::Layers(names) => Arc::new(v.layer(Layer::from(names.clone()))),
        };
    }
    v
}

/// A filter applied inside a view: the graph is seen through `views` first and
/// `inner` runs on that graph, terms included, so `and: [view, pred]` is
/// `graph.view(..).filter(pred)`. As a per-node or per-edge predicate it also asks
/// that the entity exist in the view, the way the filtered graph would.
#[derive(Clone)]
struct Viewed {
    views: Vec<ViewOp>,
    inner: Arc<dyn DynCreateFilter>,
}

impl Viewed {
    fn view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        compile_view(&self.views).create_dyn_graph_filter(graph.into_dyn_graph_arc())
    }
}

impl CreateFilter for Viewed {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        self.inner.create_dyn_graph_filter(viewed)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        let inside = self.inner.create_dyn_node_filter(viewed.clone())?;
        Ok(Arc::new(NodeExistsOp::new(viewed).and(inside)))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let viewed = self.view(graph)?;
        let inside = self.inner.create_dyn_edge_filter(viewed.clone())?;
        Ok(Arc::new(AndEdgeOp {
            left: EdgeExistsOp::new(viewed),
            right: inside,
        }))
    }
}

/// A tree answers the two questions node by node, with the same rule as the
/// typed combinators: a leaf answers its own entity's question, `and` drops
/// legs that leave the question open, `or` needs every leg, and `not` asks
/// for the opposite answer. A view has no answer below the top level.
impl FilterAnswer for FilterExpr {
    fn answer(&self, question: Question, negated: bool) -> Result<Option<Answer>, GraphError> {
        match self {
            FilterExpr::Node(expr) => {
                Predicate::new(expr.compile_value()?).answer(question, negated)
            }
            FilterExpr::Edge(expr) => {
                Predicate::new(expr.compile_value()?).answer(question, negated)
            }
            FilterExpr::ExplodedEdge(expr) => {
                Predicate::new(expr.compile_value()?).answer(question, negated)
            }
            FilterExpr::Opaque(filter) => {
                Predicate::new(filter.0.clone()).answer(question, negated)
            }
            FilterExpr::View(_) => Err(view_below_top_level()),
            FilterExpr::And(items) => all_of(
                items.iter().map(|item| item.answer(question, negated)),
                negated,
            ),
            FilterExpr::Or(items) => any_of(
                items.iter().map(|item| item.answer(question, negated)),
                negated,
            ),
            FilterExpr::Not(inner) => inner.answer(question, !negated),
        }
    }
}

/// A tree is a filter in its own right: applying it compiles it first.
impl CreateFilter for FilterExpr {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        self.compile()?.create_graph_filter(graph)
    }

    /// An edge test says nothing about which nodes belong in a node
    /// collection, so a filter that tests edges anywhere is refused here,
    /// whatever it compiles to.
    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if self.tests_edges() {
            return Err(GraphError::NotNodeFilter);
        }
        self.compile()?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.compile()?.create_edge_filter(graph)
    }
}
