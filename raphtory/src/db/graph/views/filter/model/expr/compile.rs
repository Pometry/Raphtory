//! From expression data to a filter the engine can apply.
//!
//! A term's views fold into a view chain over the graph, the term becomes the
//! typed expression that reads through it, and every other node of a value
//! expression becomes the typed expression over its compiled children: the
//! tree is the data every API builds, and the typed expressions are the one
//! compiler. A `Bool`-typed value is a filter on its entity through
//! [`Predicate`]; this module adds what only a whole filter knows: which
//! entities each leg answers for, how `and`, `or` and `not` combine those
//! answers, and where a view applies.

use super::{
    builder::Chain, Agg, EdgeLeaf, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeLeaf, ViewOp,
};
use crate::{
    db::{
        api::{
            state::{
                ops::{
                    filter::NodeExistsOp,
                    node::{Id, Name, Type},
                    NodeFilterOp,
                },
                NodeOp,
            },
            view::internal::{DynGraphArc, GraphView},
        },
        graph::views::filter::{
            model::{
                after_bounds,
                and_filter::AndFilter,
                answer::{all_of, any_of, combine, compose, Answer, FilterAnswer, Question},
                at_bounds, before_bounds,
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
                latest_filter::Latest,
                layered_filter::{DefaultLayer, ExcludeLayers, Layered},
                node_expr::{
                    AllExpr, AndExpr, AnyExpr, AvgExpr, BinaryCmpExpr, DegreeExpr, DynCreateOp,
                    EarliestExpr, FirstExpr, LastExpr, LatestExpr, LenExpr, MaxExpr, MinExpr,
                    NodeFieldExpr, NotExpr, OrExpr, Predicate, PropValueSetExpr, Scoped,
                    StringExpr, SumExpr, UnaryExpr,
                },
                node_filter::NodeFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                subgraph_filter::{ExcludeNodes, Subgraph, SubgraphNodeTypes, Valid},
                windowed_filter::{ShrinkEnd, ShrinkStart, Windowed},
                CreateView, DynCreateFilter, DynCreateView, EntityMarker, MetadataExpr,
                PropertyExpr,
            },
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;
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

/// A view chain over the graph, as the tree's view ops describe it, over the
/// identity view `root`, applied in order. A window inside a window narrows and
/// never widens, because the graph's own `window` intersects with the view it
/// is applied to, as `graph.window(..).window(..)` does.
fn view_chain(root: Arc<dyn DynCreateView>, views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    views.iter().fold(root, |chain, op| match op {
        ViewOp::Window { start, end } => window((*start, *end), chain),
        ViewOp::At(t) => window(at_bounds(*t), chain),
        ViewOp::After(t) => window(after_bounds(*t), chain),
        ViewOp::Before(t) => window(before_bounds(*t), chain),
        ViewOp::Latest => Arc::new(Latest::new(chain)),
        ViewOp::SnapshotAt(t) => Arc::new(SnapshotAt::new(*t, chain)),
        ViewOp::SnapshotLatest => Arc::new(SnapshotLatest::new(chain)),
        ViewOp::Layers(names) => Arc::new(Layered::from_layers(names.clone(), chain)),
        ViewOp::DefaultLayer => Arc::new(DefaultLayer::new(chain)),
        ViewOp::ExcludeLayers(names) => Arc::new(ExcludeLayers::from_layers(names.clone(), chain)),
        ViewOp::ShrinkStart(t) => Arc::new(ShrinkStart::new(*t, chain)),
        ViewOp::ShrinkEnd(t) => Arc::new(ShrinkEnd::new(*t, chain)),
        ViewOp::ExcludeNodes(ids) => Arc::new(ExcludeNodes::new(ids.clone(), chain)),
        ViewOp::Subgraph(ids) => Arc::new(Subgraph::new(ids.clone(), chain)),
        ViewOp::SubgraphNodeTypes(types) => Arc::new(SubgraphNodeTypes::new(types.clone(), chain)),
        ViewOp::Valid => Arc::new(Valid::new(chain)),
    })
}

fn window(
    (start, end): (EventTime, EventTime),
    chain: Arc<dyn DynCreateView>,
) -> Arc<dyn DynCreateView> {
    Arc::new(Windowed::new(start, end, chain))
}

fn node_chain(views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    view_chain(Arc::new(NodeFilter), views)
}

fn edge_chain(exploded: bool, views: &[ViewOp]) -> Arc<dyn DynCreateView> {
    if exploded {
        view_chain(Arc::new(ExplodedEdgeFilter), views)
    } else {
        view_chain(Arc::new(EdgeFilter), views)
    }
}

/// A property read through `view_expr`: its latest value, or its history.
fn property(
    view_expr: Arc<dyn DynCreateView>,
    name: &str,
    entity: EntityMarker,
    temporal: bool,
) -> Arc<dyn DynCreateOp> {
    let prop = PropertyExpr {
        view_expr,
        name: name.to_owned(),
        entity,
    };
    if temporal {
        Arc::new(prop.temporal())
    } else {
        Arc::new(prop)
    }
}

fn metadata(
    view_expr: Arc<dyn DynCreateView>,
    name: &str,
    entity: EntityMarker,
) -> Arc<dyn DynCreateOp> {
    Arc::new(MetadataExpr {
        view_expr,
        name: name.to_owned(),
        entity,
    })
}

impl Leaf for NodeLeaf {
    const ENTITY: EntityMarker = EntityMarker::Node;

    fn compile(&self) -> Result<Arc<dyn DynCreateOp>, GraphError> {
        Ok(match self {
            NodeLeaf::Field { views, field } => {
                let view_expr = node_chain(views);
                match field {
                    Field::Id => Arc::new(NodeFieldExpr {
                        view_expr,
                        field: Id,
                    }),
                    Field::Name => Arc::new(NodeFieldExpr {
                        view_expr,
                        field: Name,
                    }),
                    Field::NodeType => Arc::new(NodeFieldExpr {
                        view_expr,
                        field: Type,
                    }),
                }
            }
            NodeLeaf::Degree { views, direction } => Arc::new(DegreeExpr {
                dir: *direction,
                view_expr: node_chain(views),
            }),
            NodeLeaf::Property {
                views,
                name,
                temporal,
            } => property(node_chain(views), name, Self::ENTITY, *temporal),
            NodeLeaf::Metadata { views, name } => metadata(node_chain(views), name, Self::ENTITY),
            NodeLeaf::IsActive { views } => Arc::new(Scoped {
                view: node_chain(views),
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
        let f = |views: &Vec<ViewOp>| edge_chain(false, views);
        Ok(match self {
            EdgeLeaf::Property {
                views,
                name,
                temporal,
            } => property(f(views), name, Self::ENTITY, *temporal),
            EdgeLeaf::Metadata { views, name } => metadata(f(views), name, Self::ENTITY),
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
        let f = |views: &Vec<ViewOp>| edge_chain(true, views);
        Ok(match self {
            ExplodedEdgeLeaf::Property {
                views,
                name,
                temporal,
            } => property(f(views), name, Self::ENTITY, *temporal),
            ExplodedEdgeLeaf::Metadata { views, name } => metadata(f(views), name, Self::ENTITY),
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
                    Agg::Sum => Arc::new(SumExpr(op)),
                    Agg::Avg => Arc::new(AvgExpr(op)),
                    Agg::Min => Arc::new(MinExpr(op)),
                    Agg::Max => Arc::new(MaxExpr(op)),
                    Agg::First => Arc::new(FirstExpr(op)),
                    Agg::Last => Arc::new(LastExpr(op)),
                    Agg::Len => Arc::new(LenExpr(op)),
                    Agg::Earliest => Arc::new(EarliestExpr(op)),
                    Agg::Latest => Arc::new(LatestExpr(op)),
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
        view_chain(Arc::new(GraphFilter), &self.views).create_view(graph)
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

/// A yes/no expression is a filter on its entity, applied through its tree.
impl<L: super::Leaf> CreateFilter for Expr<L> {
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
        FilterExpr::from(self).create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_edge_filter(graph)
    }
}

/// A view on the graph is a filter: the graph seen through it.
impl CreateFilter for Chain<()> {
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
        FilterExpr::from(self).create_graph_filter(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        FilterExpr::from(self).create_edge_filter(graph)
    }
}
