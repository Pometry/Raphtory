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
    builder::Chain,
    split::{closed_edges, view_below_top_level},
    Agg, EdgeLeaf, EdgePredicate, ExplodedEdgeLeaf, Expr, Field, FilterExpr, NodeExpr, NodeLeaf,
    Question, SplitFilter, ViewOp,
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
            view::internal::{DynGraphArc, GraphView, IntoDynGraphArc},
        },
        graph::views::filter::{
            and_filtered_graph::AndFilteredGraph,
            model::{
                after_bounds, at_bounds, before_bounds,
                edge_expr::ops::{AndEdgeOp, EdgeExistsOp, NotEdgeOp, OrEdgeOp},
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
                layered_filter::{
                    DefaultLayer, ExcludeLayers, ExcludeValidLayers, Layered, ValidLayers,
                },
                node_expr::{
                    AllExpr, AndExpr, AnyExpr, AvgExpr, BinaryCmpExpr, DegreeExpr, DynCreateOp,
                    EarliestExpr, FirstExpr, LastExpr, LatestExpr, LenExpr, MaxExpr, MinExpr,
                    NotExpr, OrExpr, Predicate, PropValueSetExpr, Scoped, StringExpr, SumExpr,
                    UnaryExpr,
                },
                node_filter::NodeFilter,
                snapshot_filter::{SnapshotAt, SnapshotLatest},
                subgraph_filter::{ExcludeNodes, Subgraph, SubgraphNodeTypes, Valid},
                windowed_filter::{ShrinkEnd, ShrinkStart, Windowed},
                CreateView, DynCreateView, EntityMarker, MetadataExpr, PropertyExpr,
            },
            or_filtered_graph::OrFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::storage::timeindex::EventTime;
use std::{fmt::Debug, sync::Arc};

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
            Expr::Const(_) | Expr::Opaque(_) => {}
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
        ViewOp::ValidLayers(names) => Arc::new(ValidLayers::new(names.clone(), chain)),
        ViewOp::ExcludeValidLayers(names) => {
            Arc::new(ExcludeValidLayers::new(names.clone(), chain))
        }
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
            // A field has no time axis and does not depend on layers or on the
            // node set, so the views written on it are kept for display and
            // ignored here.
            NodeLeaf::Field { field, .. } => match field {
                Field::Id => Arc::new(Id),
                Field::Name => Arc::new(Name),
                Field::NodeType => Arc::new(Type),
            },
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
            // Whether an edge is a self loop does not depend on any view.
            EdgeLeaf::IsSelfLoop { .. } => Arc::new(IsSelfLoopEdge),
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
            ExplodedEdgeLeaf::IsSelfLoop { .. } => Arc::new(IsSelfLoopEdge),
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
            Expr::Opaque(filter) => filter.0.clone(),
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

impl SplitFilter {
    /// The graph seen through every view leg in turn, erased so every part
    /// of the filter builds over the one `Arc`.
    fn viewed<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        if self.views.is_empty() {
            Ok(graph.into_dyn_graph_arc())
        } else {
            through(&self.views.concat(), graph.into_dyn_graph_arc())
        }
    }

    /// The graph seen through each view leg on its own, for the existence
    /// tests a node or edge filter asks.
    fn each_view<'graph>(
        &self,
        graph: &DynGraphArc<'graph>,
    ) -> Result<Vec<DynGraphArc<'graph>>, GraphError> {
        self.views
            .iter()
            .map(|leg| through(leg, graph.clone()))
            .collect()
    }
}

/// A yes/no node expression as the node filter it is.
fn node_filter<'graph>(
    expr: &NodeExpr,
    graph: DynGraphArc<'graph>,
) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
    Predicate::new(expr.compile_value()?).create_node_filter(graph)
}

/// The graph seen through one view leg.
fn through<'graph>(
    views: &[ViewOp],
    graph: DynGraphArc<'graph>,
) -> Result<DynGraphArc<'graph>, GraphError> {
    view_chain(Arc::new(GraphFilter), views).create_view(graph)
}

/// The legs built one by one over the same graph and joined pairwise, left
/// to right.
fn fold<'graph, Q, T>(
    legs: &[Q],
    graph: &DynGraphArc<'graph>,
    build: impl Fn(&Q, DynGraphArc<'graph>) -> Result<T, GraphError>,
    join: impl Fn(T, T) -> T,
) -> Result<T, GraphError> {
    let Some((first, rest)) = legs.split_first() else {
        return Err(GraphError::invalid_filter(
            "an answer needs at least one leg",
        ));
    };
    rest.iter()
        .try_fold(build(first, graph.clone())?, |acc, leg| {
            Ok(join(acc, build(leg, graph.clone())?))
        })
}

/// A predicate of one question as the filtered graph it is.
pub(crate) trait GraphPredicate {
    fn create_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError>;
}

impl GraphPredicate for NodeExpr {
    fn create_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        Predicate::new(self.compile_value()?).create_graph_filter(graph)
    }
}

impl GraphPredicate for EdgePredicate {
    fn create_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        match self {
            EdgePredicate::Edge(e) => Predicate::new(e.compile_value()?).create_graph_filter(graph),
            EdgePredicate::Exploded(e) => {
                Predicate::new(e.compile_value()?).create_graph_filter(graph)
            }
        }
    }
}

impl<P: GraphPredicate> Question<P> {
    /// The graph that keeps what this answer keeps. Existence in a view has
    /// no graph below the top level.
    fn create_graph_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynGraphArc<'graph>, GraphError> {
        match self {
            Question::Predicate(p) => p.create_graph_filter(graph),
            Question::Exists { .. } => Err(view_below_top_level()),
            Question::And(legs) => fold(legs, &graph, Self::create_graph_filter, |l, r| {
                AndFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc()
            }),
            Question::Or(legs) => fold(legs, &graph, Self::create_graph_filter, |l, r| {
                OrFilteredGraph::new(graph.clone(), l, r).into_dyn_graph_arc()
            }),
        }
    }
}

impl Question<NodeExpr> {
    /// The per-node test for the nodes this answer keeps.
    fn create_node_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        match self {
            Question::Predicate(nodes) => node_filter(nodes, graph),
            Question::Exists { views, negated } => {
                let exists = NodeExistsOp::new(through(views, graph)?);
                Ok(if *negated {
                    Arc::new(exists.not())
                } else {
                    Arc::new(exists)
                })
            }
            Question::And(legs) => fold(legs, &graph, Self::create_node_filter, |l, r| {
                Arc::new(l.and(r))
            }),
            Question::Or(legs) => fold(legs, &graph, Self::create_node_filter, |l, r| {
                Arc::new(l.or(r))
            }),
        }
    }
}

impl Question<EdgePredicate> {
    /// The per-edge test for the edges this answer keeps.
    fn create_edge_filter<'graph>(
        &self,
        graph: DynGraphArc<'graph>,
    ) -> Result<DynEdgeFilter<'graph>, GraphError> {
        match self {
            Question::Predicate(EdgePredicate::Edge(e)) => {
                Predicate::new(e.compile_value()?).create_edge_filter(graph)
            }
            Question::Predicate(EdgePredicate::Exploded(e)) => {
                Predicate::new(e.compile_value()?).create_edge_filter(graph)
            }
            Question::Exists { views, negated } => {
                let exists = EdgeExistsOp::new(through(views, graph)?);
                Ok(if *negated {
                    Arc::new(NotEdgeOp(exists))
                } else {
                    Arc::new(exists)
                })
            }
            Question::And(legs) => fold(legs, &graph, Self::create_edge_filter, |l, r| {
                Arc::new(AndEdgeOp { left: l, right: r })
            }),
            Question::Or(legs) => fold(legs, &graph, Self::create_edge_filter, |l, r| {
                Arc::new(OrEdgeOp { left: l, right: r })
            }),
        }
    }
}

/// The filter a split filter is: the graph seen through its view legs, then
/// its node answer and its edge answer side by side, one of them alone, or
/// the viewed graph when nothing constrains either. As a per-node or per-edge
/// test it asks that the entity exist in each view leg and pass the answers
/// on the collection's own graph: there a view leg is a test like any other.
impl CreateFilter for SplitFilter {
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
        let graph = self.viewed(graph)?;
        let nodes = self
            .nodes
            .map(|nodes| nodes.create_graph_filter(graph.clone()))
            .transpose()?;
        let edges = self
            .edges
            .map(|edges| edges.create_graph_filter(graph.clone()))
            .transpose()?;
        Ok(match (nodes, edges) {
            (Some(nodes), Some(edges)) => {
                AndFilteredGraph::new(graph, nodes, edges).into_dyn_graph_arc()
            }
            (Some(answer), None) | (None, Some(answer)) => answer,
            (None, None) => graph,
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let graph = graph.into_dyn_graph_arc();
        let views = self.each_view(&graph)?;
        let test: Arc<dyn NodeOp<Output = bool> + 'graph> = match &self.nodes {
            Some(nodes) => nodes.create_node_filter(graph.clone())?,
            None if views.is_empty() => return Ok(Arc::new(NodeExistsOp::new(graph))),
            None => Arc::new(NodeExistsOp::new(graph)),
        };
        Ok(views.into_iter().rev().fold(test, |test, view| {
            Arc::new(NodeExistsOp::new(view).and(test))
        }))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        let graph = graph.into_dyn_graph_arc();
        let views = self.each_view(&graph)?;
        let answer = match (self.nodes.map(closed_edges), self.edges) {
            (Some(nodes), Some(edges)) => Some(nodes.and(edges)),
            (Some(answer), None) | (None, Some(answer)) => Some(answer),
            (None, None) => None,
        };
        let test: DynEdgeFilter<'graph> = match answer {
            Some(answer) => answer.create_edge_filter(graph.clone())?,
            None if views.is_empty() => return Ok(Arc::new(EdgeExistsOp::new(graph))),
            None => Arc::new(EdgeExistsOp::new(graph)),
        };
        Ok(views.into_iter().rev().fold(test, |test, view| {
            Arc::new(AndEdgeOp {
                left: EdgeExistsOp::new(view),
                right: test,
            })
        }))
    }
}

/// A tree is a filter in its own right: applying it splits it by question
/// first.
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
        self.split()?.create_graph_filter(graph)
    }

    /// An edge test says nothing about which nodes belong in a node
    /// collection, so a filter that tests edges anywhere is refused here,
    /// whatever it splits into.
    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if self.tests_edges() {
            return Err(GraphError::NotNodeFilter);
        }
        self.split()?.create_node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        self.split()?.create_edge_filter(graph)
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
