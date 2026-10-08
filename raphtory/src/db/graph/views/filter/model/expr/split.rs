//! A filter split by the question it answers.
//!
//! A filtered graph answers two questions, which nodes stay and which edges
//! stay, and every filter answers both: a node predicate answers the first
//! directly and the second by "both ends stayed", an edge predicate the
//! reverse, and a view leg answers both with "exists in the view". `and`,
//! `or` and `not` combine the direct answers question by question, so
//! `name == "b" | name == "c"` keeps the edge b→c, `not` never flips an
//! answer the filter did not give, and an `or` across kinds is the union of
//! what its legs keep.
//!
//! [`FilterExpr::split`] applies that rule as a rewrite of the tree, with no
//! graph at hand. The result is the view legs the graph is seen through, one
//! answer per question: the data the value compiler already takes, so the
//! rule lives here and the compiler stays a compiler.

use super::{EdgeExpr, EdgeLeaf, ExplodedEdgeExpr, Expr, FilterExpr, NodeExpr, ViewOp};
use crate::errors::GraphError;

/// A filter with its legs sorted by the question they answer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SplitFilter {
    /// The view legs of the top-level `and`, in the order written, one list
    /// per leg. A filtered graph is seen through all of them, chained; a
    /// node or edge filter asks that the entity exist in each.
    pub(crate) views: Vec<Vec<ViewOp>>,
    /// Which nodes stay; `None` leaves every node.
    pub(crate) nodes: Option<Question<NodeExpr>>,
    /// Which edges stay, beyond the edges whose ends both stayed; `None`
    /// leaves the question open.
    pub(crate) edges: Option<Question<EdgePredicate>>,
}

/// One question's answer: predicates on the question's entity, the entity's
/// existence in a view, and `and` / `or` between them. Negation sits inside
/// the leaves.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Question<P> {
    Predicate(P),
    /// The entity exists in the view, or does not when `negated`: what a
    /// view leg means on a node or edge collection. A filtered graph cannot
    /// answer this below the top level.
    Exists {
        views: Vec<ViewOp>,
        negated: bool,
    },
    And(Vec<Question<P>>),
    Or(Vec<Question<P>>),
}

/// A predicate on the edge question. One on edges and one on exploded edges
/// decide different things, an edge or one update of it, and are applied by
/// different filtered graphs, so legs of the two kinds combine as filters
/// where legs of one kind combine as one expression.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EdgePredicate {
    Edge(EdgeExpr),
    Exploded(ExplodedEdgeExpr),
}

impl<P: Fold> Question<P> {
    /// This answer and another, both required.
    pub(crate) fn and(self, other: Self) -> Self {
        Self::join(vec![self, other], Junction::And)
    }
}

/// The direct answers one leg gives to the two questions.
struct Answers {
    nodes: Option<Question<NodeExpr>>,
    edges: Option<Question<EdgePredicate>>,
}

#[derive(Clone, Copy)]
pub(crate) enum Junction {
    And,
    Or,
}

impl Junction {
    /// The junction the opposite answers combine with: `not(a and b)` is
    /// `not a or not b`.
    fn flipped(self, negated: bool) -> Self {
        match (self, negated) {
            (Junction::And, true) => Junction::Or,
            (Junction::Or, true) => Junction::And,
            (same, false) => same,
        }
    }
}

/// Predicates that may fold into one expression. `Err` hands the legs back
/// when they cannot, so they combine as filters instead.
pub(crate) trait Fold: Sized {
    fn fold(legs: Vec<Self>, how: Junction) -> Result<Self, Vec<Self>>;
}

impl<L> Fold for Expr<L> {
    fn fold(mut legs: Vec<Self>, how: Junction) -> Result<Self, Vec<Self>> {
        match legs.len() {
            0 => Err(legs),
            1 => Ok(legs.pop().expect("one leg")),
            _ => Ok(match how {
                Junction::And => Expr::And(legs),
                Junction::Or => Expr::Or(legs),
            }),
        }
    }
}

impl Fold for EdgePredicate {
    fn fold(legs: Vec<Self>, how: Junction) -> Result<Self, Vec<Self>> {
        let edges = legs
            .iter()
            .map(|leg| match leg {
                EdgePredicate::Edge(e) => Some(e.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let exploded = legs
            .iter()
            .map(|leg| match leg {
                EdgePredicate::Exploded(e) => Some(e.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        match (edges, exploded) {
            (Some(edges), _) => Expr::fold(edges, how)
                .map(EdgePredicate::Edge)
                .map_err(|_| legs),
            (_, Some(exploded)) => Expr::fold(exploded, how)
                .map(EdgePredicate::Exploded)
                .map_err(|_| legs),
            (None, None) => Err(legs),
        }
    }
}

/// An answer that can take other answers to the same question beside it.
trait Join: Sized {
    fn join(legs: Vec<Self>, how: Junction) -> Self;
}

impl<P: Fold> Join for Question<P> {
    fn join(legs: Vec<Self>, how: Junction) -> Self {
        // predicates of one kind are one expression; everything else combines
        // as filters beside it
        let mut predicates = Vec::new();
        let mut others = Vec::new();
        for leg in legs {
            match leg {
                Question::Predicate(p) => predicates.push(p),
                other => others.push(other),
            }
        }
        match P::fold(predicates, how) {
            Ok(p) => others.push(Question::Predicate(p)),
            Err(ps) => others.extend(ps.into_iter().map(Question::Predicate)),
        }
        match (others.len(), how) {
            (1, _) => others.pop().expect("one leg"),
            (_, Junction::And) => Question::And(others),
            (_, Junction::Or) => Question::Or(others),
        }
    }
}

/// The legs that answered, joined; `None` when none did.
fn joined<T: Join>(mut legs: Vec<T>, how: Junction) -> Option<T> {
    match legs.len() {
        0 => None,
        1 => legs.pop(),
        _ => Some(T::join(legs, how)),
    }
}

/// All legs joined when every one answered; `None` as soon as one left the
/// question open.
fn every<T: Join>(answers: impl Iterator<Item = Option<T>>, how: Junction) -> Option<T> {
    answers
        .collect::<Option<Vec<T>>>()
        .and_then(|legs| joined(legs, how))
}

/// The `and` of answers: a leg that leaves a question open is dropped and the
/// rest combine. Negated, the legs were negated and combine with `or`.
fn all_of(legs: Vec<Answers>, negated: bool) -> Answers {
    let how = Junction::And.flipped(negated);
    let (nodes, edges): (Vec<_>, Vec<_>) = legs.into_iter().map(|a| (a.nodes, a.edges)).unzip();
    Answers {
        nodes: joined(nodes.into_iter().flatten().collect(), how),
        edges: joined(edges.into_iter().flatten().collect(), how),
    }
}

/// The `or` of answers: every leg must answer, or the question stays open.
/// Negated, the legs were negated and combine with `and`.
fn any_of(legs: Vec<Answers>, negated: bool) -> Answers {
    let how = Junction::Or.flipped(negated);
    let (nodes, edges): (Vec<_>, Vec<_>) = legs.into_iter().map(|a| (a.nodes, a.edges)).unzip();
    Answers {
        nodes: every(nodes.into_iter(), how),
        edges: every(edges.into_iter(), how),
    }
}

/// The `or` of legs of different kinds is the union of what each keeps: an
/// edge leg keeps every node, which no union can narrow, so the node question
/// is open; a node leg keeps the edges whose ends it keeps both, so for the
/// edge question each node leg is closed to those edges and the `or` unions
/// them with the edge legs' edges. Legs of one kind are left to `any_of`, so
/// `name == "b" | name == "c"` still keeps the edge b→c through its node
/// answer. Negated, the `or` is an `and` of the opposites and nothing is closed.
fn or_of(legs: Vec<Answers>, negated: bool) -> Answers {
    let mixed =
        |answered: fn(&Answers) -> bool| legs.iter().any(answered) && !legs.iter().all(answered);
    if negated {
        return any_of(legs, true);
    }
    // a leg that leaves the node question open keeps every node
    let nodes = if mixed(|a| a.nodes.is_some()) {
        None
    } else {
        every(legs.iter().map(|a| a.nodes.clone()), Junction::Or)
    };
    // a leg that leaves the edge question open keeps the edges whose ends it keeps
    let edges = if mixed(|a| a.edges.is_some()) {
        every(
            legs.into_iter()
                .map(|a| a.edges.or_else(|| a.nodes.map(closed_edges))),
            Junction::Or,
        )
    } else {
        every(legs.into_iter().map(|a| a.edges), Junction::Or)
    };
    Answers { nodes, edges }
}

/// The edges a node answer keeps: those whose ends it keeps both. A view leg
/// answers the edge question itself, so its node answer closes to that same
/// answer: the edge's existence in the view stands for its ends'.
pub(crate) fn closed_edges(nodes: Question<NodeExpr>) -> Question<EdgePredicate> {
    match nodes {
        Question::Predicate(nodes) => Question::Predicate(EdgePredicate::Edge(Expr::And(vec![
            Expr::Term(EdgeLeaf::Src(Box::new(nodes.clone()))),
            Expr::Term(EdgeLeaf::Dst(Box::new(nodes))),
        ]))),
        Question::Exists { views, negated } => Question::Exists { views, negated },
        Question::And(legs) => Question::And(legs.into_iter().map(closed_edges).collect()),
        Question::Or(legs) => Question::Or(legs.into_iter().map(closed_edges).collect()),
    }
}

/// A yes/no expression as the answer to its entity's question, or the
/// opposite answer when negated. A bare constant is not a filter.
fn predicate<L: Clone>(expr: &Expr<L>, negated: bool) -> Result<Expr<L>, GraphError> {
    if matches!(expr, Expr::Const(_)) {
        return Err(GraphError::invalid_filter("a constant is not a filter"));
    }
    Ok(if negated {
        Expr::Not(Box::new(expr.clone()))
    } else {
        expr.clone()
    })
}

/// An empty list has no meaning either way: `and` of nothing is not
/// "everything", `or` of nothing is not "nothing" the caller asked for.
fn needs_operand(name: &str) -> GraphError {
    GraphError::invalid_filter(format!("`{name}` needs at least one operand"))
}

fn needs_view() -> GraphError {
    GraphError::invalid_filter("a view filter needs at least one view")
}

/// A filtered graph is seen through a view; there is no graph that is seen
/// through "this view or that predicate", nor one seen through "not this
/// view", so on that path a view stands alone or beside the other legs of
/// the top-level `and`.
pub(crate) fn view_below_top_level() -> GraphError {
    GraphError::invalid_filter(
        "a view applies to the whole filter: use it alone or as a leg of the top-level `and`, \
         not under `or` or `not`",
    )
}

impl FilterExpr {
    /// This filter sorted by question.
    ///
    /// What a view leg means depends on what the filter is applied to. A
    /// filtered graph is seen through the view legs of the top-level `and`
    /// first and the other legs run inside it, the way
    /// `graph.window(..).filter(expr)` does; a view anywhere else has no
    /// graph to produce and is refused there. A node or edge filter asks that
    /// the entity exist in the view and runs the other legs on the
    /// collection's own graph, so there a view is a test like any other and
    /// combines with `and`, `or` and `not`.
    pub(crate) fn split(&self) -> Result<SplitFilter, GraphError> {
        let (views, predicates) = self.top_views()?;
        let legs = predicates
            .into_iter()
            .map(|p| p.answers(false))
            .collect::<Result<_, _>>()?;
        let Answers { nodes, edges } = all_of(legs, false);
        Ok(SplitFilter {
            views,
            nodes,
            edges,
        })
    }

    /// The view legs at the top of the filter, in order, and the predicates
    /// beside them. `and` nests flatten; anything else is a predicate.
    fn top_views(&self) -> Result<(Vec<Vec<ViewOp>>, Vec<&FilterExpr>), GraphError> {
        fn walk<'a>(
            filter: &'a FilterExpr,
            views: &mut Vec<Vec<ViewOp>>,
            predicates: &mut Vec<&'a FilterExpr>,
        ) -> Result<(), GraphError> {
            match filter {
                FilterExpr::View(ops) if ops.is_empty() => Err(needs_view()),
                FilterExpr::View(ops) => {
                    views.push(ops.clone());
                    Ok(())
                }
                FilterExpr::And(items) if items.is_empty() => Err(needs_operand("and")),
                FilterExpr::And(items) => items
                    .iter()
                    .try_for_each(|item| walk(item, views, predicates)),
                other => {
                    predicates.push(other);
                    Ok(())
                }
            }
        }
        let mut views = Vec::new();
        let mut predicates = Vec::new();
        walk(self, &mut views, &mut predicates)?;
        Ok((views, predicates))
    }

    /// The direct answers of this leg, or the opposite answers when negated.
    /// A leaf answers its own entity's question, a view leg answers both
    /// with the entity's existence in it; `and`, `or` and `not` combine
    /// their legs' answers question by question.
    fn answers(&self, negated: bool) -> Result<Answers, GraphError> {
        let legs = |items: &[FilterExpr]| -> Result<Vec<Answers>, GraphError> {
            items.iter().map(|item| item.answers(negated)).collect()
        };
        Ok(match self {
            FilterExpr::Node(expr) => Answers {
                nodes: Some(Question::Predicate(predicate(expr, negated)?)),
                edges: None,
            },
            FilterExpr::Edge(expr) => Answers {
                nodes: None,
                edges: Some(Question::Predicate(EdgePredicate::Edge(predicate(
                    expr, negated,
                )?))),
            },
            FilterExpr::ExplodedEdge(expr) => Answers {
                nodes: None,
                edges: Some(Question::Predicate(EdgePredicate::Exploded(predicate(
                    expr, negated,
                )?))),
            },
            FilterExpr::View(ops) if ops.is_empty() => return Err(needs_view()),
            FilterExpr::View(ops) => Answers {
                nodes: Some(Question::Exists {
                    views: ops.clone(),
                    negated,
                }),
                edges: Some(Question::Exists {
                    views: ops.clone(),
                    negated,
                }),
            },
            FilterExpr::And(items) if items.is_empty() => return Err(needs_operand("and")),
            FilterExpr::Or(items) if items.is_empty() => return Err(needs_operand("or")),
            FilterExpr::And(items) => all_of(legs(items)?, negated),
            FilterExpr::Or(items) => or_of(legs(items)?, negated),
            FilterExpr::Not(inner) => inner.answers(!negated)?,
        })
    }
}
