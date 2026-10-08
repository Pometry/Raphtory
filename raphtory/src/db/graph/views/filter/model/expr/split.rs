//! A filter split by the question it answers.
//!
//! A filtered graph answers two questions, which nodes stay and which edges
//! stay, and every filter answers both: a node predicate answers the first
//! directly and the second by "both ends stayed", an edge predicate the
//! reverse. `and`, `or` and `not` combine the direct answers question by
//! question, so `name == "b" | name == "c"` keeps the edge b→c, `not` never
//! flips an answer the filter did not give, and an `or` across kinds is the
//! union of what its legs keep.
//!
//! [`FilterExpr::split`] applies that rule as a rewrite of the tree, with no
//! graph at hand. The result is the view the graph is seen through, one node
//! expression and one edge question: the data the value compiler already
//! takes, so the rule lives here and the compiler stays a compiler.

use super::{EdgeExpr, EdgeLeaf, ExplodedEdgeExpr, Expr, FilterExpr, NodeExpr, ViewOp};
use crate::errors::GraphError;

/// A filter with its legs sorted by the question they answer.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SplitFilter {
    /// The view the graph is seen through before anything else: the view
    /// legs of the top-level `and`, in the order written.
    pub(crate) views: Vec<ViewOp>,
    /// Which nodes stay; `None` leaves every node.
    pub(crate) nodes: Option<NodeExpr>,
    /// Which edges stay, beyond the edges whose ends both stayed; `None`
    /// leaves the question open.
    pub(crate) edges: Option<EdgeQuestion>,
}

/// The edge question's answer. A predicate on edges and one on exploded edges
/// decide different things, an edge or one update of it, and are applied by
/// different filtered graphs, so legs of the two kinds combine as filters
/// where legs of one kind combine as one expression.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EdgeQuestion {
    Edge(EdgeExpr),
    Exploded(ExplodedEdgeExpr),
    And(Vec<EdgeQuestion>),
    Or(Vec<EdgeQuestion>),
}

/// The direct answers one leg gives to the two questions.
struct Answers {
    nodes: Option<NodeExpr>,
    edges: Option<EdgeQuestion>,
}

#[derive(Clone, Copy)]
enum Junction {
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

/// An answer that can take other answers to the same question beside it.
trait Join: Sized {
    fn join(legs: Vec<Self>, how: Junction) -> Self;
}

impl<L> Join for Expr<L> {
    fn join(legs: Vec<Self>, how: Junction) -> Self {
        match how {
            Junction::And => Expr::And(legs),
            Junction::Or => Expr::Or(legs),
        }
    }
}

impl Join for EdgeQuestion {
    fn join(legs: Vec<Self>, how: Junction) -> Self {
        let edges = legs
            .iter()
            .map(|leg| match leg {
                EdgeQuestion::Edge(e) => Some(e.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let exploded = legs
            .iter()
            .map(|leg| match leg {
                EdgeQuestion::Exploded(e) => Some(e.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        match (edges, exploded, how) {
            (Some(edges), _, how) => EdgeQuestion::Edge(Expr::join(edges, how)),
            (_, Some(exploded), how) => EdgeQuestion::Exploded(Expr::join(exploded, how)),
            (None, None, Junction::And) => EdgeQuestion::And(legs),
            (None, None, Junction::Or) => EdgeQuestion::Or(legs),
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
    if negated || !(mixed(|a| a.nodes.is_some()) || mixed(|a| a.edges.is_some())) {
        return any_of(legs, negated);
    }
    let edges = legs
        .into_iter()
        .map(|a| a.edges.or_else(|| a.nodes.map(closed_edges)));
    Answers {
        nodes: None,
        edges: every(edges, Junction::Or),
    }
}

impl EdgeQuestion {
    /// This answer and another, both required.
    pub(crate) fn and(self, other: EdgeQuestion) -> EdgeQuestion {
        Self::join(vec![self, other], Junction::And)
    }
}

/// The edges a node answer keeps: those whose ends it keeps both.
pub(crate) fn closed_edges(nodes: NodeExpr) -> EdgeQuestion {
    EdgeQuestion::Edge(Expr::And(vec![
        Expr::Term(EdgeLeaf::Src(Box::new(nodes.clone()))),
        Expr::Term(EdgeLeaf::Dst(Box::new(nodes))),
    ]))
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

fn view_below_top_level() -> GraphError {
    GraphError::invalid_filter(
        "a view applies to the whole filter: use it alone or as a leg of the top-level `and`, \
         not under `or` or `not`",
    )
}

impl FilterExpr {
    /// This filter sorted by question.
    ///
    /// A view (`View`) applies first: the graph is seen through it and the
    /// other legs run inside it, terms included, the way
    /// `graph.window(..).filter(expr)` does. A view therefore stands alone or
    /// is a leg of the top-level `and` (nested `and`s count as top level);
    /// under `or` or `not` it has no meaning the engine can give it and is
    /// refused.
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

    /// The view ops at the top of the filter, in order, and the predicates
    /// beside them. `and` nests flatten; anything else is a predicate.
    fn top_views(&self) -> Result<(Vec<ViewOp>, Vec<&FilterExpr>), GraphError> {
        fn walk<'a>(
            filter: &'a FilterExpr,
            views: &mut Vec<ViewOp>,
            predicates: &mut Vec<&'a FilterExpr>,
        ) -> Result<(), GraphError> {
            match filter {
                FilterExpr::View(ops) if ops.is_empty() => Err(needs_view()),
                FilterExpr::View(ops) => {
                    views.extend(ops.iter().cloned());
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
    /// A leaf answers its own entity's question; `and`, `or` and `not`
    /// combine their legs' answers question by question.
    fn answers(&self, negated: bool) -> Result<Answers, GraphError> {
        let legs = |items: &[FilterExpr]| -> Result<Vec<Answers>, GraphError> {
            items.iter().map(|item| item.answers(negated)).collect()
        };
        Ok(match self {
            FilterExpr::Node(expr) => Answers {
                nodes: Some(predicate(expr, negated)?),
                edges: None,
            },
            FilterExpr::Edge(expr) => Answers {
                nodes: None,
                edges: Some(EdgeQuestion::Edge(predicate(expr, negated)?)),
            },
            FilterExpr::ExplodedEdge(expr) => Answers {
                nodes: None,
                edges: Some(EdgeQuestion::Exploded(predicate(expr, negated)?)),
            },
            FilterExpr::View(_) => return Err(view_below_top_level()),
            FilterExpr::And(items) if items.is_empty() => return Err(needs_operand("and")),
            FilterExpr::Or(items) if items.is_empty() => return Err(needs_operand("or")),
            FilterExpr::And(items) => all_of(legs(items)?, negated),
            FilterExpr::Or(items) => or_of(legs(items)?, negated),
            FilterExpr::Not(inner) => inner.answers(!negated)?,
        })
    }
}
