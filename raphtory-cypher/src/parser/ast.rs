use std::collections::HashMap;

use raphtory::core::Direction;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Query {
    SingleQuery(SingleQuery),
}

impl Query {
    pub fn single(clauses: impl IntoIterator<Item = Clause>) -> Self {
        Query::SingleQuery(SingleQuery {
            clauses: clauses.into_iter().collect(),
        })
    }

    pub fn clauses(&self) -> &[Clause] {
        match self {
            Query::SingleQuery(q) => &q.clauses,
        }
    }

    pub fn clauses_mut(&mut self) -> &mut [Clause] {
        match self {
            Query::SingleQuery(q) => &mut q.clauses,
        }
    }

    pub fn rel_patterns(&self) -> impl Iterator<Item = &RelPattern> + '_ {
        self.clauses()
            .iter()
            .filter_map(|clause| match clause {
                Clause::Match(m) => {
                    let iter =
                        m.pattern.0.iter().flat_map(|part: &PatternPart| {
                            part.rel_chain.iter().map(|(rel, _)| rel)
                        });
                    Some(iter)
                }
                _ => None,
            })
            .flatten()
    }

    pub fn node_patterns(&self) -> impl Iterator<Item = &NodePattern> + '_ {
        self.clauses()
            .iter()
            .filter_map(|clause| match clause {
                Clause::Match(m) => {
                    let iter = m.pattern.0.iter().map(|part: &PatternPart| {
                        std::iter::once(&part.node)
                            .chain(part.rel_chain.iter().map(|(_, node)| node))
                    });
                    Some(iter)
                }
                _ => None,
            })
            .flatten()
            .flatten()
    }

    pub fn node_patterns_mut(&mut self) -> impl Iterator<Item = &mut NodePattern> + '_ {
        self.clauses_mut()
            .iter_mut()
            .filter_map(|clause| match clause {
                Clause::Match(m) => {
                    let iter = m.pattern.0.iter_mut().map(|part: &mut PatternPart| {
                        std::iter::once(&mut part.node)
                            .chain(part.rel_chain.iter_mut().map(|(_, node)| node))
                    });
                    Some(iter)
                }
                _ => None,
            })
            .flatten()
            .flatten()
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SingleQuery {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Clause {
    Match(Match),
    Return(Return),
}

impl Clause {
    pub fn match_(pattern: Pattern, filter: Option<Expr>) -> Self {
        Clause::Match(Match {
            pattern,
            where_clause: filter,
        })
    }

    pub fn return_(
        all: bool,
        order_by: Option<OrderBy>,
        items: impl IntoIterator<Item = ReturnItem>,
    ) -> Self {
        Clause::Return(Return {
            all,
            order_by,
            items: items.into_iter().collect(),
            limit: None,
        })
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Match {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pattern(pub Vec<PatternPart>);

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PatternPart {
    pub var: Option<String>,
    pub node: NodePattern,
    pub rel_chain: Vec<(RelPattern, NodePattern)>,
}

impl PatternPart {
    pub fn named_path(
        name: &str,
        start: NodePattern,
        rel_chain: impl IntoIterator<Item = (RelPattern, NodePattern)>,
    ) -> Self {
        PatternPart {
            var: Some(name.to_string()),
            node: start,
            rel_chain: rel_chain.into_iter().collect(),
        }
    }

    pub fn path(
        start: NodePattern,
        rel_chain: impl IntoIterator<Item = (RelPattern, NodePattern)>,
    ) -> Self {
        PatternPart {
            var: None,
            node: start,
            rel_chain: rel_chain.into_iter().collect(),
        }
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodePattern {
    pub name: String,
    pub labels: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

impl NodePattern {
    pub fn named(name: &str) -> Self {
        NodePattern {
            name: name.to_string(),
            labels: vec![],
            props: None,
        }
    }

    pub fn named_labelled<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        NodePattern {
            name: name.to_string(),
            labels: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelPattern {
    pub name: String,
    pub direction: Direction,
    pub rel_types: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

impl RelPattern {
    pub fn out(name: &str) -> Self {
        RelPattern {
            name: name.to_string(),
            direction: Direction::OUT,
            rel_types: vec![],
            props: None,
        }
    }

    pub fn into(name: &str) -> Self {
        RelPattern {
            name: name.to_string(),
            direction: Direction::IN,
            rel_types: vec![],
            props: None,
        }
    }

    pub fn into_labels<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        RelPattern {
            name: name.to_string(),
            direction: Direction::IN,
            rel_types: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }

    pub fn out_labels<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        RelPattern {
            name: name.to_string(),
            direction: Direction::OUT,
            rel_types: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }

    pub fn undirected(name: &str) -> Self {
        RelPattern {
            name: name.to_string(),
            direction: Direction::BOTH,
            rel_types: vec![],
            props: None,
        }
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderBy {
    pub exprs: Vec<(Expr, Option<bool>)>,
}

impl OrderBy {
    pub fn new(exprs: impl IntoIterator<Item = (Expr, Option<bool>)>) -> Self {
        OrderBy {
            exprs: exprs.into_iter().collect(),
        }
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Return {
    pub all: bool,
    pub items: Vec<ReturnItem>,
    pub limit: Option<usize>,
    pub order_by: Option<OrderBy>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReturnItem {
    pub expr: Expr,
    pub as_name: Option<String>,
}

impl ReturnItem {
    pub fn new(expr: Expr, as_name: Option<&str>) -> Self {
        ReturnItem {
            expr,
            as_name: as_name.map(|s| s.to_string()),
        }
    }
}

type Ex = Box<Expr>;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Expr {
    Var {
        var_name: String,
        attrs: Vec<String>,
    },
    Literal(Literal),
    BinOp {
        op: BinOpType,
        left: Ex,
        right: Ex,
    },
    UnaryOp {
        op: UnaryOpType,
        expr: Ex,
    },
    FunctionInvocation {
        name: String,
        distinct: bool,
        args: Vec<Expr>,
    },
    CountAll,
    Nested(Ex),
}

impl Expr {
    pub fn binds(&self) -> Vec<String> {
        match self {
            Expr::Var { var_name, .. } => {
                vec![var_name.clone()]
            }
            Expr::Literal(_) => vec![],
            Expr::BinOp { left, right, .. } => {
                let mut res = left.binds();
                res.extend(right.binds());
                res
            }
            Expr::UnaryOp { expr, .. } => expr.binds(),
            Expr::FunctionInvocation { args, .. } => args.iter().flat_map(|e| e.binds()).collect(),
            Expr::CountAll => vec![],
            Expr::Nested(expr) => expr.binds(),
        }
    }

    pub fn var<S: AsRef<str>>(var: &str, attrs: impl IntoIterator<Item = S>) -> Self {
        Expr::Var {
            var_name: var.to_string(),
            attrs: attrs.into_iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }

    pub fn new(tpe: BinOpType, left: Expr, right: Expr) -> Self {
        Expr::BinOp {
            op: tpe,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn contains(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Contains, left, right)
    }

    pub fn starts_with(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::StartsWith, left, right)
    }

    pub fn ends_with(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::EndsWith, left, right)
    }

    pub fn not(expr: Expr) -> Self {
        Expr::UnaryOp {
            op: UnaryOpType::Not,
            expr: Box::new(expr),
        }
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Eq, left, right)
    }

    pub fn neq(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Neq, left, right)
    }

    pub fn lt(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Lt, left, right)
    }

    pub fn lte(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Lte, left, right)
    }

    pub fn gt(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Gt, left, right)
    }

    pub fn gte(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Gte, left, right)
    }

    pub fn nested(expr: Expr) -> Self {
        Expr::Nested(Box::new(expr))
    }

    pub fn and(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::And, left, right)
    }

    pub fn or(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Or, left, right)
    }

    pub fn sub(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Sub, left, right)
    }

    pub fn str(s: &str) -> Self {
        Expr::Literal(Literal::Str(s.to_string()))
    }

    pub fn int(i: i64) -> Self {
        Expr::Literal(Literal::Int(i))
    }

    pub fn is_null(prop: Expr) -> Self {
        Expr::eq(prop, Expr::Literal(Literal::Null))
    }

    pub fn is_not_null(prop: Expr) -> Self {
        Expr::neq(prop, Expr::Literal(Literal::Null))
    }

    pub fn in_(prop: Expr, list: impl IntoIterator<Item = Literal>) -> Self {
        Self::new(
            BinOpType::In,
            prop,
            Expr::Literal(Literal::List(list.into_iter().collect())),
        )
    }

    pub fn prop<S: AsRef<str>>(var: &str, args: impl IntoIterator<Item = S>) -> Self {
        Expr::Var {
            var_name: var.to_string(),
            attrs: args.into_iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }

    pub fn prop_named(var: &str) -> Self {
        Expr::Var {
            var_name: var.to_string(),
            attrs: vec![],
        }
    }

    pub fn count_all() -> Self {
        Expr::CountAll
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOpType {
    Not,
    Neg,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BinOpType {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    In,
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Literal {
    Null,
    Bool(bool),
    Str(String),
    Int(i64),
    Float(f64),
    List(Vec<Literal>),
}
