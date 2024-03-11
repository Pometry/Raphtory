use std::collections::HashMap;

use raphtory::core::Direction;

#[derive(Debug)]
pub enum Query {
    SingleQuery(SingleQuery),
}

#[derive(Debug, Default)]
pub struct SingleQuery {
    pub clauses: Vec<Clause>,
}

#[derive(Debug)]
pub enum Clause {
    Match(Match),
    Return(Return),
}

#[derive(Debug, Default)]
pub struct Match {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Default)]
pub struct Pattern(pub Vec<PatternPart>);

#[derive(Debug, Default)]
pub struct PatternPart {
    pub var: Option<String>,
    pub node: NodePattern,
    pub rel_chain: Vec<(RelPattern, NodePattern)>,
}

#[derive(Debug, Default)]
pub struct NodePattern {
    pub name: Option<String>,
    pub label: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

#[derive(Debug, Default)]
pub struct RelPattern {
    pub name: Option<String>,
    pub direction: Direction,
    pub rel_types: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Return {
    pub all: bool,
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub as_name: Option<String>,
}

type Ex = Box<Expr>;

#[derive(Debug, PartialEq)]
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
}

impl Expr {
    pub fn new(tpe: BinOpType, left: Expr, right: Expr) -> Self {
        Expr::BinOp {
            op: tpe,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn eq(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Eq, left, right)
    }

    pub fn lt(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::Lt, left, right)
    }

    pub fn and(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::And, left, right)
    }

    pub fn str(s: &str) -> Self {
        Expr::Literal(Literal::Str(s.to_string()))
    }

    pub fn int(i: i64) -> Self {
        Expr::Literal(Literal::Int(i))
    }

    pub fn prop<S:AsRef<str>>(var: &str, args: impl IntoIterator<Item = S>) -> Self {
        Expr::Var {
            var_name: var.to_string(),
            attrs: args.into_iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum UnaryOpType {
    Not,
    Neg,
}

#[derive(Debug, PartialEq)]
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Bool(bool),
    Str(String),
    Int(i64),
    Float(f64),
    List(Vec<Literal>),
}
