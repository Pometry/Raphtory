use std::collections::HashMap;

use raphtory::core::Direction;

#[derive(Debug, PartialEq)]
pub enum Query {
    SingleQuery(SingleQuery),
}

#[derive(Debug, Default, PartialEq)]
pub struct SingleQuery {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, PartialEq)]
pub enum Clause {
    Match(Match),
    Return(Return),
}

#[derive(Debug, Default, PartialEq)]
pub struct Match {
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Pattern(pub Vec<PatternPart>);

#[derive(Debug, Default, PartialEq)]
pub struct PatternPart {
    pub var: Option<String>,
    pub node: NodePattern,
    pub rel_chain: Vec<(RelPattern, NodePattern)>,
}

#[derive(Debug, Default, PartialEq)]
pub struct NodePattern {
    pub name: Option<String>,
    pub labels: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

impl NodePattern {
    pub fn named(name: &str) -> Self {
        NodePattern {
            name: Some(name.to_string()),
            labels: vec![],
            props: None,
        }
    }

    pub fn named_labelled<S:AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        NodePattern {
            name: Some(name.to_string()),
            labels: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct RelPattern {
    pub name: Option<String>,
    pub direction: Direction,
    pub rel_types: Vec<String>,
    pub props: Option<HashMap<String, Expr>>,
}

impl RelPattern {
    pub fn out(name: &str) -> Self {
        RelPattern {
            name: Some(name.to_string()),
            direction: Direction::OUT,
            rel_types: vec![],
            props: None,
        }
    }

    pub fn into(name: &str) -> Self {
        RelPattern {
            name: Some(name.to_string()),
            direction: Direction::IN,
            rel_types: vec![],
            props: None,
        }
    }

    pub fn undirected(name: &str) -> Self {
        RelPattern {
            name: Some(name.to_string()),
            direction: Direction::BOTH,
            rel_types: vec![],
            props: None,
        }
    }
    
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
