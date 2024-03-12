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

    pub fn return_(all: bool, items: impl IntoIterator<Item = ReturnItem>) -> Self {
        Clause::Return(Return {
            all,
            items: items.into_iter().collect(),
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
    // pub fn node(node: NodePattern) -> Self {
    //     PatternPart {
    //         var: None,
    //         node,
    //         rel_chain: vec![],
    //     }
    // }

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

    pub fn named_labelled<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        NodePattern {
            name: Some(name.to_string()),
            labels: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }
}

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
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

    pub fn into_labels<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        RelPattern {
            name: Some(name.to_string()),
            direction: Direction::IN,
            rel_types: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
            props: None,
        }
    }

    pub fn out_labels<S: AsRef<str>>(name: &str, labels: impl IntoIterator<Item = S>) -> Self {
        RelPattern {
            name: Some(name.to_string()),
            direction: Direction::OUT,
            rel_types: labels.into_iter().map(|s| s.as_ref().to_string()).collect(),
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

#[derive(Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Return {
    pub all: bool,
    pub items: Vec<ReturnItem>,
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

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
    Count(Ex),
    CountAll,
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

    pub fn and(left: Expr, right: Expr) -> Self {
        Self::new(BinOpType::And, left, right)
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

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOpType {
    Not,
    Neg,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Literal {
    Bool(bool),
    Str(String),
    Int(i64),
    Float(f64),
    List(Vec<Literal>),
}
