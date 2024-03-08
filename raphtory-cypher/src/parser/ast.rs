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

#[derive(Debug, Default)]
pub struct Return {
    pub all: bool,
    pub items: Vec<ReturnItem>,
}


#[derive(Debug)]
pub struct ReturnItem {
    pub expr: Expr,
    pub as_name: Option<String>,
}

type Ex = Box<Expr>;

#[derive(Debug)]
pub enum Expr {
    Var {
        var_name: String,
        attr: Option<String>,
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

#[derive(Debug)]
pub enum UnaryOpType {
    Not,
    Neg,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum Literal {
    Bool(bool),
    Str(String),
    Int(i64),
    Float(f64),
}
