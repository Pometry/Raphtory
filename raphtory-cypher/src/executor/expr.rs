type Ex = Box<Expr>;

#[derive(Debug, Clone)]
pub enum Expr {
    Str(String),
    Int(i64),
    Float(f64),
    Var { rel: usize, col: usize },
    Eq(Ex, Ex),
    And(Ex, Ex),
    Or(Ex, Ex),
    Gt(Ex, Ex),
    Lt(Ex, Ex),
    Gte(Ex, Ex),
    Lte(Ex, Ex),
}
