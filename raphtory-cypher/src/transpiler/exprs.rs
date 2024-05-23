use sqlparser::ast::{self as sql_ast};

use crate::parser::ast::{Clause, Query, Return};

pub fn parse_limit(query: &Query) -> Option<sql_ast::Expr> {
    query.clauses().iter().find_map(|clause| match clause {
        Clause::Return(Return {
            limit: Some(limit), ..
        }) => Some(sql_ast::Expr::Value(sql_ast::Value::Number(
            limit.to_string(),
            true,
        ))),
        _ => None,
    })
}
