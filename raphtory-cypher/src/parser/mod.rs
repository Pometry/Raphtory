pub mod ast;
use pest::error::Error;
use pest::iterators::Pair;
use pest::iterators::Pairs;
use pest::pratt_parser::*;
use pest::Parser;
use pest_derive::Parser;

use self::ast::*;

#[derive(Parser)]
#[grammar = "parser/cypher.pest"]
pub struct CypherParser;

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("Failed to parse {0}")]
    PestErr(#[from] Error<Rule>),
    #[error("Unable to parse query: [{0}] could be unsupported or syntaxt error")]
    Unsupported(String),
    #[error("Unable to parse query: {0}")]
    SyntaxError(String),
}

pub fn parse_cypher(input: &str) -> Result<ast::Query, ParseError> {
    let pairs = CypherParser::parse(Rule::Cypher, input)?;

    // rewrite the above with for loops and when meeting SingleQuery use return

    for pair in pairs {
        match pair.as_rule() {
            Rule::Statement => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::Query => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::RegularQuery => {
                                        for pair in pair.into_inner() {
                                            match pair.as_rule() {
                                                Rule::SingleQuery => {
                                                    let s_query = parse_single_query(pair)?;
                                                    return Ok(Query::SingleQuery(s_query));
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Err(ParseError::Unsupported(input.to_string()))
}

pub fn parse_single_query(pair: Pair<Rule>) -> Result<SingleQuery, ParseError> {
    let mut clauses = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::SinglePartQuery => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::ReadingClause => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::Match => {
                                        let match_clause = parse_match(pair)?;
                                        clauses.push(Clause::Match(match_clause));
                                    }
                                    Rule::Return => {
                                        let return_clause = parse_return(pair)?;
                                        clauses.push(Clause::Return(return_clause));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(SingleQuery { clauses })
}

pub fn parse_match(pair: Pair<Rule>) -> Result<Match, ParseError> {
    let mut pattern = Pattern::default();
    let mut where_clause = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Pattern => {
                pattern = parse_pattern(pair)?;
            }
            Rule::Where => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::Expression => {
                            where_clause = Some(parse_expression(pair)?);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Match {
        pattern,
        where_clause,
    })
}

pub fn parse_return(pair: Pair<Rule>) -> Result<Return, ParseError> {
    let mut items = Vec::new();
    let mut all = false;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::ProjectionBody => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::ProjectionItems => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::ProjectionItem => {
                                        let item = parse_return_item(pair)?;
                                        items.push(item);
                                    }
                                    Rule::STAR => {
                                        all = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(Return { all, items })
}

pub fn parse_return_item(pair: Pair<Rule>) -> Result<ReturnItem, ParseError> {
    let mut expr = None;
    let mut as_name = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Expression => {
                expr = Some(parse_expression(pair)?);
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::AS => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::Variable => {
                                        as_name = Some(parse_variable(pair)?);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(ReturnItem {
        expr: expr.unwrap(),
        as_name,
    })
}

pub fn parse_variable(pair: Pair<Rule>) -> Result<String, ParseError> {
    Ok(pair.as_str().to_string())
}

lazy_static::lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use Rule::*;
        use Assoc::*;

        PrattParser::new()
            .op(Op::infix(add, Left) | Op::infix(subtract, Left))
            .op(Op::infix(multiply, Left) | Op::infix(divide, Left))
            .op(Op::infix(pow, Right))
            .op(Op::prefix(not))
            .op(Op::infix(and, Left))
            .op(Op::infix(xor, Left))
            .op(Op::infix(or, Left))

    };
}

pub fn parse_bin_expr(pairs: Pairs<Rule>) -> Result<Expr, ParseError> {
    PRATT_PARSER
        .map_primary(|primary| parse_primary(primary))
        .map_infix(|lhs, op, rhs| {
            let bin_op_type = match op.as_rule() {
                Rule::add => BinOpType::Add,
                Rule::subtract => BinOpType::Sub,
                Rule::multiply => BinOpType::Mul,
                Rule::divide => BinOpType::Div,
                Rule::pow => BinOpType::Pow,
                Rule::eq => BinOpType::Eq,
                Rule::lt => BinOpType::Lt,
                Rule::lte => BinOpType::Lte,
                Rule::gt => BinOpType::Gt,
                Rule::gte => BinOpType::Gte,
                rule => unreachable!("Unknown bin op type {rule:?}"),
            };

            Ok(Expr::BinOp {
                op: bin_op_type,
                left: Box::new(lhs?),
                right: Box::new(rhs?),
            })
        })
        .map_prefix(|op, expr| match op.as_rule() {
            Rule::not => Ok(Expr::UnaryOp {
                op: UnaryOpType::Not,
                expr: Box::new(expr?),
            }),
            Rule::minus => Ok(Expr::UnaryOp {
                op: UnaryOpType::Neg,
                expr: Box::new(expr?),
            }),
            _ => unreachable!("Expected not"),
        })
        .parse(pairs)
}

pub fn parse_expression(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    if let Some(pair) = pair.into_inner().next() {
        match pair.as_rule() {
            Rule::bin_expr => parse_bin_expr(pair.into_inner()),
            rule => todo!("parse_expression Unsuported rule {rule:?}"),
        }
    } else {
        unreachable!("Expression should have a child")
    }
}

pub fn parse_primary(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut pairs = pair.into_inner();
    if let Some(pair) = pairs.next() {
        match pair.as_rule() {
            Rule::Atom => parse_Atom(pair),
            Rule::Expression => parse_expression(pair),
            rule => todo!("parse_primary Unsuported rule {rule:?}"),
        }
    } else {
        unreachable!()
    }
}

pub fn parse_Atom(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut pairs = pair.into_inner();
    if let Some(pair) = pairs.next() {
        match pair.as_rule() {
            Rule::Variable => Ok(Expr::Var {
                var_name: pair.as_str().to_string(),
                attr: None,
            }),
            Rule::Literal => Ok(Expr::Literal(parse_literal(pair)?)),
            _ => todo!(),
        }
    } else {
        unreachable!()
    }
}

pub fn parse_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    pair.into_inner()
        .next()
        .ok_or_else(|| {
            ParseError::SyntaxError(
                "Expecting at least one token in parse_literal".to_string(),
            )
        })
        .and_then(|pair| match pair.as_rule() {
            Rule::NumberLiteral => parse_number_literal(pair),
            Rule::StringLiteral => parse_string_literal(pair),
            Rule::BooleanLiteral => parse_boolean_literal(pair),
            rule => Err(ParseError::Unsupported(format!(
                "parse_literal Unsuported rule {rule:?}"
            ))),
        })
}

pub fn parse_number_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    pair.into_inner()
        .next()
        .ok_or_else(|| {
            ParseError::SyntaxError(
                "Expecting at least one token in parse_number_literal".to_string(),
            )
        })
        .and_then(|pair| match pair.as_rule() {
            Rule::DoubleLiteral => {
                let f = pair.as_str().parse().unwrap();
                Ok(Literal::Float(f))
            }
            Rule::IntegerLiteral => {
                let i = pair.as_str().parse().unwrap();
                Ok(Literal::Int(i))
            }
            rule => Err(ParseError::Unsupported(format!(
                "parse_number_literal Unsuported rule {rule:?}"
            ))),
        })
}

pub fn parse_string_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let s = pair.as_str().to_string();
    Ok(Literal::Str(s))
}

pub fn parse_boolean_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let b = pair.as_str().parse().unwrap();
    Ok(Literal::Bool(b))
}

pub fn parse_pattern(pair: Pair<Rule>) -> Result<Pattern, ParseError> {
    let mut parts = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::PatternPart => {
                parts.push(parse_pattern_part(pair)?);
            }
            _ => {}
        }
    }
    Ok(Pattern(parts))
}

#[cfg(test)]
mod test {

    use super::*;
    use pest::Parser;

    #[test]
    fn match_1() {
        let input = "MATCH (n) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_2() {
        let input = "MATCH (n:A) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_3() {
        let input = "MATCH (a:A:B) RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_4() {
        let input = "MATCH (n {name: 'bar'}) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_5() {
        let input = "MATCH (n), (m) RETURN n.num AS n, m.num AS m";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_6() {
        let input = "MATCH (n $param) RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_7() {
        let input = "MATCH ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
    #[test]
    fn match_8() {
        let input = "MATCH ()-[r]->() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_9() {
        let input = "MATCH ()<-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_10() {
        let input = "MATCH (), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_11() {
        let input = "MATCH ()-[r]-(), () RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_12() {
        let input = "MATCH ()-[]-(), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_13() {
        let input = "MATCH ()-[]-()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_14() {
        let input = "MATCH ()-[]-()-[]-(), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_15() {
        let input = "MATCH ()-[]-()-[]-(), ()-[r]-(), () RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_16() {
        let input = "MATCH ()-[]-()-[]-(), (), ()-[r]-() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_17() {
        let input = "MATCH (x), (a)-[q]-(b), (s), (s)-[r]->(t)<-[]-(b) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
    #[test]
    fn match_18() {
        let input = "MATCH (:A)-[r]->(:B) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_19() {
        let input = "MATCH ()-[r]-() RETURN type(r) AS r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_20() {
        let input = "MATCH (node)-[r:KNOWS {name: 'monkey'}]->(a) RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_21() {
        let input = "MATCH (n)-[r:KNOWS|HATES]->(x) RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_22() {
        let input = "MATCH (a1)-[r:T]->() WITH r, a1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_23() {
        let input = "MATCH ()-[r:FOO $param]->() RETURN r";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    #[ignore = "pest parser fails this one, but it's a valid cypher query"]
    fn match_24() {
        let input = "MATCH (a)-[:ADMIN]-(b) WHERE a:A";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_25() {
        let input = "MATCH (n) WHERE n.name = 'Bar' RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_26() {
        let input = "MATCH (n:Person)-->() WHERE n.name = 'Bob' RETURN n";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_27() {
        let input = "MATCH ()-[rel:X]-(a) WHERE a.name = 'Andres' RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }

    #[test]
    fn match_28() {
        let input = "MATCH (advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a) WHERE advertiser.id = $1 AND a.id = $2 AND red.time > 12 AND out.name = 'product1' RETURN out.name";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok())
    }
}
