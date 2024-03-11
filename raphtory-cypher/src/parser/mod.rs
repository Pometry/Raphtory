pub mod ast;
use std::str::ParseBoolError;

use pest::{
    error::Error,
    iterators::{Pair, Pairs},
    pratt_parser::*,
    Parser,
};
use pest_derive::Parser;
use raphtory::core::Direction;

use self::ast::*;

#[derive(Parser)]
#[grammar = "parser/cypher.pest"]
pub struct CypherParser;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ParseError {
    #[error("Failed to parse {0}")]
    PestErr(#[from] Error<Rule>),
    #[error("Unable to parse query: [{0}] could be unsupported or syntaxt error")]
    Unsupported(String),
    #[error("Unable to parse query: {0}")]
    SyntaxError(String),
    #[error("Unable to parse bool: {0}")]
    ParseBool(#[from] ParseBoolError),
    // parse int error
    #[error("Unable to parse int: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    // parse float error
    #[error("Unable to parse float: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
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
                            where_clause = Some(parse_expr(pair.into_inner())?);
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
    let mut inner = pair.into_inner();
    // skip return
    inner.next();
    for pair in inner {
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
                expr = Some(parse_expr(pair.into_inner())?);
            }
            Rule::Variable => {
                as_name = Some(parse_variable(pair)?);
            }
            _ => {}
        }
    }
    Ok(ReturnItem {
        expr: expr
            .ok_or_else(|| ParseError::SyntaxError("Return item missing expression".to_string()))?,
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
            .op(Op::infix(eq, Left) | Op::infix(ne, Left))
            .op(Op::infix(lt, Left) | Op::infix(lte, Left) | Op::infix(gt, Left) | Op::infix(gte, Left))
            .op(Op::infix(modulo, Left))
            .op(Op::infix(IN, Left))

    };
}

pub fn parse_expr(pairs: Pairs<Rule>) -> Result<Expr, ParseError> {
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
                Rule::modulo => BinOpType::Mod,
                Rule::lt => BinOpType::Lt,
                Rule::lte => BinOpType::Lte,
                Rule::gt => BinOpType::Gt,
                Rule::gte => BinOpType::Gte,
                Rule::ne => BinOpType::Neq,
                Rule::and => BinOpType::And,
                Rule::or => BinOpType::Or,
                Rule::xor => BinOpType::Xor,
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

pub fn parse_primary(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    match pair.as_rule() {
        Rule::Expression => parse_expr(pair.into_inner()),
        Rule::Atom => parse_atom(pair),
        Rule::Literal => parse_literal(pair).map(Expr::Literal),
        Rule::PropertyExpression => parse_prop_expr(pair),
        rule => todo!("parse_primary Unsuported rule {rule:?}"),
    }
}

pub fn parse_prop_expr(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut v_name = None;
    let mut attrs = vec![];
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Atom => {
                if let Expr::Var { var_name, .. } = parse_atom(pair)? {
                    v_name = Some(var_name);
                }
            }
            Rule::PropertyLookup => {
                let attr_name = parse_prop_lookup(pair).ok_or_else(|| {
                    ParseError::SyntaxError("Property expression missing property".to_string())
                })?;
                attrs.push(attr_name);
            }
            _ => {}
        }
    }
    let var_name = v_name.ok_or_else(|| {
        ParseError::SyntaxError("Property expression missing property".to_string())
    })?;
    Ok(Expr::Var { var_name, attrs })
}

fn parse_prop_lookup(pair: Pair<'_, Rule>) -> Option<String> {
    pair.into_inner()
        .filter_map(|pair| match pair.as_rule() {
            Rule::PropertyKeyName => Some(pair.as_str().to_string()), // TODO: deal with escaping ```a```
            _ => None,
        })
        .next()
}

pub fn parse_atom(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    pair.into_inner()
        .next()
        .ok_or_else(|| {
            ParseError::SyntaxError("Expecting at least one token in parse_Atom".to_string())
        })
        .and_then(|pair| match pair.as_rule() {
            Rule::Variable => Ok(Expr::Var {
                var_name: pair.as_str().to_string(),
                attrs: Vec::with_capacity(0),
            }),
            Rule::Literal => Ok(Expr::Literal(parse_literal(pair)?)),
            rule => Err(ParseError::Unsupported(format!(
                "parse_Atom Unsuported rule {rule:?}"
            ))),
        })
}

pub fn parse_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    pair.into_inner()
        .next()
        .ok_or_else(|| {
            ParseError::SyntaxError("Expecting at least one token in parse_literal".to_string())
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
                let f = pair.as_str().parse()?;
                Ok(Literal::Float(f))
            }
            Rule::IntegerLiteral => {
                let i = pair.as_str().parse()?;
                Ok(Literal::Int(i))
            }
            rule => Err(ParseError::Unsupported(format!(
                "parse_number_literal Unsuported rule {rule:?}"
            ))),
        })
}

pub fn parse_string_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let s = pair
        .as_str()
        .strip_prefix("'")
        .ok_or_else(|| ParseError::SyntaxError("String literal missing opening quote".to_string()))?
        .strip_suffix("'")
        .ok_or_else(|| {
            ParseError::SyntaxError("String literal missing closing quote".to_string())
        })?;
    Ok(Literal::Str(s.to_string()))
}

pub fn parse_boolean_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let b = pair.as_str().parse()?;
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

pub fn parse_pattern_part(pair: Pair<Rule>) -> Result<PatternPart, ParseError> {
    let mut var = None;
    let mut first_node = None;
    let mut rel_chain = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                var = Some(parse_variable(pair)?);
            }
            Rule::PatternElement => {
                parse_pattern_element(&mut rel_chain, &mut first_node, pair)?;
            }
            _ => {}
        }
    }
    Ok(PatternPart {
        var,
        node: first_node.unwrap(),
        rel_chain,
    })
}

fn parse_pattern_element(
    rel_chain: &mut Vec<(RelPattern, NodePattern)>,
    first_node: &mut Option<NodePattern>,
    pair: Pair<'_, Rule>,
) -> Result<(), ParseError> {
    let mut cur_elem_chain: Option<RelPattern> = None;
    let mut last_node_pattern = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::NodePattern => {
                if first_node.is_none() {
                    let np = parse_node_pattern(pair)?;
                    *first_node = Some(np);
                } else {
                    if let Some(rel) = cur_elem_chain.take() {
                        if let Some(node) = last_node_pattern.take() {
                            rel_chain.push((rel, node));
                        }
                    }
                    last_node_pattern.replace(parse_node_pattern(pair)?);
                }
            }
            Rule::RelationshipPattern => {
                cur_elem_chain.replace(parse_rel_pattern(pair)?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_rel_detail(pair: Pair<'_, Rule>) -> Result<RelPattern, ParseError> {
    let mut rel_pattern = RelPattern::default();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                let var = parse_variable(pair)?;
                rel_pattern.name = Some(var);
            }
            Rule::RelationshipTypes => {
                let rel_types = pair
                    .into_inner()
                    .map(|pair| pair.as_str().to_string())
                    .collect();
                rel_pattern.rel_types = rel_types;
            }
            Rule::Properties => match pair.into_inner().next() {
                Some(pair) => match pair.as_rule() {
                    Rule::MapLiteral => {
                        let props = parse_map_literal(pair)?;
                        rel_pattern.props = Some(props);
                    }
                    _ => {}
                },
                None => {}
            },
            _ => {}
        }
    }
    Ok(rel_pattern)
}

fn parse_rel_pattern(pair: Pair<'_, Rule>) -> Result<RelPattern, ParseError> {
    let mut rel_pattern = RelPattern::default();

    let mut direction = Direction::BOTH;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::LeftArrowHead => {
                direction = Direction::IN;
            }
            Rule::RightArrowHead => {
                direction = Direction::OUT;
            }
            Rule::RelationshipDetail => {
                rel_pattern = parse_rel_detail(pair)?;
            }
            _ => {}
        }
    }

    rel_pattern.direction = direction;

    Ok(rel_pattern)
}

fn parse_node_pattern(pair: Pair<'_, Rule>) -> Result<NodePattern, ParseError> {
    let mut node = NodePattern::default();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                let var = parse_variable(pair)?;
                node.name = Some(var);
            }
            Rule::NodeLabels => {
                let labels = pair
                    .into_inner()
                    .filter_map(|pair| parse_node_label(pair))
                    .collect();
                node.labels = labels;
            }
            Rule::Properties => match pair.into_inner().next() {
                Some(pair) => match pair.as_rule() {
                    Rule::MapLiteral => {
                        let props = parse_map_literal(pair)?;
                        node.props = Some(props);
                    }
                    _ => {}
                },
                None => {}
            },
            _ => {}
        }
    }

    Ok(node)
}

fn parse_node_label(pair: Pair<'_, Rule>) -> Option<String> {
    match pair.as_rule() {
        Rule::NodeLabel => {
            let mut pairs = pair.into_inner();
            pairs.next().map(|pair| pair.as_str().to_string())
        }
        _ => None,
    }
}

fn parse_map_literal(
    pair: Pair<'_, Rule>,
) -> Result<std::collections::HashMap<String, Expr>, ParseError> {
    let mut map = std::collections::HashMap::new();
    let mut last_key: Option<String> = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::PropertyKeyName => {
                let key = pair.as_str().to_string();
                last_key = Some(key);
            }
            Rule::Expression => {
                let value = parse_expr(pair.into_inner())?;
                if let Some(key) = last_key.take() {
                    map.insert(key, value);
                }
            }

            _ => {}
        }
    }
    Ok(map)
}

fn parse_list_literal(pair: Pair<'_, Rule>) -> Result<Literal, ParseError> {
    let mut list = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Expression => {
                let expr = parse_expr(pair.into_inner())?;
                list.push(expr);
            }
            _ => {}
        }
    }
    Ok(Literal::List(
        list.into_iter()
            .filter_map(|expr| match expr {
                Expr::Literal(lit) => Some(lit),
                _ => None,
            })
            .collect(),
    ))
}

#[cfg(test)]
mod test {

    use crate::parser::parse_expr;

    use super::*;
    use pest::Parser;

    use proptest::prelude::*;

    #[test]
    fn check_parse_return_1() {
        let input = "RETURN n";

        let pairs = CypherParser::parse(Rule::Return, input);
        assert!(pairs.is_ok());

        let return_clause = parse_return(pairs.unwrap().next().unwrap());

        assert_eq!(
            return_clause,
            Ok(Return {
                all: false,
                items: vec![ReturnItem {
                    expr: Expr::Var {
                        var_name: "n".to_string(),
                        attrs: Vec::with_capacity(0),
                    },
                    as_name: None
                }]
            })
        );
    }

    #[test]
    fn check_parse_return_2() {
        let input = "RETURN n AS m";

        let pairs = CypherParser::parse(Rule::Return, input);
        assert!(pairs.is_ok());

        let return_clause = parse_return(pairs.unwrap().next().unwrap());

        assert_eq!(
            return_clause,
            Ok(Return {
                all: false,
                items: vec![ReturnItem {
                    expr: Expr::Var {
                        var_name: "n".to_string(),
                        attrs: vec![]
                    },
                    as_name: Some("m".to_string())
                }]
            })
        );
    }

    #[test]
    fn check_parse_return_3() {
        let input = "ReTuRn n, m as b";

        let pairs = CypherParser::parse(Rule::Return, input);
        assert!(pairs.is_ok());

        let return_clause = parse_return(pairs.unwrap().next().unwrap());

        assert_eq!(
            return_clause,
            Ok(Return {
                all: false,
                items: vec![
                    ReturnItem {
                        expr: Expr::Var {
                            var_name: "n".to_string(),
                            attrs: vec![],
                        },
                        as_name: None
                    },
                    ReturnItem {
                        expr: Expr::Var {
                            var_name: "m".to_string(),
                            attrs: vec![],
                        },
                        as_name: Some("b".to_string())
                    }
                ]
            })
        );
    }

    #[test]
    fn check_property_expr() {
        let input = "a.b.c";
        let pairs = CypherParser::parse(Rule::Expression, input);
        assert!(pairs.is_ok());

        let expr = parse_expr(pairs.unwrap());
        assert_eq!(
            expr,
            Ok(Expr::Var {
                var_name: "a".to_string(),
                attrs: vec!["b".to_string(), "c".to_string()],
            })
        );
    }

    proptest! {
        #[test]
        fn literal_int(input in any::<i64>().prop_filter("only positive", |x| *x > 0)) {
            let n = input;
            let input = format!("{}", n);
            let pairs = CypherParser::parse(Rule::Literal, &input);
            assert!(pairs.is_ok());

            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Int(n)));
        }

        #[test]
        fn literal_float(input in any::<f64>().prop_filter("only positive", |x| *x > 0.0)) {
            let n = input;

            let str_num = format!("{:+e}", n);
            let input = str_num.strip_prefix("+").unwrap();
            let pairs = CypherParser::parse(Rule::Literal, &input);
            assert!(pairs.is_ok());

            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Float(n)));


            let str_num = format!("{:.1000}", n);
            let input = str_num.trim();
            let pairs = CypherParser::parse(Rule::Literal, &input);
            assert!(pairs.is_ok());

            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Float(n)));
        }

        #[test]
        fn literal_string(input in any::<String>().prop_filter("no single qotes", |s| !s.contains("'") && !s.contains("\\")).prop_map(|input| format!("'{}'", input))) {
            let pairs = CypherParser::parse(Rule::Literal, &input);
            assert!(pairs.is_ok());

            let actual_input = input.strip_prefix("'").unwrap().strip_suffix("'").unwrap();
            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Str(actual_input.to_string())));
        }

    }

    #[test]
    fn check_node_pattern_empty() {
        let input = "()";

        let pairs = CypherParser::parse(Rule::NodePattern, input);
        assert!(pairs.is_ok());

        let node = parse_node_pattern(pairs.unwrap().next().unwrap());
        assert_eq!(
            node,
            Ok(NodePattern {
                name: None,
                labels: vec![],
                props: None
            })
        );
    }

    #[test]
    fn check_node_pattern_name() {
        let input = "(n)";

        let pairs = CypherParser::parse(Rule::NodePattern, input);
        assert!(pairs.is_ok());

        let node = parse_node_pattern(pairs.unwrap().next().unwrap());
        assert_eq!(
            node,
            Ok(NodePattern {
                name: Some("n".to_string()),
                labels: vec![],
                props: None
            })
        );
    }

    #[test]
    fn check_node_pattern_label() {
        let input = "(n:Person)";

        let pairs = CypherParser::parse(Rule::NodePattern, input);
        assert!(pairs.is_ok());

        let node = parse_node_pattern(pairs.unwrap().next().unwrap());
        assert_eq!(
            node,
            Ok(NodePattern {
                name: Some("n".to_string()),
                labels: vec!["Person".to_string()],
                props: None
            })
        );
    }

    #[test]
    fn check_node_pattern_label_props() {
        let input = "(n:Person {name: 'John'})";

        let pairs = CypherParser::parse(Rule::NodePattern, input);
        assert!(pairs.is_ok());

        let node = parse_node_pattern(pairs.unwrap().next().unwrap());
        assert_eq!(
            node,
            Ok(NodePattern {
                name: Some("n".to_string()),
                labels: vec!["Person".to_string()],
                props: Some(
                    vec![(
                        "name".to_string(),
                        Expr::Literal(Literal::Str("John".to_string()))
                    )]
                    .into_iter()
                    .collect()
                )
            })
        );
    }

    #[test]
    fn check_edge_pattern_label_props() {
        let input = "-[r:KNOWS {name: 'John'}]->";

        let pairs = CypherParser::parse(Rule::RelationshipPattern, input);
        assert!(pairs.is_ok());

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap());
        assert_eq!(
            rel,
            Ok(RelPattern {
                name: Some("r".to_string()),
                direction: Direction::OUT,
                rel_types: vec!["KNOWS".to_string()],
                props: Some(
                    vec![(
                        "name".to_string(),
                        Expr::Literal(Literal::Str("John".to_string()))
                    )]
                    .into_iter()
                    .collect()
                )
            })
        );
    }

    #[test]
    fn map_literal() {
        let input = "{a: 1, b: 2}";
        let pairs = CypherParser::parse(Rule::MapLiteral, input);
        assert!(pairs.is_ok());

        let map = parse_map_literal(pairs.unwrap().next().unwrap());
        assert_eq!(
            map,
            Ok(vec![
                ("a".to_string(), Expr::Literal(Literal::Int(1))),
                ("b".to_string(), Expr::Literal(Literal::Int(2)))
            ]
            .into_iter()
            .collect())
        );
    }

    #[test]
    fn list_literal() {
        let input = "[1, 2, 3]";
        let pairs = CypherParser::parse(Rule::ListLiteral, input);
        assert!(pairs.is_ok());

        let list = parse_list_literal(pairs.unwrap().next().unwrap());
        assert_eq!(
            list,
            Ok(Literal::List(vec![
                Literal::Int(1),
                Literal::Int(2),
                Literal::Int(3)
            ]))
        );
    }

    #[test]
    fn and_expression() {
        let input = "a.name = 'John' and 1 < a.age";
        let pairs = CypherParser::parse(Rule::Expression, input);
        assert!(pairs.is_ok());
        println!("{:?}", pairs);

        let expr = parse_expr(pairs.unwrap());

        assert_eq!(
            expr,
            Ok(Expr::and(
                Expr::eq(Expr::prop("a", ["name"]), Expr::str("John")),
                Expr::lt(Expr::int(1), Expr::prop("a", ["age"]))
            ))
        );
    }

    #[test]
    fn test_parse_bin_expr() {
        for op in [
            "+", "-", "*", "/", "AND", "OR", "XOR", ">", "<", ">=", "<=", "=", "<>",
        ] {
            let input = format!("1 {} 2", op);
            let pairs = CypherParser::parse(Rule::Expression, &input);
            assert!(pairs.is_ok());
            let pairs = pairs.unwrap();

            let expr = parse_expr(pairs);
            match expr {
                Ok(Expr::BinOp {
                    op:
                        BinOpType::Add
                        | BinOpType::Sub
                        | BinOpType::Mul
                        | BinOpType::Div
                        | BinOpType::Mod
                        | BinOpType::Pow
                        | BinOpType::Eq
                        | BinOpType::Neq
                        | BinOpType::Lt
                        | BinOpType::Lte
                        | BinOpType::Gt
                        | BinOpType::Gte
                        | BinOpType::And
                        | BinOpType::Or
                        | BinOpType::Xor,
                    left,
                    right,
                }) => {
                    assert_eq!(*left, Expr::Literal(Literal::Int(1)));
                    assert_eq!(*right, Expr::Literal(Literal::Int(2)));
                }
                Ok(actual) => panic!("Expected BinOp got {:?} testing for {:?}", actual, op),
                Err(e) => panic!("Error: {:?} testing for {:?}", e, op),
            }
        }
    }

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
