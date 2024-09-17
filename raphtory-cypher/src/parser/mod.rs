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

fn unsupported<B>(msg: &str, rule: &Rule) -> Result<B, ParseError> {
    Err(ParseError::Unsupported(format!(
        "{msg} Unsupported rule {rule:?}"
    )))
}

pub fn parse_cypher(input: &str) -> Result<ast::Query, ParseError> {
    let mut pairs = CypherParser::parse(Rule::Cypher, input)?;

    let pair = pairs
        .next()
        .ok_or_else(|| ParseError::SyntaxError("Empty input".to_string()))?;

    // rewrite the above with for loops and when meeting SingleQuery use return

    for pair in pair.into_inner() {
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
            rule => return unsupported("parse_cypher", &rule),
        }
    }

    Err(ParseError::Unsupported(input.to_string()))
}

pub fn parse_single_query(pair: Pair<Rule>) -> Result<SingleQuery, ParseError> {
    let mut clauses = Vec::new();
    let mut un_named_counter = 0;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::SinglePartQuery => {
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::ReadingClause => {
                            for pair in pair.into_inner() {
                                match pair.as_rule() {
                                    Rule::Match => {
                                        let match_clause =
                                            parse_match(pair, &mut un_named_counter)?;
                                        clauses.push(Clause::Match(match_clause));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Rule::Return => {
                            let return_clause = parse_return(pair)?;
                            clauses.push(Clause::Return(return_clause));
                        }
                        rule => return unsupported("parse_single_query 2", &rule),
                    }
                }
            }
            rule => return unsupported("parse_single_query 1", &rule),
        }
    }
    Ok(SingleQuery { clauses })
}

pub fn parse_match(pair: Pair<Rule>, un_named_counter: &mut usize) -> Result<Match, ParseError> {
    let mut pattern = Pattern::default();
    let mut where_clause = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::MATCH => {}
            Rule::Pattern => {
                pattern = parse_pattern(pair, un_named_counter)?;
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
            rule => return unsupported("parse_match", &rule),
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
    let mut limit = None;
    let mut order_by = None;
    // skip return
    inner.next();
    for pair in inner {
        match pair.as_rule() {
            Rule::RETURN => {}
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
                        Rule::Limit => {
                            let maybe_limit = pair
                                .into_inner()
                                .nth(1)
                                .map(|pair| parse_expr(pair.into_inner()));
                            if let Some(Ok(Expr::Literal(Literal::Int(n)))) = maybe_limit {
                                limit = Some(n as usize);
                            } else {
                                return Err(ParseError::SyntaxError(
                                    "Limit must be an integer".to_string(),
                                ));
                            }
                        }
                        Rule::Order => {
                            order_by = parse_order(pair)?;
                        }
                        rule => return unsupported("parse_return 2", &rule),
                    }
                }
            }
            rule => return unsupported("parse_return 1", &rule),
        }
    }
    Ok(Return {
        all,
        order_by,
        items,
        limit,
    })
}

fn parse_order(pairs: Pair<Rule>) -> Result<Option<OrderBy>, ParseError> {
    let mut exprs = vec![];
    let mut ord = vec![];
    for pair in pairs.into_inner() {
        match pair.as_rule() {
            Rule::SortItem => {
                let mut order: Option<bool> = None;
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::Expression => {
                            exprs.push(parse_expr(pair.into_inner())?);
                        }
                        Rule::DESC | Rule::DESCENDING => {
                            order = Some(false);
                        }
                        Rule::ASC | Rule::ASCENDING => {
                            order = Some(true);
                        }
                        _ => {}
                    }
                }
                ord.push(order)
            }
            _ => {}
        }
    }

    Ok(Some(OrderBy::new(exprs.into_iter().zip(ord))))
}

pub fn parse_return_item(pair: Pair<Rule>) -> Result<ReturnItem, ParseError> {
    let mut expr = None;
    let mut as_name = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::AS => {}
            Rule::Expression => {
                expr = Some(parse_expr(pair.into_inner())?);
            }
            Rule::Variable => {
                as_name = Some(parse_variable(pair)?);
            }
            rule => return unsupported("parse_return_item", &rule),
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
            .op(Op::prefix(not))
            .op(Op::postfix(is_null))
            .op(Op::infix(and, Left))
            .op(Op::infix(xor, Left))
            .op(Op::infix(or, Left))
            .op(Op::infix(eq, Left) | Op::infix(ne, Left))
            .op(Op::infix(lt, Left) | Op::infix(lte, Left) | Op::infix(gt, Left) | Op::infix(gte, Left))
            .op(Op::infix(add, Left) | Op::infix(subtract, Left))
            .op(Op::infix(multiply, Left) | Op::infix(divide, Left))
            .op(Op::infix(pow, Right))
            .op(Op::infix(modulo, Left))
            .op(Op::infix(IN, Left) | Op::infix(contains, Left) | Op::infix(starts_with, Left) | Op::infix(ends_with, Left))
            .op(Op::infix(IS, Right))

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
                Rule::IN => BinOpType::In,
                Rule::contains => BinOpType::Contains,
                Rule::starts_with => BinOpType::StartsWith,
                Rule::ends_with => BinOpType::EndsWith,
                rule => return unsupported("parse_expr", &rule),
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
            rule => unsupported("parse_expr", &rule),
        })
        .map_postfix(|expr, op| match op.as_rule() {
            Rule::is_null => {
                if let Some(Rule::NOT) = op.into_inner().next().map(|op| op.as_rule()) {
                    Ok(Expr::is_not_null(expr?))
                } else {
                    Ok(Expr::is_null(expr?))
                }
            }
            rule => unsupported("parse_expr", &rule),
        })
        .parse(pairs)
}

pub fn parse_primary(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    match pair.as_rule() {
        Rule::Expression => parse_expr(pair.into_inner()),
        Rule::Atom => parse_atom(pair),
        Rule::PropertyExpression => parse_prop_expr(pair),
        rule => unsupported("parse_primary", &rule),
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
            rule => return unsupported("parse_prop_expr", &rule),
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
            Rule::COUNT => Ok(Expr::CountAll),
            Rule::FunctionInvocation => Ok(parse_funcion_invocation(pair)?),
            Rule::ParenthesizedExpression => {
                parse_expr(pair.into_inner()).map(|ex| Expr::Nested(Box::new(ex)))
            }
            rule => unsupported("parse_atom", &rule),
        })
}

fn parse_funcion_invocation(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    let mut name = None;
    let mut args = vec![];
    let mut distinct = false;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::FunctionName => {
                name = Some(pair.as_str().to_string());
            }
            Rule::DISTINCT => {
                distinct = true;
            }
            Rule::Expression => {
                args.push(parse_expr(pair.into_inner())?);
            }
            rule => return unsupported("parse_funcion_invocation", &rule),
        }
    }
    let name = name.ok_or_else(|| {
        ParseError::SyntaxError("Function invocation missing function name".to_string())
    })?;
    Ok(Expr::FunctionInvocation {
        name,
        distinct,
        args,
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
            Rule::ListLiteral => parse_list_literal(pair),
            Rule::NULL => Ok(Literal::Null),
            rule => unsupported("parse_literal", &rule),
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
            rule => unsupported("parse_number_literal", &rule),
        })
}

pub fn parse_string_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let s = pair
        .as_str()
        .strip_prefix('\'')
        .ok_or_else(|| ParseError::SyntaxError("String literal missing opening quote".to_string()))?
        .strip_suffix('\'')
        .ok_or_else(|| {
            ParseError::SyntaxError("String literal missing closing quote".to_string())
        })?;
    Ok(Literal::Str(s.to_string()))
}

pub fn parse_boolean_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let b = pair.as_str().parse()?;
    Ok(Literal::Bool(b))
}

pub fn parse_pattern(
    pair: Pair<Rule>,
    un_named_counter: &mut usize,
) -> Result<Pattern, ParseError> {
    let mut parts = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::PatternPart => {
                parts.push(parse_pattern_part(pair, un_named_counter)?);
            }
            _ => {}
        }
    }
    Ok(Pattern(parts))
}

pub fn parse_pattern_part(
    pair: Pair<Rule>,
    un_named_counter: &mut usize,
) -> Result<PatternPart, ParseError> {
    let mut var = None;
    let mut first_node = None;
    let mut rel_chain = Vec::new();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                var = Some(parse_variable(pair)?);
            }
            Rule::PatternElement => {
                parse_pattern_element(&mut rel_chain, &mut first_node, pair, un_named_counter)?;
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
    un_named_counter: &mut usize,
) -> Result<(), ParseError> {
    let mut rels = vec![];
    let mut nodes = vec![];

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::NodePattern => {
                if first_node.is_none() {
                    let np = parse_node_pattern(pair, un_named_counter)?;
                    *first_node = Some(np);
                } else {
                    nodes.push(parse_node_pattern(pair, un_named_counter)?);
                }
            }
            Rule::RelationshipPattern => {
                rels.push(parse_rel_pattern(pair, un_named_counter)?);
            }
            rule => return unsupported("parse_pattern_element", &rule),
        }
    }
    // interleave nodes and rels

    for hop in rels.into_iter().zip(nodes) {
        rel_chain.push(hop);
    }

    Ok(())
}

fn parse_rel_detail(
    pair: Pair<'_, Rule>,
    un_named_counter: &mut usize,
) -> Result<RelPattern, ParseError> {
    let mut rel_pattern = RelPattern::default();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                let var = parse_variable(pair)?;
                rel_pattern.name = var;
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
            rule => return unsupported("parse_rel_detail", &rule),
        }
    }

    if rel_pattern.name.is_empty() {
        rel_pattern.name = format!("r_{}", un_named_counter);
        *un_named_counter += 1;
    }

    Ok(rel_pattern)
}

fn parse_rel_pattern(
    pair: Pair<'_, Rule>,
    un_named_counter: &mut usize,
) -> Result<RelPattern, ParseError> {
    let mut rel_pattern = RelPattern::default();

    let mut direction = Direction::BOTH;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Dash => {}
            Rule::LeftArrowHead => {
                direction = Direction::IN;
            }
            Rule::RightArrowHead => {
                direction = Direction::OUT;
            }
            Rule::RelationshipDetail => {
                rel_pattern = parse_rel_detail(pair, un_named_counter)?;
            }
            rule => return unsupported("parse_rel_pattern", &rule),
        }
    }

    rel_pattern.direction = direction;

    Ok(rel_pattern)
}

fn parse_node_pattern(
    pair: Pair<'_, Rule>,
    un_named_counter: &mut usize,
) -> Result<NodePattern, ParseError> {
    let mut node = NodePattern::default();
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::Variable => {
                let var = parse_variable(pair)?;
                node.name = var;
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
                    rule => return unsupported("parse_node_pattern 2", &rule),
                },
                None => {}
            },
            rule => return unsupported("parse_node_pattern 1", &rule),
        }
    }

    if node.name.is_empty() {
        node.name = format!("n_{}", un_named_counter);
        *un_named_counter += 1;
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

            rule => return unsupported("parse_map_literal", &rule),
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
            rule => return unsupported("parse_list_literal", &rule),
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

    // FIXME: add support for named expressions and function calls eg. return count(x) as y
    use crate::parser::parse_expr;

    use super::*;
    use pest::Parser;

    use proptest::prelude::*;

    #[test]
    fn check_parse_query_1() {
        let input = "MATCH (n) RETURN n";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("n"), [])]),
                    None
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("n"), None)])
            ]))
        );
    }

    #[test]
    fn check_parse_query_1_count() {
        let input = "MATCH (n) RETURN count(*)";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("n"), [])]),
                    None
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::count_all(), None)])
            ]))
        );
    }

    #[test]
    fn match_with_order_by() {
        let input = "MATCH (n) RETURN n ORDER BY n.name";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("n"), [])]),
                    None
                ),
                Clause::return_(
                    false,
                    Some(OrderBy::new([(Expr::var("n", ["name"]), None)])),
                    [ReturnItem::new(Expr::prop_named("n"), None)]
                ),
            ]))
        );
    }

    #[test]
    fn match_with_order_by_desc_asc() {
        let input = "MATCH (n)-[e]->(m) RETURN e ORDER BY n.name DESC, m.name ASC";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(
                        NodePattern::named("n"),
                        [(RelPattern::out("e"), NodePattern::named("m")),]
                    )]),
                    None
                ),
                Clause::return_(
                    false,
                    Some(OrderBy::new([
                        (Expr::var("n", ["name"]), Some(false)),
                        (Expr::var("m", ["name"]), Some(true))
                    ])),
                    [ReturnItem::new(Expr::prop_named("e"), None)]
                ),
            ]))
        );
    }

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
                limit: None,
                order_by: None,
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
    fn check_parse_return_limit() {
        let input = "RETURN n LIMIT 10";

        let pairs = CypherParser::parse(Rule::Return, input);
        assert!(pairs.is_ok());

        let return_clause = parse_return(pairs.unwrap().next().unwrap());

        assert_eq!(
            return_clause,
            Ok(Return {
                all: false,
                limit: Some(10),
                order_by: None,
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
                limit: None,
                order_by: None,
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
                limit: None,
                order_by: None,
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
            let input = str_num.strip_prefix('+').unwrap();
            let pairs = CypherParser::parse(Rule::Literal, input);
            assert!(pairs.is_ok());

            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Float(n)));


            let str_num = format!("{:.1000}", n);
            let input = str_num.trim();
            let pairs = CypherParser::parse(Rule::Literal, input);
            assert!(pairs.is_ok());

            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Float(n)));
        }

        #[test]
        fn literal_string(input in any::<String>().prop_filter("no single qotes", |s| !s.contains('\'') && !s.contains('\\')).prop_map(|input| format!("'{}'", input))) {
            let pairs = CypherParser::parse(Rule::Literal, &input);
            assert!(pairs.is_ok());

            let actual_input = input.strip_prefix('\'').unwrap().strip_suffix('\'').unwrap();
            let literal = parse_literal(pairs.unwrap().next().unwrap());
            assert_eq!(literal, Ok(Literal::Str(actual_input.to_string())));
        }

    }

    #[test]
    fn check_node_pattern_empty() {
        let input = "()";

        let pairs = CypherParser::parse(Rule::NodePattern, input);
        assert!(pairs.is_ok());

        let node = parse_node_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            node,
            Ok(NodePattern {
                name: "n_0".to_string(),
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

        let node = parse_node_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            node,
            Ok(NodePattern {
                name: "n".to_string(),
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

        let node = parse_node_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            node,
            Ok(NodePattern {
                name: "n".to_string(),
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

        let node = parse_node_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            node,
            Ok(NodePattern {
                name: "n".to_string(),
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

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            rel,
            Ok(RelPattern {
                name: "r".to_string(),
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
    fn check_edge_pattern_label() {
        let input = "-[r:KNOWS]->";

        let pairs = CypherParser::parse(Rule::RelationshipPattern, input);
        assert!(pairs.is_ok());

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(
            rel,
            Ok(RelPattern {
                name: "r".to_string(),
                direction: Direction::OUT,
                rel_types: vec!["KNOWS".to_string()],
                props: None
            })
        );
    }

    #[test]
    fn check_edge_pattern_name_only() {
        let input = "-[r]->";

        let pairs = CypherParser::parse(Rule::RelationshipPattern, input);
        assert!(pairs.is_ok());

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(rel, Ok(RelPattern::out("r")));

        let input = "<-[r]-";

        let pairs = CypherParser::parse(Rule::RelationshipPattern, input);
        assert!(pairs.is_ok());

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(rel, Ok(RelPattern::into("r")));

        let input = "-[r]-";

        let pairs = CypherParser::parse(Rule::RelationshipPattern, input);
        assert!(pairs.is_ok());

        let rel = parse_rel_pattern(pairs.unwrap().next().unwrap(), &mut 0);
        assert_eq!(rel, Ok(RelPattern::undirected("r")));
    }

    #[test]
    fn map_literal() {
        let input = "{a: 1, b: true}";
        let pairs = CypherParser::parse(Rule::MapLiteral, input);
        assert!(pairs.is_ok());

        let map = parse_map_literal(pairs.unwrap().next().unwrap());
        assert_eq!(
            map,
            Ok(vec![
                ("a".to_string(), Expr::Literal(Literal::Int(1))),
                ("b".to_string(), Expr::Literal(Literal::Bool(true)))
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
        global_info_logger();
        let input = "a.name = 'John' and 1 < a.age";
        let pairs = CypherParser::parse(Rule::Expression, input);
        assert!(pairs.is_ok());
        info!("{:?}", pairs);

        let expr = parse_expr(pairs.unwrap());

        info!("{:?}", expr);
        assert_eq!(
            expr,
            Ok(Expr::and(
                Expr::eq(Expr::prop("a", ["name"]), Expr::str("John")),
                Expr::lt(Expr::int(1), Expr::prop("a", ["age"]))
            ))
        );
    }

    #[test]
    fn expression_with_parens_change_priority() {
        let input = "a.name = 'John' or (1 < a.age and a.age < 10)";
        let pairs = CypherParser::parse(Rule::Expression, input);
        assert!(pairs.is_ok());

        let expr = parse_expr(pairs.unwrap());

        assert_eq!(
            expr,
            Ok(Expr::or(
                Expr::eq(Expr::prop("a", ["name"]), Expr::str("John")),
                Expr::nested(Expr::and(
                    Expr::lt(Expr::int(1), Expr::prop("a", ["age"])),
                    Expr::lt(Expr::prop("a", ["age"]), Expr::int(10))
                ))
            ))
        );
    }

    #[test]
    fn in_expression() {
        let input = "a.name in ['John', 'Doe']";
        let pairs = CypherParser::parse(Rule::Expression, input);
        assert!(pairs.is_ok());

        let expr = parse_expr(pairs.unwrap());

        assert_eq!(
            expr,
            Ok(Expr::in_(
                Expr::prop("a", ["name"]),
                [
                    Literal::Str("John".to_string()),
                    Literal::Str("Doe".to_string())
                ]
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

    use pretty_assertions::assert_eq;
    use raphtory::logging::global_info_logger;
    use tracing::info;

    #[test]
    fn parse_lanl_large_paths_with_expr() {
        let input = "MATCH
        (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B),
        (E)<-[nf2:Netflow]-(C)<-[login2:Events2v]-(A), (C)<-[prog2:Events1v]-(C),
        (E)<-[nf3:Netflow]-(D)<-[login3:Events2v]-(A), (D)<-[prog3:Events1v]-(D)
      WHERE A <> B AND A <> C AND A <> D AND A <> E AND B <> C AND B <> D AND B <> E
        AND C <> D AND C <> E AND D <> E
        AND login1.eventID = 4624 AND login2.eventID = 4624 AND login3.eventID = 4624 
        AND prog1.eventID = 4688 AND prog2.eventID = 4688 AND prog3.eventID = 4688
        AND nf1.dstBytes > 100000000 AND nf2.dstBytes > 100000000 AND nf3.dstBytes > 100000000
        AND login1.epochtime < login2.epochtime
        AND login2.epochtime < login3.epochtime
        AND login3.epochtime - login1.epochtime < 3600
        AND nf1.dstPort = nf2.dstPort AND nf2.dstPort = nf3.dstPort
        AND prog1.processName = prog2.processName AND prog2.processName = prog3.processName
        AND login1.epochtime < prog1.epochtime
        AND prog1.epochtime < nf1.epochtime
        AND nf1.epochtime - login1.epochtime <= 30
        AND login2.epochtime < prog2.epochtime
        AND prog2.epochtime < nf2.epochtime
        AND nf2.epochtime - login2.epochtime <= 30
        AND login3.epochtime < prog3.epochtime
        AND prog3.epochtime < nf3.epochtime
        AND nf3.epochtime - login3.epochtime <= 30 
      RETURN login1.epochtime as time1, login2.epochtime as time2,
        login3.epochtime as time3, login3.epochtime - login1.epochtime as interval,
        nf1.dstPort as dport1, nf2.dstPort as dport2, nf3.dstPort as dport3";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert!(query.is_ok());

        // get the where expression
        match query {
            Ok(Query::SingleQuery(q)) => {
                let where_clause = q.clauses.iter().find_map(|c| match c {
                    Clause::Match(Match {
                        where_clause: Some(expr),
                        ..
                    }) => Some(expr),
                    _ => None,
                });
                assert!(where_clause.is_some());

                let where_clause = where_clause.unwrap();

                let ands = count_expr(where_clause, &BinOpType::And);
                assert_eq!(ands, 34);

                let ltes = count_expr(where_clause, &BinOpType::Lte);
                assert_eq!(ltes, 3);

                let eqs = count_expr(where_clause, &BinOpType::Eq);
                assert_eq!(eqs, 10);

                let gts = count_expr(where_clause, &BinOpType::Gt);
                assert_eq!(gts, 3);

                let sub = count_expr(where_clause, &BinOpType::Sub);
                assert_eq!(sub, 4);
            }
            _ => unreachable!(),
        }
    }

    fn count_expr(expr: &Expr, bin_op_tpe: &BinOpType) -> usize {
        match expr {
            Expr::BinOp { op, left, right } if op == bin_op_tpe => {
                1 + count_expr(left, bin_op_tpe) + count_expr(right, bin_op_tpe)
            }
            Expr::BinOp { left, right, .. } => {
                count_expr(left, bin_op_tpe) + count_expr(right, bin_op_tpe)
            }
            _ => 0,
        }
    }

    #[test]
    fn parse_lanl_large_paths() {
        let input = "MATCH
        (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B),
        (E)<-[nf2:Netflow]-(C)<-[login2:Events2v]-(A), (C)<-[prog2:Events1v]-(C),
        (E)<-[nf3:Netflow]-(D)<-[login3:Events2v]-(A), (D)<-[prog3:Events1v]-(D)
      RETURN count(*)";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert!(query.is_ok());

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf1", ["Netflow"]),
                                    NodePattern::named("B")
                                ),
                                (
                                    RelPattern::into_labels("login1", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("B"),
                            vec![(
                                RelPattern::into_labels("prog1", ["Events1v"]),
                                NodePattern::named("B")
                            ),]
                        ),
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf2", ["Netflow"]),
                                    NodePattern::named("C")
                                ),
                                (
                                    RelPattern::into_labels("login2", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("C"),
                            vec![(
                                RelPattern::into_labels("prog2", ["Events1v"]),
                                NodePattern::named("C")
                            ),]
                        ),
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf3", ["Netflow"]),
                                    NodePattern::named("D")
                                ),
                                (
                                    RelPattern::into_labels("login3", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("D"),
                            vec![(
                                RelPattern::into_labels("prog3", ["Events1v"]),
                                NodePattern::named("D")
                            ),]
                        ),
                    ]),
                    None
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::count_all(), None)])
            ]))
        );
    }

    #[test]
    fn parse_lanl_no_where() {
        let input = "MATCH (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B) RETURN count(*)";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert!(query.is_ok());

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf1", ["Netflow"]),
                                    NodePattern::named("B")
                                ),
                                (
                                    RelPattern::into_labels("login1", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("B"),
                            vec![(
                                RelPattern::into_labels("prog1", ["Events1v"]),
                                NodePattern::named("B")
                            ),]
                        ),
                    ]),
                    None
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::count_all(), None)])
            ]))
        );
    }

    #[test]
    fn parse_lanl_where_on_nodes() {
        let input = "MATCH (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B) WHERE A <> B AND B <> E AND A <> E RETURN count(*)";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert!(query.is_ok());

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf1", ["Netflow"]),
                                    NodePattern::named("B")
                                ),
                                (
                                    RelPattern::into_labels("login1", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("B"),
                            vec![(
                                RelPattern::into_labels("prog1", ["Events1v"]),
                                NodePattern::named("B")
                            ),]
                        ),
                    ]),
                    Some(Expr::and(
                        Expr::and(
                            Expr::neq(Expr::prop_named("A"), Expr::prop_named("B")),
                            Expr::neq(Expr::prop_named("B"), Expr::prop_named("E"))
                        ),
                        Expr::neq(Expr::prop_named("A"), Expr::prop_named("E"))
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::count_all(), None)])
            ]))
        );
    }

    #[test]
    fn parse_lanl_full() {
        let input = "MATCH (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
                            WHERE A <> B AND B <> E AND A <> E
                                AND login1.eventID = 4624
                                AND prog1.eventID = 4688
                                AND nf1.dstBytes > 100000000
                                AND login1.epochtime < prog1.epochtime
                                AND prog1.epochtime < nf1.epochtime
                                AND nf1.epochtime - login1.epochtime <= 30
                            RETURN count(*)";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![
                        PatternPart::path(
                            NodePattern::named("E"),
                            vec![
                                (
                                    RelPattern::into_labels("nf1", ["Netflow"]),
                                    NodePattern::named("B")
                                ),
                                (
                                    RelPattern::into_labels("login1", ["Events2v"]),
                                    NodePattern::named("A")
                                ),
                            ]
                        ),
                        PatternPart::path(
                            NodePattern::named("B"),
                            vec![(
                                RelPattern::into_labels("prog1", ["Events1v"]),
                                NodePattern::named("B")
                            ),]
                        ),
                    ]),
                    Some(Expr::and(
                        Expr::and(
                            Expr::and(
                                Expr::and(
                                    Expr::and(
                                        Expr::and(
                                            Expr::and(
                                                Expr::and(
                                                    Expr::neq(
                                                        Expr::prop_named("A"),
                                                        Expr::prop_named("B")
                                                    ),
                                                    Expr::neq(
                                                        Expr::prop_named("B"),
                                                        Expr::prop_named("E")
                                                    )
                                                ),
                                                Expr::neq(
                                                    Expr::prop_named("A"),
                                                    Expr::prop_named("E")
                                                )
                                            ),
                                            Expr::eq(
                                                Expr::prop("login1", ["eventID"]),
                                                Expr::int(4624)
                                            )
                                        ),
                                        Expr::eq(Expr::prop("prog1", ["eventID"]), Expr::int(4688))
                                    ),
                                    Expr::gt(Expr::prop("nf1", ["dstBytes"]), Expr::int(100000000))
                                ),
                                Expr::lt(
                                    Expr::prop("login1", ["epochtime"]),
                                    Expr::prop("prog1", ["epochtime"])
                                )
                            ),
                            Expr::lt(
                                Expr::prop("prog1", ["epochtime"]),
                                Expr::prop("nf1", ["epochtime"])
                            )
                        ),
                        Expr::lte(
                            Expr::sub(
                                Expr::prop("nf1", ["epochtime"]),
                                Expr::prop("login1", ["epochtime"])
                            ),
                            Expr::int(30)
                        )
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::count_all(), None)])
            ]))
        );
    }

    #[test]
    fn parse_with_in_clause_and_return_path() {
        let input = "MATCH p=(a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.name in ['John', 'Doe'] return p";

        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);

        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::named_path(
                        "p",
                        NodePattern {
                            name: "a".to_string(),
                            labels: vec!["Person".to_string()],
                            props: None
                        },
                        vec![(
                            RelPattern {
                                name: "r".to_string(),
                                direction: Direction::OUT,
                                rel_types: vec!["KNOWS".to_string()],
                                props: None
                            },
                            NodePattern {
                                name: "b".to_string(),
                                labels: vec!["Person".to_string()],
                                props: None
                            }
                        )]
                    )]),
                    Some(Expr::in_(
                        Expr::prop("a", ["name"]),
                        [
                            Literal::Str("John".to_string()),
                            Literal::Str("Doe".to_string())
                        ]
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("p"), None)])
            ]))
        );
    }

    #[test]
    fn parse_where_is_null_and_not_null() {
        let input = "MATCH (a) WHERE a.name IS NULL RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("a"), [])]),
                    Some(Expr::is_null(Expr::prop("a", ["name"])))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("a"), None)])
            ]))
        );

        let input = "MATCH (a) WHERE a.name IS NOT NULL RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("a"), [])]),
                    Some(Expr::is_not_null(Expr::prop("a", ["name"])))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("a"), None)])
            ]))
        );

        // with and

        let input = "MATCH (a) WHERE a.name IS NOT NULL AND a.age = 12 RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("a"), [])]),
                    Some(Expr::and(
                        Expr::is_not_null(Expr::prop("a", ["name"])),
                        Expr::eq(Expr::prop("a", ["age"]), Expr::int(12))
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("a"), None)])
            ]))
        );
    }

    #[test]
    fn parse_contains_starts_ends_with() {
        let input = "MATCH (a) WHERE a.name CoNTaINS 'John' AND a.name ENDS WITH 'Doe' RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("a"), [])]),
                    Some(Expr::and(
                        Expr::contains(Expr::prop("a", ["name"]), Expr::str("John")),
                        Expr::ends_with(Expr::prop("a", ["name"]), Expr::str("Doe"))
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("a"), None)])
            ]))
        );
    }

    #[test]
    fn parse_contains_not_starts_ends_with() {
        let input =
            "MATCH (a) WHERE a.name CoNTaINS 'John' AND NOT a.name ENDS WITH 'Doe' RETURN a";
        let pairs = CypherParser::parse(Rule::Cypher, input);
        assert!(pairs.is_ok());

        let query = parse_cypher(input);
        assert_eq!(
            query,
            Ok(Query::single(vec![
                Clause::match_(
                    Pattern(vec![PatternPart::path(NodePattern::named("a"), [])]),
                    Some(Expr::and(
                        Expr::contains(Expr::prop("a", ["name"]), Expr::str("John")),
                        Expr::not(Expr::ends_with(Expr::prop("a", ["name"]), Expr::str("Doe")))
                    ))
                ),
                Clause::return_(false, None, [ReturnItem::new(Expr::prop_named("a"), None)])
            ]))
        );
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
    fn match_24() {
        let input = "MATCH (a)-[:ADMIN]-(b) WHERE a:A AND b:B RETURN a, b";
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
