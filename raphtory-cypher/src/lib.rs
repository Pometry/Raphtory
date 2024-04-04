use std::sync::Arc;

use arrow_array::{builder, UInt64Array};
use arrow_schema::{DataType, Fields, Schema};
use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SQLOptions, SessionContext, SessionState},
        runtime_env::RuntimeEnv,
    },
    logical_expr::{create_udf, ColumnarValue, Volatility},
};
use executor::{table_provider::edge::EdgeListTableProvider, ExecError};
use itertools::Itertools;
use parser::ast::*;
use raphtory::{arrow::graph_impl::ArrowGraph, core::Direction};
use sqlparser::ast::{
    self as sql_ast, GroupByExpr, SetExpr, TableAlias, WildcardAdditionalOptions, With,
};

use crate::{executor::table_provider::node::NodeTableProvider, hop::rule::HopRule};

pub mod executor;
pub mod hop;
pub mod parser;

pub fn cypher_to_sql(query: &Query, graph: &ArrowGraph) -> sql_ast::Statement {
    sql_ast::Statement::Query(Box::new(sql_ast::Query {
        // WITH (common table expressions, or CTEs)
        with: None,
        // SELECT or UNION / EXCEPT / INTERSECT
        body: parse_select_body(query, graph),
        // ORDER BY
        order_by: parse_order_by(query),
        // `LIMIT { <N> | ALL }`
        limit: parse_limit(query),

        // `LIMIT { <N> } BY { <expr>,<expr>,... } }`
        limit_by: vec![],
        // `OFFSET <N> [ { ROW | ROWS } ]`
        offset: None,
        // `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
        fetch: None,
        // `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
        locks: vec![],
        // `FOR XML { RAW | AUTO | EXPLICIT | PATH } [ , ELEMENTS ]`
        // `FOR JSON { AUTO | PATH } [ , INCLUDE_NULL_VALUES ]`
        // (MSSQL-specific)
        for_clause: None,
    }))
}

pub fn cypher_to_sql_with_ctes(query: &Query, graph: &ArrowGraph) -> sql_ast::Statement {
    let with = parse_rels_to_ctes(query, graph);
    sql_ast::Statement::Query(Box::new(sql_ast::Query {
        // WITH (common table expressions, or CTEs)
        with: Some(with),
        // SELECT or UNION / EXCEPT / INTERSECT
        body: parse_select_body(query, graph),
        // ORDER BY
        order_by: parse_order_by(query),
        // `LIMIT { <N> | ALL }`
        limit: parse_limit(query),

        // `LIMIT { <N> } BY { <expr>,<expr>,... } }`
        limit_by: vec![],
        // `OFFSET <N> [ { ROW | ROWS } ]`
        offset: None,
        // `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
        fetch: None,
        // `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
        locks: vec![],
        // `FOR XML { RAW | AUTO | EXPLICIT | PATH } [ , ELEMENTS ]`
        // `FOR JSON { AUTO | PATH } [ , INCLUDE_NULL_VALUES ]`
        // (MSSQL-specific)
        for_clause: None,
    }))
}

fn parse_limit(query: &Query) -> Option<sql_ast::Expr> {
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

fn parse_order_by(_query: &Query) -> Vec<sql_ast::OrderByExpr> {
    vec![]
}

fn sql_table(
    layer_names: &[impl AsRef<str>],
    name: &impl AsRef<str>,
    graph: &ArrowGraph,
) -> sql_ast::Cte {
    // fetch and merge the schemas

    let schemas = layer_names
        .iter()
        .filter_map(|layer| graph.find_layer_id(layer.as_ref()))
        .filter_map(|layer_id| full_layer_fields(graph, layer_id))
        .map(Schema::new);

    // this is the schema that all layers must match, any missing columns will be filled with NULLs
    let schema = Schema::try_merge(schemas).expect("failed to merge schemas");

    if layer_names.len() > 1 {
        let union_query = layer_names
            .iter()
            .map(|layer| select_scan_query(layer.as_ref(), graph, Some(&schema)))
            // FIXME: there seems to be an issue in which DataFusion executes the query where it sometimes complains about the schema not matching
            // this is an attempted workaround where we lift the most descriptive schema (the one with fewest nulls) to the top of the UNION ALL
            .sorted_by(|(null_count1, _), (null_count2, _)| null_count1.cmp(null_count2))
            .map(|(_, query)| query)
            .reduce(query_union)
            .unwrap();
        sql_ast::Cte {
            alias: TableAlias {
                name: sql_ast::Ident::new(name.as_ref()),
                columns: vec![],
            },
            query: union_query,
            from: None,
        }
    } else {
        sql_ast::Cte {
            alias: TableAlias {
                name: sql_ast::Ident::new(name.as_ref()),
                columns: vec![],
            },
            query: select_scan_query(layer_names.first().unwrap().as_ref(), graph, None).1,
            from: None,
        }
    }
}

fn full_layer_fields(graph: &ArrowGraph, layer_id: usize) -> Option<Fields> {
    let dt = graph.layer(layer_id).edges_props_data_type();
    let arr_dt: arrow_schema::DataType = dt.clone().into();
    match arr_dt {
        arrow_schema::DataType::Struct(fields) => {
            let mut all_fields = vec![];
            all_fields.push(Arc::new(arrow_schema::Field::new(
                "layer_id",
                arrow_schema::DataType::UInt64,
                false,
            )));
            all_fields.push(Arc::new(arrow_schema::Field::new(
                "src",
                arrow_schema::DataType::UInt64,
                false,
            )));
            all_fields.push(Arc::new(arrow_schema::Field::new(
                "dst",
                arrow_schema::DataType::UInt64,
                false,
            )));

            all_fields.extend(fields.iter().cloned());

            Some(all_fields.into())
        }
        _ => None,
    }
}

fn query_union(q1: Box<sql_ast::Query>, q2: Box<sql_ast::Query>) -> Box<sql_ast::Query> {
    Box::new(sql_ast::Query {
        // WITH (common table expressions, or CTEs)
        with: None,
        // SELECT or UNION / EXCEPT / INTERSECT
        body: Box::new(SetExpr::SetOperation {
            op: sql_ast::SetOperator::Union,
            set_quantifier: sql_ast::SetQuantifier::All,
            left: q1.body,
            right: q2.body,
        }),
        // ORDER BY
        order_by: vec![],
        // `LIMIT { <N> | ALL }`
        limit: None,
        // `LIMIT { <N> } BY { <expr>,<expr>,... } }`
        limit_by: vec![],
        // `OFFSET <N> [ { ROW | ROWS } ]`
        offset: None,
        // `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
        fetch: None,
        // `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
        locks: vec![],
        // `FOR XML { RAW | AUTO | EXPLICIT | PATH } [ , ELEMENTS ]`
        // `FOR JSON { AUTO | PATH } [ , INCLUDE_NULL_VALUES ]`
        // (MSSQL-specific)
        for_clause: None,
    })
}

// fn dt_to_sql_dt(dt: &DataType) -> sqlparser::ast::DataType {
//     match dt {
//         DataType::Boolean => sqlparser::ast::DataType::Boolean,
//         DataType::Int8 => sqlparser::ast::DataType::TinyInt(None),
//         DataType::Int16 => sqlparser::ast::DataType::SmallInt(None),
//         DataType::Int32 => sqlparser::ast::DataType::Int(None),
//         DataType::Int64 => sqlparser::ast::DataType::BigInt(None),
//         DataType::UInt8 => sqlparser::ast::DataType::TinyInt(None),
//         DataType::UInt16 => sqlparser::ast::DataType::SmallInt(None),
//         DataType::UInt32 => sqlparser::ast::DataType::Int(None),
//         DataType::UInt64 => sqlparser::ast::DataType::BigInt(None),
//         DataType::Float32 => sqlparser::ast::DataType::Real,
//         DataType::Float64 => sqlparser::ast::DataType::Double,
//         DataType::Utf8 => sqlparser::ast::DataType::Text,
//         DataType::LargeUtf8 => sqlparser::ast::DataType::Text,

//         _ => unimplemented!("unsupported data type {:?}", dt),
//     }
// }

fn select_scan_query(
    layer_name: &str,
    graph: &ArrowGraph,
    total_schema: Option<&Schema>,
) -> (usize, Box<sql_ast::Query>) {
    let layer_id = graph.find_layer_id(layer_name).expect("layer not found");
    let layer_schema = full_layer_fields(graph, layer_id);

    let projection_with_priority = total_schema
        .zip(layer_schema)
        .map(|(schema, layer_schema)| {
            let mut select_items = vec![];
            let mut null_count = 0usize;
            for field in schema.fields().iter() {
                if let Some(field) = layer_schema.into_iter().find(|&f| f.name() == field.name()) {
                    // if the field is present in the layer schema
                    let item = sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::Identifier(
                        sql_ast::Ident::new(field.name().clone()),
                    ));
                    select_items.push(item);
                } else {
                    // if the field is missing in the layer schema replace with NULL as field name
                    let item = sql_ast::SelectItem::ExprWithAlias {
                        expr: sql_ast::Expr::Value(sql_ast::Value::Null),
                        alias: sql_ast::Ident::new(field.name()),
                    };
                    select_items.push(item);
                    null_count += 1;
                }
            }
            (select_items, null_count)
        })
        .unwrap_or_else(|| {
            (
                vec![sql_ast::SelectItem::Wildcard(
                    WildcardAdditionalOptions::default(),
                )],
                0,
            )
        });

    let (projection, null_count) = projection_with_priority;

    (
        null_count,
        Box::new(sql_ast::Query {
            // WITH (common table expressions, or CTEs)
            with: None,
            // SELECT or UNION / EXCEPT / INTERSECT
            body: Box::new(SetExpr::Select(Box::new(sql_ast::Select {
                distinct: None,
                // MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
                top: None,
                // projection expressions
                projection,
                // INTO
                into: None,
                // FROM
                from: vec![sql_ast::TableWithJoins {
                    relation: table_from_name(layer_name),
                    joins: vec![],
                }],
                // LATERAL VIEWs
                lateral_views: vec![],
                // WHERE
                selection: None,
                // GROUP BY
                group_by: GroupByExpr::Expressions(vec![]),
                // CLUSTER BY (Hive)
                cluster_by: vec![],
                // DISTRIBUTE BY (Hive)
                distribute_by: vec![],
                // SORT BY (Hive)
                sort_by: vec![],
                // HAVING
                having: None,
                // WINDOW AS
                named_window: vec![],
                // QUALIFY (Snowflake)
                qualify: None,
            }))),
            // ORDER BY
            order_by: vec![],
            // `LIMIT { <N> | ALL }`
            limit: None,
            // `LIMIT { <N> } BY { <expr>,<expr>,... } }`
            limit_by: vec![],
            // `OFFSET <N> [ { ROW | ROWS } ]`
            offset: None,
            // `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
            fetch: None,
            // `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
            locks: vec![],
            // `FOR XML { RAW | AUTO | EXPLICIT | PATH } [ , ELEMENTS ]`
            // `FOR JSON { AUTO | PATH } [ , INCLUDE_NULL_VALUES ]`
            // (MSSQL-specific)
            for_clause: None,
        }),
    )
}

fn parse_rels_to_ctes(query: &Query, graph: &ArrowGraph) -> With {
    // each rel can become a CTE
    // inside the cte
    // if the pattern has no layers -[e]- and the graph has one layer then we just select * from the layer
    // if the pattern has one layer -[e:A]- then we just select * from A
    // if the pattern has no layers -[e]-> and the graph has multiple layers then we UNION ALL select * from each layer
    // if the pattern has multiple layers -[e:A:B]-> then we select * from A UNION ALL select * from B
    // we name these CTEs with the rel name
    // in each CTE where we UNION ALL we must merge the schemas and add missing columns with NULLs if they don't match.

    let mut cte_tables = vec![];

    let layer_names = graph.layer_names();

    for rel in all_rels(query) {
        // rewrite the conditions in a nicer way
        if rel.rel_types.is_empty() {
            // select * from layer
            cte_tables.push(sql_table(layer_names, &rel.name, graph))
        } else {
            // UNION ALL for all the layers of the relation pattern
            cte_tables.push(sql_table(&rel.rel_types, &rel.name, graph))
        }
    }

    With {
        recursive: false,
        cte_tables,
    }
}

fn parse_select_body(query: &Query, _graph: &ArrowGraph) -> Box<sql_ast::SetExpr> {
    let order_by = vec![];

    let from_tables = parse_tables(query);

    let binds = rel_names(query);

    Box::new(sql_ast::SetExpr::Select(Box::new(sql_ast::Select {
        distinct: None,
        // MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
        top: None,
        // projection expressions
        projection: parse_projection(query, &binds),
        // INTO
        into: None,
        // FROM
        from: from_tables,
        // LATERAL VIEWs
        lateral_views: vec![],
        // WHERE
        selection: parse_selection(query, &binds),
        // GROUP BY
        group_by: GroupByExpr::Expressions(vec![]),
        // CLUSTER BY (Hive)
        cluster_by: vec![],
        // DISTRIBUTE BY (Hive)
        distribute_by: vec![],
        // SORT BY (Hive)
        sort_by: order_by,
        // HAVING
        having: None,
        // WINDOW AS
        named_window: vec![],
        // QUALIFY (Snowflake)
        qualify: None,
    })))
}

fn rel_names(query: &Query) -> Vec<String> {
    all_rels(query).map(|rel| rel.name.clone()).collect()
}

fn all_rels(query: &Query) -> impl Iterator<Item = &RelPattern> + '_ {
    query
        .clauses()
        .iter()
        .filter_map(|clause| match clause {
            Clause::Match(m) => {
                let iter = m
                    .pattern
                    .0
                    .iter()
                    .flat_map(|part: &PatternPart| part.rel_chain.iter().map(|(rel, _)| rel));
                Some(iter)
            }
            _ => None,
        })
        .flatten()
}

fn parse_tables(query: &Query) -> Vec<sql_ast::TableWithJoins> {
    let m = query
        .clauses()
        .iter()
        .find(|clause| matches!(clause, Clause::Match(_)));
    if let Some(Clause::Match(m)) = m {
        let mut tables = vec![];
        let mut patterns = m.pattern.0.iter();
        let first = patterns.next().unwrap();
        let first_table: sql_ast::TableWithJoins = as_table_with_joins(first);
        tables.push(first_table);
        tables
    } else {
        vec![]
    }
}

fn as_table_with_joins(first: &PatternPart) -> sql_ast::TableWithJoins {
    let PatternPart { rel_chain, .. } = first;
    let mut joins = vec![];
    let mut iter = rel_chain.iter().peekable();

    let (rel, _) = iter.peek().unwrap(); // FIXME: it will breake for match (n)

    for ((rel1, _), (rel2, _)) in iter.tuple_windows() {
        let (from, to) = match (rel1.direction, rel2.direction) {
            (Direction::OUT, Direction::OUT) => ("dst", "src"),
            (Direction::OUT, Direction::IN) => ("dst", "dst"),
            (Direction::IN, Direction::OUT) => ("src", "src"),
            (Direction::IN, Direction::IN) => ("src", "dst"),
            _ => unimplemented!("both direction not supported"),
        };
        let join_op =
            sql_ast::JoinOperator::Inner(sql_ast::JoinConstraint::On(sql_ast::Expr::BinaryOp {
                left: Box::new(sql_ast::Expr::CompoundIdentifier(vec![
                    sql_ast::Ident::new(&rel1.name),
                    sql_ast::Ident::new(from),
                ])),
                op: sql_ast::BinaryOperator::Eq,
                right: Box::new(sql_ast::Expr::CompoundIdentifier(vec![
                    sql_ast::Ident::new(&rel2.name),
                    sql_ast::Ident::new(to),
                ])),
            }));

        let join = sql_ast::Join {
            relation: table_from_name(&rel2.name),
            join_operator: join_op,
        };

        joins.push(join)
    }

    sql_ast::TableWithJoins {
        relation: table_from_name(&rel.name),
        joins,
    }
}

fn table_from_name(name: &str) -> sql_ast::TableFactor {
    sql_ast::TableFactor::Table {
        name: sql_ast::ObjectName(vec![sql_ast::Ident::new(name.to_string())]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        partitions: vec![],
    }
}

fn parse_projection(query: &Query, binds: &[String]) -> Vec<sql_ast::SelectItem> {
    query
        .clauses()
        .iter()
        .find(|clause| matches!(clause, Clause::Return(_)))
        .map(|clause| match clause {
            Clause::Return(Return { all: true, .. }) => {
                vec![sql_ast::SelectItem::Wildcard(
                    WildcardAdditionalOptions::default(),
                )]
            }
            Clause::Return(Return { items, .. }) => items
                .iter()
                .map(|ret_i| {
                    let expr = cypher_to_sql_expr(&ret_i.expr, binds, true);
                    if let Some(name) = ret_i.as_name.as_ref() {
                        sql_ast::SelectItem::ExprWithAlias {
                            expr,
                            alias: sql_ast::Ident::new(name),
                        }
                    } else if let sql_ast::Expr::QualifiedWildcard(name) = expr {
                        sql_ast::SelectItem::QualifiedWildcard(
                            name,
                            sql_ast::WildcardAdditionalOptions::default(),
                        )
                    } else {
                        sql_ast::SelectItem::UnnamedExpr(expr)
                    }
                })
                .collect(),
            _ => unreachable!(),
        })
        .unwrap_or_default()
}

fn parse_selection(query: &Query, binds: &[String]) -> Option<sql_ast::Expr> {
    let rel_exprs = query
        .clauses()
        .iter()
        .filter(|clause| matches!(clause, Clause::Match(_)))
        .flat_map(|clause| match clause {
            Clause::Match(Match {
                pattern: Pattern(pat_parts),
                ..
            }) => pat_parts.iter().flat_map(|part| {
                part.rel_chain.iter().flat_map(|(rel, _)| {
                    rel.props.iter().flat_map(|props| {
                        props.iter().map(|(prop, expr)| Expr::BinOp {
                            op: BinOpType::Eq,
                            left: Box::new(Expr::Var {
                                var_name: rel.name.clone(),
                                attrs: vec![prop.clone()],
                            }),
                            right: Box::new(expr.clone()),
                        })
                    })
                })
            }),
            _ => unreachable!(),
        })
        .map(|expr| cypher_to_sql_expr(&expr, binds, false));
    let where_exprs = query.clauses().iter().filter_map(|clause| match clause {
        Clause::Match(m) => m
            .where_clause
            .as_ref()
            .map(|expr| cypher_to_sql_expr(expr, binds, false)),
        _ => None,
    });
    where_exprs
        .chain(rel_exprs)
        .reduce(|a, b| sql_ast::Expr::BinaryOp {
            left: Box::new(a),
            op: sql_ast::BinaryOperator::And,
            right: Box::new(b),
        })
}

fn cypher_unary_op_to_sql(op: &UnaryOpType) -> sql_ast::UnaryOperator {
    match op {
        UnaryOpType::Not => sql_ast::UnaryOperator::Not,
        UnaryOpType::Neg => sql_ast::UnaryOperator::Minus,
    }
}

fn cypher_binary_op_to_sql(op: &BinOpType) -> sql_ast::BinaryOperator {
    match op {
        BinOpType::Add => sql_ast::BinaryOperator::Plus,
        BinOpType::Sub => sql_ast::BinaryOperator::Minus,
        BinOpType::Mul => sql_ast::BinaryOperator::Multiply,
        BinOpType::Div => sql_ast::BinaryOperator::Divide,
        BinOpType::Mod => sql_ast::BinaryOperator::Modulo,
        BinOpType::Eq => sql_ast::BinaryOperator::Eq,
        BinOpType::Neq => sql_ast::BinaryOperator::NotEq,
        BinOpType::Gt => sql_ast::BinaryOperator::Gt,
        BinOpType::Gte => sql_ast::BinaryOperator::GtEq,
        BinOpType::Lt => sql_ast::BinaryOperator::Lt,
        BinOpType::Lte => sql_ast::BinaryOperator::LtEq,
        BinOpType::And => sql_ast::BinaryOperator::And,
        BinOpType::Or => sql_ast::BinaryOperator::Or,
        BinOpType::Xor => sql_ast::BinaryOperator::Xor,
        BinOpType::In => unimplemented!("IN operator handled in cypher_to_sql_expr"),
        _ => unimplemented!("unsupported binary operator {:?}", op),
    }
}

fn cypher_to_sql_expr(expr: &Expr, binds: &[String], allow_wildcard_edges: bool) -> sql_ast::Expr {
    match expr {
        Expr::Var { var_name, attrs } => {
            if attrs.is_empty() {
                if allow_wildcard_edges {
                    sql_ast::Expr::QualifiedWildcard(sql_ast::ObjectName(vec![
                        sql_ast::Ident::new(var_name),
                    ]))
                } else {
                    // this makes sure that non-wildcard edges are selected with their id when passed down to functions
                    sql_ast::Expr::CompoundIdentifier(vec![
                        sql_ast::Ident::new(var_name),
                        sql_ast::Ident::new("id"),
                    ])
                }
            } else {
                sql_ast::Expr::CompoundIdentifier(
                    std::iter::once(sql_ast::Ident::new(var_name))
                        .chain(attrs.iter().map(sql_ast::Ident::new))
                        .collect(),
                )
            }
        }
        // contains
        Expr::BinOp {
            op: BinOpType::Contains,
            left,
            right,
        } => str_op(right, left, |s| format!("%{}%", s), binds),
        // starts_with
        Expr::BinOp {
            op: BinOpType::StartsWith,
            left,
            right,
        } => str_op(right, left, |s| format!("{}%", s), binds),
        // ends_with
        Expr::BinOp {
            op: BinOpType::EndsWith,
            left,
            right,
        } => str_op(right, left, |s| format!("%{}", s), binds),
        // in
        Expr::BinOp {
            op: BinOpType::In,
            left,
            right,
        } => {
            let sql_list = match right.as_ref() {
                Expr::Literal(Literal::List(exprs)) => exprs
                    .iter()
                    .map(|lit| cypher_to_sql_expr(&Expr::Literal(lit.clone()), binds, false))
                    .collect::<Vec<_>>(),
                expr => unimplemented!("unexpected right hand side of IN operator {:?}", expr),
            };
            sql_ast::Expr::InList {
                expr: Box::new(cypher_to_sql_expr(left, binds, false)),
                list: sql_list,
                negated: false,
            }
        }
        Expr::BinOp { op, left, right } => sql_ast::Expr::BinaryOp {
            left: Box::new(cypher_to_sql_expr(left, binds, false)),
            op: cypher_binary_op_to_sql(op),
            right: Box::new(cypher_to_sql_expr(right, binds, false)),
        },
        Expr::UnaryOp { op, expr } => sql_ast::Expr::UnaryOp {
            op: cypher_unary_op_to_sql(op),
            expr: Box::new(cypher_to_sql_expr(expr, binds, false)),
        },
        Expr::CountAll => sql_ast::Expr::Function(sql_ast::Function {
            name: sql_ast::ObjectName(vec![sql_ast::Ident::new("COUNT")]),
            args: vec![sql_ast::FunctionArg::Unnamed(
                sql_ast::FunctionArgExpr::Expr(sql_ast::Expr::CompoundIdentifier(
                    vec![binds[0].clone(), "src".to_string()]
                        .into_iter()
                        .map(sql_ast::Ident::new)
                        .collect(),
                )), // this is a hack because datafusion gets confused when there are no columns selected
            )],
            over: None,
            distinct: false,
            filter: None,
            null_treatment: None,
            special: false,
            order_by: vec![],
        }),

        // literals
        Expr::Literal(Literal::Null) => sql_ast::Expr::Value(sql_ast::Value::Null),
        Expr::Literal(Literal::Bool(b)) => sql_ast::Expr::Value(sql_ast::Value::Boolean(*b)),
        Expr::Literal(Literal::Int(i)) => {
            sql_ast::Expr::Value(sql_ast::Value::Number(i.to_string(), true))
        }
        Expr::Literal(Literal::Float(f)) => {
            sql_ast::Expr::Value(sql_ast::Value::Number(f.to_string(), false))
        }
        Expr::Literal(Literal::Str(s)) => {
            sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString(s.to_string()))
        }

        // functions
        Expr::FunctionInvocation {
            name,
            distinct,
            args,
        } => {
            if name == "type" {
                // turn this into type(e.layer_id)
                let first_arg = args.first().expect(
                    "type function must have one argument representing the bind of an edge",
                );
                match first_arg {
                    Expr::Var { var_name, .. } => sql_function(
                        name,
                        &vec![Expr::var(var_name, vec!["layer_id"])],
                        binds,
                        distinct,
                    ),
                    expr => unimplemented!(
                        "type function must have a variable as argument, found {:?}",
                        expr
                    ),
                }
            } else {
                sql_function(name, args, binds, distinct)
            }
        }
        Expr::Nested(expr) => sql_ast::Expr::Nested(Box::new(cypher_to_sql_expr(
            expr,
            binds,
            allow_wildcard_edges & true,
        ))),
        _ => unimplemented!("unsupported expression {:?}", expr),
    }
}

fn sql_function(
    name: &String,
    args: &Vec<Expr>,
    binds: &[String],
    distinct: &bool,
) -> sql_ast::Expr {
    sql_ast::Expr::Function(sql_ast::Function {
        name: sql_ast::ObjectName(vec![sql_ast::Ident::new(name)]),
        args: args
            .iter()
            .map(|arg| {
                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(cypher_to_sql_expr(
                    arg, binds, false,
                )))
            })
            .collect(),
        over: None,
        distinct: *distinct,
        filter: None,
        null_treatment: None,
        special: false,
        order_by: vec![],
    })
}

fn str_op(
    right: &Box<Expr>,
    left: &Box<Expr>,
    pattern: impl Fn(&String) -> String,
    binds: &[String],
) -> sql_ast::Expr {
    match right.as_ref() {
        Expr::Literal(Literal::Str(s)) => sql_ast::Expr::Like {
            negated: false,
            expr: Box::new(cypher_to_sql_expr(left, binds, false)),
            pattern: Box::new(sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString(
                pattern(s),
            ))),
            escape_char: None,
        },
        pattern => unimplemented!(
            "unexpected right hand side of CONTAINS operator {:?}",
            pattern
        ),
    }
}

pub async fn run_cypher(query: &str, graph: &ArrowGraph) -> Result<DataFrame, ExecError> {
    println!("Running query: {:?}", query);
    let query = parser::parse_cypher(query)?;

    let config = SessionConfig::from_env()?.with_information_schema(true);
    // config.options_mut().optimizer.skip_failed_rules = true; // should probably raise these with Datafusion

    let runtime = Arc::new(RuntimeEnv::default());
    let state =
        SessionState::new_with_config_rt(config, runtime).add_optimizer_rule(Arc::new(HopRule {
            graph: graph.clone(),
        }));
    let ctx = SessionContext::new_with_state(state);

    for layer in graph.layer_names() {
        let edge_list_table = EdgeListTableProvider::new(layer, graph.clone())?;
        ctx.register_table(layer, Arc::new(edge_list_table))?;
    }

    let node_table_provider = NodeTableProvider::new(graph.clone())?;
    ctx.register_table("nodes", Arc::new(node_table_provider))?;
    let layer_names = graph.layer_names().to_vec();

    ctx.register_udf(create_udf(
        "type",
        vec![DataType::UInt64],
        DataType::Utf8.into(),
        Volatility::Immutable,
        Arc::new(move |cols| {
            let layer_id_col = match &cols[0] {
                ColumnarValue::Array(a) => a.clone(),
                ColumnarValue::Scalar(a) => a.to_array()?,
            };

            let layer_id_col = layer_id_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Expected column of type u64".to_string())
                })?;

            let mut type_col = builder::StringBuilder::new();
            for layer_id in layer_id_col.values() {
                let layer_name = layer_names
                    .get(*layer_id as usize)
                    .ok_or_else(|| DataFusionError::Execution("Layer not found".to_string()))?;
                type_col.append_value(layer_name);
            }
            Ok(ColumnarValue::Array(Arc::new(type_col.finish())))
        }),
    ));
    ctx.refresh_catalogs().await?;
    let query = cypher_to_sql_with_ctes(&query, graph);

    println!("SQL: {:?}", query.to_string());
    let plan = ctx
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            query,
        )))
        .await?;
    let opts = SQLOptions::new();
    opts.verify_plan(&plan)?;

    let plan = ctx.state().optimize(&plan)?;
    println!("PLAN! {:?}", plan);
    let df = ctx.execute_logical_plan(plan).await?;
    Ok(df)
}

pub async fn run_sql(query: &str, graph: &ArrowGraph) -> Result<DataFrame, ExecError> {
    let ctx = SessionContext::new();

    for layer in graph.layer_names() {
        let table = EdgeListTableProvider::new(layer, graph.clone())?;
        ctx.register_table(layer, Arc::new(table))?;
    }

    let df = ctx.sql(query).await?;
    Ok(df)
}

#[cfg(test)]
mod cyper_2_sql_tests {

    use std::path::Path;

    use super::*;
    use arrow::util::pretty::print_batches;
    use raphtory::{
        core::Prop,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use tempfile::tempdir;

    use pretty_assertions::assert_eq;

    #[test]
    fn select_all() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e",
        );
    }

    #[test]
    fn select_unnamed() {
        check_cypher_to_sql(
            "MATCH ()-[]-() RETURN *",
            "WITH r_1 AS (SELECT * FROM _default) SELECT * FROM r_1",
        );
    }

    #[test]
    fn select_wildcard_name() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN *",
            "WITH e AS (SELECT * FROM _default) SELECT * FROM e",
        );
    }

    #[test]
    fn select_projection() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN e.src, e.weight",
            "WITH e AS (SELECT * FROM _default) SELECT e.src, e.weight FROM e",
        );
    }

    #[test]
    fn select_wildcard_from_layer() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e:KNOWS]-() RETURN e",
            "WITH e AS (SELECT * FROM KNOWS) SELECT e.* FROM e",
            ["KNOWS"],
        );
    }

    #[test]
    fn select_wildcard_count_all() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(*)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.src) FROM e",
        );
    }

    #[test]
    fn select_one_col_count_items() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(e.name)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.name) FROM e",
        );
    }

    #[test]
    fn select_one_col_count_items_distinct() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(distinct e.name)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(DISTINCT e.name) FROM e",
        );
    }

    #[test]
    fn select_with_limit() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN e LIMIT 2",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e LIMIT 2L",
        );
    }

    #[test]
    fn select_where_expr_1() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where e.time > 10 RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE e.time > 10L",
        );
    }

    #[test]
    fn select_edge_with_type() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e]-() where e.time > 10 RETURN e,type(e)",
            "WITH e AS (SELECT time FROM _default UNION ALL SELECT time FROM L1 UNION ALL SELECT time FROM L2) SELECT e.*, type(e.layer_id) FROM e WHERE e.time > 10L",
            ["L1", "L2"],
        );
    }

    #[test]
    fn select_where_str_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where e.name contains 'baa' RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE e.name LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_str_not_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where NOT e.name contains 'baa' RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE NOT e.name LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_expr_2() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]-() RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM _default) SELECT e.time, e.type FROM e WHERE e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_3() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]-() where e.time < 5 RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM _default) SELECT e.time, e.type FROM e WHERE e.time < 5L AND e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_4_layer() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e :LAYER {time: 7}]-() where e.type = 'one' RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM LAYER) SELECT e.time, e.type FROM e WHERE e.type = 'one' AND e.time = 7L",
            ["LAYER"]
        );
    }

    #[test]
    fn hop_two_times_out() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->() RETURN e1, e2",
            "WITH e1 AS (SELECT * FROM _default), e2 AS (SELECT * FROM _default) SELECT e1.*, e2.* FROM e1 JOIN e2 ON e1.dst = e2.src",
        );
    }

    #[test]
    fn hop_3_times_out() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->()-[e3]->() RETURN e1, e2, e3",
            "WITH e1 AS (SELECT * FROM _default), e2 AS (SELECT * FROM _default), e3 AS (SELECT * FROM _default) SELECT e1.*, e2.*, e3.* FROM e1 JOIN e2 ON e1.dst = e2.src JOIN e3 ON e2.dst = e3.src",
        );
    }

    #[test]
    fn hop_two_times_in() {
        check_cypher_to_sql(
            "MATCH ()<-[e1]-()<-[e2]-() RETURN e1, e2",
            "WITH e1 AS (SELECT * FROM _default), e2 AS (SELECT * FROM _default) SELECT e1.*, e2.* FROM e1 JOIN e2 ON e1.src = e2.dst",
        );
    }

    #[test]
    fn hop_out_in() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()<-[e2]-() RETURN e1, e2",
            "WITH e1 AS (SELECT * FROM _default), e2 AS (SELECT * FROM _default) SELECT e1.*, e2.* FROM e1 JOIN e2 ON e1.dst = e2.dst",
        );
    }

    #[test]
    fn respect_parens() {
        check_cypher_to_sql(
            "match ()-[a]->() where a.name = 'John' or (1 < a.age and a.age < 10) RETURN a",
            "WITH a AS (SELECT * FROM _default) SELECT a.* FROM a WHERE a.name = 'John' OR (1L < a.age AND a.age < 10L)",
        );
    }

    // #[test]
    // fn scan_nodes_properties() {
    //     check_cypher_to_sql(
    //         "MATCH (n) RETURN n.name",
    //         "WITH n AS (SELECT * FROM nodes__default) SELECT n.name FROM n",
    //     );
    // }

    // #[test]
    // fn scan_edge_with_node_properties(){
    //     check_cypher_to_sql(
    //         "MATCH (n)-[e]->() RETURN n.name, e.src",
    //         "WITH e AS (SELECT * FROM _default) SELECT e.src_props.name, e.src FROM e",
    //     );
    // }

    // #[test]
    // fn scan_2_hop_with_nodes(){
    //     check_cypher_to_sql(
    //         "MATCH (u)-[e1]->(v)-[e2]->(w) RETURN u.name, e1.src, e2.src, w.name",
    //         "WITH e1 AS (SELECT * FROM _default), e2 AS (SELECT * FROM _default) SELECT e1.src_props.name, e1.src, e2.src, e2.dst_props.name FROM e1 JOIN e2 ON e1.dst = e2.src",
    //     );
    // }

    fn check_cypher_to_sql_layers<LS: IntoIterator<Item = impl AsRef<str>>>(
        query: &str,
        expected: &str,
        layers: LS,
    ) {
        let query = parser::parse_cypher(query).unwrap();
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();
        for layer in layers {
            g.add_edge(0, 0, 0, NO_PROPS, Some(layer.as_ref()))
                .expect("failed to add edge");
        }
        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();
        let sql = cypher_to_sql_with_ctes(&query, &graph);
        assert_eq!(sql.to_string(), expected.to_string());
    }

    fn check_cypher_to_sql(query: &str, expected: &str) {
        check_cypher_to_sql_layers::<[String; 0]>(query, expected, [])
    }

    lazy_static::lazy_static! {
    static ref EDGES: Vec<(u64, u64, i64, f64)> = vec![
            (0, 1, 1, 3.),
            (0, 1, 2, 4.),
            (0, 3, 0, 1.),
            (1, 2, 2, 4.),
            (1, 2, 3, 4.),
            (1, 5, 1, 1.),
            (2, 3, 5, 5.),
            (3, 4, 1, 6.),
            (3, 4, 3, 6.),
            (3, 5, 7, 6.),
            (4, 5, 9, 7.),
        ];

    static ref EDGES2: Vec<(u64, u64, i64, f64, String)> = vec![
            (0, 1, 1, 3., "baa".to_string()),
            (0, 2, 2, 7., "buu".to_string()),
            (2, 3, 1, 9., "xbaa".to_string()),
            (2, 3, 2, 1., "xbaa".to_string()),
            (3, 0, 3, 4., "beea".to_string()),
            (3, 0, 3, 1., "beex".to_string()),
            (4, 1, 5, 5., "baaz".to_string()),
            (4, 5, 1, 6., "bxx".to_string()),
            (5, 6, 3, 6., "mbaa".to_string()),
            (6, 4, 7, 8., "baa".to_string()),
            (6, 4, 9, 7., "bzz".to_string()),
        ];
    }

    //TODO: need better way of testing these, since they run in parallel order of batches is non-deterministic

    #[tokio::test]
    async fn select_table() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher("match ()-[e]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");
    }
    #[tokio::test]
    async fn select_table_filter_weight() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 10, 10);

        let df = run_cypher("match ()-[e {src: 0}]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");

        let df = run_cypher(
            "match ()-[e]->() where e.time >2 and e.weight<7 RETURN *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).expect("failed to print batches");
    }

    #[tokio::test]
    async fn two_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->() return e1.src as start, e1.dst as mid, e2.dst as end",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();

        // TODO: figure out a way to test this
    }

    #[tokio::test]
    async fn three_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->()<-[e3]-() where e2.weight > 5 return *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn five_hops() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 100, 100);

        let df = run_cypher(
            "match ()-[e1]->()-[e2]->()-[e3]->()-[e4]->()-[e5]->() return *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    fn make_graph_with_str_col(graph_dir: impl AsRef<Path>) -> ArrowGraph {
        let graph = Graph::new();

        load_edges_2(&graph, None);

        ArrowGraph::from_graph(&graph, graph_dir).unwrap()
    }

    fn load_edges_2(graph: &Graph, layer: Option<&str>) {
        for (src, dst, t, weight, name) in EDGES2.iter() {
            graph
                .add_edge(
                    *t,
                    *src,
                    *dst,
                    [
                        ("weight", Prop::F64(*weight)),
                        ("name", Prop::Str(name.to_owned().into())),
                    ],
                    layer,
                )
                .unwrap();
        }
    }

    fn load_edges_1(graph: &Graph, layer: Option<&str>) {
        for (src, dst, t, weight) in EDGES.iter() {
            graph
                .add_edge(*t, *src, *dst, [("weight", Prop::F64(*weight))], layer)
                .unwrap();
        }
    }

    #[tokio::test]
    async fn select_contains() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]->() where e.name ends WITH 'z' RETURN e",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_count() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]-() where e.name ends with 'z' return count(e.name)",
            &graph,
        )
        .await
        .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_union_multiple_layers() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_2(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();

        let df = run_cypher(
            "match ()-[e:_default|LAYER1|LAYER2]-() where (e.weight > 3 and e.weight < 5) or e.name starts with 'xb' return e",
            &graph,
        ).await.unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_all_multiple_layers() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_2(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();

        let df = run_cypher("match ()-[e]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        print_batches(&data).expect("failed to print batches");
    }

    #[tokio::test]
    async fn select_all_layers_expand_layer_type() {
        let graph_dir = tempdir().unwrap();
        let g = Graph::new();

        load_edges_1(&g, Some("LAYER1"));
        load_edges_2(&g, Some("LAYER2"));

        let graph = ArrowGraph::from_graph(&g, graph_dir).unwrap();
        let df = run_cypher("match ()-[e]-() return type(e), e", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_count_star() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher("match ()-[e]-() return count(*)", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    #[tokio::test]
    async fn select_contains_limit() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher(
            "match ()-[e]-() where e.name contains 'a' return e limit 2",
            &graph,
        )
        .await
        .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();
    }

    // fn new_record_batch(arrays: Vec<(&str, ArrayRef)>) -> RecordBatch {
    //     let fields: Vec<Field> = arrays
    //         .iter()
    //         .map(|(name, array)| Field::new(name.to_string(), array.data_type().clone(), false))
    //         .collect();

    //     let schema = Schema::new(fields);

    //     let arrays = arrays.into_iter().map(|(_, array)| array).collect();

    //     RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    // }

    // fn make_record_batch(
    //     e_ids: Vec<u64>,
    //     srcs: Vec<u64>,
    //     dsts: Vec<u64>,
    //     times: Vec<i64>,
    //     weights: Vec<f64>,
    // ) -> RecordBatch {
    //     let e_ids_array = UInt64Array::from(e_ids);
    //     let src_array = UInt64Array::from(srcs);
    //     let dst_array = UInt64Array::from(dsts);
    //     let time_array = Int64Array::from(times);
    //     let weight_array = Float64Array::from(weights);

    //     new_record_batch(vec![
    //         ("e_id", Arc::new(e_ids_array)),
    //         ("src", Arc::new(src_array)),
    //         ("dst", Arc::new(dst_array)),
    //         ("time", Arc::new(time_array)),
    //         ("weight", Arc::new(weight_array)),
    //     ])
    // }
}
