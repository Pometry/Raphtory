use std::sync::Arc;

use datafusion::{
    dataframe::DataFrame,
    execution::context::{SQLOptions, SessionContext},
};
use executor::{table_provider::EdgeListTableProvider, ExecError};
use itertools::Itertools;
use parser::ast::*;
use raphtory::{arrow::graph_impl::ArrowGraph, core::Direction};
use sqlparser::ast::{self as sql_ast, GroupByExpr, WildcardAdditionalOptions};

mod executor;
pub mod parser;

pub fn cypher_to_sql(query: &Query) -> sql_ast::Statement {
    sql_ast::Statement::Query(Box::new(sql_ast::Query {
        // WITH (common table expressions, or CTEs)
        with: None,
        // SELECT or UNION / EXCEPT / INTERSECT
        body: parse_select_body(query),
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

fn parse_order_by(query: &Query) -> Vec<sql_ast::OrderByExpr> {
    vec![]
}

fn parse_select_body(query: &Query) -> Box<sql_ast::SetExpr> {
    let mut order_by = vec![];

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
    let binds = query
        .clauses()
        .iter()
        .flat_map(|clause| match clause {
            Clause::Match(m) => m
                .pattern
                .0
                .iter()
                .flat_map(|part| {
                    part.rel_chain
                        .iter()
                        .map(|(rel, _)| rel.name.clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect::<Vec<_>>();
    binds
}

fn parse_tables(query: &Query) -> Vec<sql_ast::TableWithJoins> {
    let m = query
        .clauses()
        .into_iter()
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
    let mut iter = rel_chain.into_iter().peekable();

    let (rel, _) = iter.peek().unwrap(); // FIXME: it will breake for match (n)

    for ((rel1, _), (rel2, _)) in iter.tuple_windows() {
        let (from, to) = match rel1.direction {
            Direction::OUT => ("dst", "src"),
            Direction::IN => ("src", "dst"),
            Direction::BOTH => unimplemented!("both direction not supported"),
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
            relation: sql_ast::TableFactor::Table {
                name: sql_ast::ObjectName(vec![sql_ast::Ident::new(rel_layer(rel2))]),
                alias: Some(sql_ast::TableAlias {
                    name: sql_ast::Ident::new(&rel2.name),
                    columns: vec![],
                }),
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            join_operator: join_op,
        };

        joins.push(join)
    }

    sql_ast::TableWithJoins {
        relation: sql_ast::TableFactor::Table {
            name: sql_ast::ObjectName(vec![sql_ast::Ident::new(rel_layer(rel))]),
            alias: Some(sql_ast::TableAlias {
                name: sql_ast::Ident::new(&rel.name),
                columns: vec![],
            }),
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
        },
        joins,
    }
}

fn rel_layer(pat: &RelPattern) -> String {
    // FIXME: we need to handle multiple rel types
    pat.rel_types
        .first()
        .cloned()
        .unwrap_or("_default".to_string())
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
                    let expr = cypher_to_sql_expr(&ret_i.expr, binds);
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
        .into_iter()
        .filter(|clause| matches!(clause, Clause::Match(_)))
        .flat_map(|clause| match clause {
            Clause::Match(Match {
                pattern: Pattern(pat_parts),
                ..
            }) => pat_parts.into_iter().flat_map(|part| {
                part.rel_chain.iter().flat_map(|(rel, _)| {
                    rel.props.iter().flat_map(|props| {
                        props.into_iter().map(|(prop, expr)| Expr::BinOp {
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
        .map(|expr| cypher_to_sql_expr(&expr, binds));
    let where_exprs = query
        .clauses()
        .into_iter()
        .filter_map(|clause| match clause {
            Clause::Match(m) => m
                .where_clause
                .as_ref()
                .map(|expr| cypher_to_sql_expr(expr, binds)),
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

fn cypher_to_sql_expr(expr: &Expr, binds: &[String]) -> sql_ast::Expr {
    match expr {
        Expr::Var { var_name, attrs } => {
            if attrs.is_empty() {
                sql_ast::Expr::QualifiedWildcard(sql_ast::ObjectName(vec![sql_ast::Ident::new(
                    var_name,
                )]))
            } else {
                sql_ast::Expr::CompoundIdentifier(
                    std::iter::once(sql_ast::Ident::new(var_name))
                        .chain(attrs.iter().map(|attr| sql_ast::Ident::new(attr)))
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
                    .into_iter()
                    .map(|lit| cypher_to_sql_expr(&Expr::Literal(lit.clone()), binds))
                    .collect::<Vec<_>>(),
                expr => unimplemented!("unexpected right hand side of IN operator {:?}", expr),
            };
            sql_ast::Expr::InList {
                expr: Box::new(cypher_to_sql_expr(left, binds)),
                list: sql_list,
                negated: false,
            }
        }
        Expr::BinOp { op, left, right } => sql_ast::Expr::BinaryOp {
            left: Box::new(cypher_to_sql_expr(left, binds)),
            op: cypher_binary_op_to_sql(op),
            right: Box::new(cypher_to_sql_expr(right, binds)),
        },
        Expr::UnaryOp { op, expr } => sql_ast::Expr::UnaryOp {
            op: cypher_unary_op_to_sql(op),
            expr: Box::new(cypher_to_sql_expr(expr, binds)),
        },
        Expr::CountAll => sql_ast::Expr::Function(sql_ast::Function {
            name: sql_ast::ObjectName(vec![sql_ast::Ident::new("COUNT")]),
            args: vec![sql_ast::FunctionArg::Unnamed(
                sql_ast::FunctionArgExpr::Expr(sql_ast::Expr::CompoundIdentifier(
                    vec![binds[0].clone(), "src".to_string()]
                        .into_iter()
                        .map(|s| sql_ast::Ident::new(s))
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
        } => sql_ast::Expr::Function(sql_ast::Function {
            name: sql_ast::ObjectName(vec![sql_ast::Ident::new(name)]),
            args: args
                .iter()
                .map(|arg| {
                    sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(
                        cypher_to_sql_expr(arg, binds),
                    ))
                })
                .collect(),
            over: None,
            distinct: *distinct,
            filter: None,
            null_treatment: None,
            special: false,
            order_by: vec![],
        }),
        _ => unimplemented!("unsupported expression {:?}", expr),
    }
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
            expr: Box::new(cypher_to_sql_expr(left, binds)),
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
    let ctx = SessionContext::new();

    for layer in graph.layer_names() {
        let table = EdgeListTableProvider::new(layer, graph.clone())?;
        ctx.register_table(layer, Arc::new(table))?;
    }
    let query = cypher_to_sql(&query);

    println!("SQL: {:?}", query.to_string());
    println!("SQL AST: {:?}", query);
    let plan = ctx
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            query,
        )))
        .await?;
    let opts = SQLOptions::new();
    opts.verify_plan(&plan)?;
    let df = ctx.execute_logical_plan(plan).await?;

    Ok(df)
}

#[cfg(test)]
mod cyper_2_sql_tests {

    use std::path::Path;

    use super::*;
    use arrow::util::pretty::print_batches;
    use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{Field, Schema};
    use raphtory::{
        core::Prop,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
    };
    use tempfile::tempdir;

    #[test]
    fn select_all() {
        check_cypher_to_sql("MATCH ()-[e]-() RETURN e", "SELECT e.* FROM _default AS e");
    }

    #[test]
    fn select_unnamed() {
        check_cypher_to_sql("MATCH ()-[]-() RETURN *", "SELECT * FROM _default AS r_1");
    }

    #[test]
    fn select_wildcard_name() {
        check_cypher_to_sql("MATCH ()-[e]-() RETURN *", "SELECT * FROM _default AS e");
    }

    #[test]
    fn select_projection() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN e.src, e.weight",
            "SELECT e.src, e.weight FROM _default AS e",
        );
    }

    #[test]
    fn select_wildcard_from_layer() {
        check_cypher_to_sql(
            "MATCH ()-[e:KNOWS]-() RETURN e",
            "SELECT e.* FROM KNOWS AS e",
        );
    }

    #[test]
    fn select_wildcard_count_all() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(*)",
            "SELECT COUNT(*) FROM _default AS e",
        );
    }

    #[test]
    fn select_one_col_count_items() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(e.name)",
            "SELECT COUNT(e.name) FROM _default AS e",
        );
    }

    #[test]
    fn select_one_col_count_items_distinct() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN COUNT(distinct e.name)",
            "SELECT COUNT(DISTINCT e.name) FROM _default AS e",
        );
    }

    #[test]
    fn select_with_limit() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() RETURN e LIMIT 2",
            "SELECT e.* FROM _default AS e LIMIT 2L",
        );
    }

    #[test]
    fn select_where_expr_1() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where e.time > 10 RETURN e",
            "SELECT e.* FROM _default AS e WHERE e.time > 10L",
        );
    }

    #[test]
    fn select_where_str_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where e.name contains 'baa' RETURN e",
            "SELECT e.* FROM _default AS e WHERE e.name LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_str_not_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]-() where NOT e.name contains 'baa' RETURN e",
            "SELECT e.* FROM _default AS e WHERE e.name NOT LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_expr_2() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]-() RETURN e.time, e.type",
            "SELECT e.time, e.type FROM _default AS e WHERE e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_3() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]-() where e.time < 5 RETURN e.time, e.type",
            "SELECT e.time, e.type FROM _default AS e WHERE e.time < 5L AND e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_4_layer() {
        check_cypher_to_sql(
            "MATCH ()-[e :LAYER {time: 7}]-() where e.type = 'one' RETURN e.time, e.type",
            "SELECT e.time, e.type FROM LAYER AS e WHERE e.type = 'one' AND e.time = 7L",
        );
    }

    #[test]
    fn hop_two_times_out() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->() RETURN e1, e2",
            "SELECT e1.*, e2.* FROM _default AS e1 JOIN _default AS e2 ON e1.dst = e2.src",
        );
    }

    #[test]
    fn hop_3_times_out() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->()-[e3]->() RETURN e1, e2, e3",
            "SELECT e1.*, e2.*, e3.* FROM _default AS e1 JOIN _default AS e2 ON e1.dst = e2.src JOIN _default AS e3 ON e2.dst = e3.src",
        );
    }

    #[test]
    fn hop_two_times_in() {
        check_cypher_to_sql(
            "MATCH ()<-[e1]-()<-[e2]-() RETURN e1, e2",
            "SELECT e1.*, e2.* FROM _default AS e1 JOIN _default AS e2 ON e1.src = e2.dst",
        );
    }

    fn check_cypher_to_sql(query: &str, expected: &str) {
        let query = parser::parse_cypher(query).unwrap();
        // println!("{:?}", query);
        let sql = cypher_to_sql(&query);
        assert_eq!(sql.to_string(), expected.to_string());
    }

    lazy_static::lazy_static! {
    static ref EDGES: Vec<(u64, u64, i64, f64)> = vec![
            (0, 1, 1, 3.),
            (0, 1, 2, 4.),
            (1, 2, 2, 4.),
            (1, 2, 3, 4.),
            (2, 3, 5, 5.),
            (3, 4, 1, 6.),
            (3, 4, 3, 6.),
            (3, 4, 7, 6.),
            (4, 5, 9, 7.),
        ];
    }

    #[tokio::test]
    async fn select_table_filter_weight() {
        let graph_dir = tempdir().unwrap();
        let graph = ArrowGraph::make_simple_graph(graph_dir, &EDGES, 10, 10);

        let df = run_cypher("match ()-[e {src: 0}]->() RETURN *", &graph)
            .await
            .unwrap();

        let data = df.collect().await.unwrap();

        let expected = make_record_batch(vec![0, 0], vec![1, 1], vec![1, 2], vec![3., 4.]);

        assert_eq!(&data[0], &expected);

        let df = run_cypher(
            "match ()-[e]->() where e.time >2 and e.weight<7 RETURN *",
            &graph,
        )
        .await
        .unwrap();

        let data = df.collect().await.unwrap();

        let expected = make_record_batch(
            vec![1, 2, 3, 3],
            vec![2, 3, 4, 4],
            vec![3, 5, 3, 7],
            vec![4., 5., 6., 6.],
        );

        assert_eq!(&data[0], &expected);
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

        let df = run_cypher("match ()-[e1]->()-[e2]->()-[e3]->() return *", &graph)
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
        let edges = vec![
            (0u64, 1u64, 1i64, 3., "baa".to_string()),
            (0, 1, 2, 4., "buu".to_string()),
            (1, 2, 2, 4., "xbaa".to_string()),
            (1, 2, 3, 4., "beea".to_string()),
            (2, 3, 5, 5., "baaz".to_string()),
            (3, 4, 1, 6., "bxx".to_string()),
            (3, 4, 3, 6., "mbaa".to_string()),
            (3, 4, 7, 6., "baa".to_string()),
            (4, 5, 9, 7., "bzz".to_string()),
        ];
        let graph = Graph::new();

        for (src, dst, time, weight, name) in edges {
            graph
                .add_edge(
                    time,
                    src,
                    dst,
                    [
                        ("weight", Prop::F64(weight)),
                        ("name", Prop::Str(name.into())),
                    ],
                    None,
                )
                .unwrap();
        }

        ArrowGraph::from_graph(&graph, graph_dir).unwrap()
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
    async fn select_contains_count_star() {
        let graph_dir = tempdir().unwrap();
        let graph = make_graph_with_str_col(graph_dir);

        let df = run_cypher("match ()-[e]-() return count(*)", &graph)
            .await
            .unwrap();
        let data = df.collect().await.unwrap();
        print_batches(&data).unwrap();

        // Query(Query {
        //     with: None,
        //     body: Select(Select {
        //         distinct: None,
        //         top: None,
        //         projection: [UnnamedExpr(Function(Function {
        //             name: ObjectName([Ident {
        //                 value: "COUNT",
        //                 quote_style: None,
        //             }]),
        //             args: [Unnamed(Wildcard)],
        //             filter: None,
        //             null_treatment: None,
        //             over: None,
        //             distinct: false,
        //             special: false,
        //             order_by: [],
        //         }))],
        //         into: None,
        //         from: [TableWithJoins {
        //             relation: Table {
        //                 name: ObjectName([Ident {
        //                     value: "_default",
        //                     quote_style: None,
        //                 }]),
        //                 alias: Some(TableAlias {
        //                     name: Ident {
        //                         value: "e",
        //                         quote_style: None,
        //                     },
        //                     columns: [],
        //                 }),
        //                 args: None,
        //                 with_hints: [],
        //                 version: None,
        //                 partitions: [],
        //             },
        //             joins: [],
        //         }],
        //         lateral_views: [],
        //         selection: None,
        //         group_by: Expressions([]),
        //         cluster_by: [],
        //         distribute_by: [],
        //         sort_by: [],
        //         having: None,
        //         named_window: [],
        //         qualify: None,
        //     }),
        //     order_by: [],
        //     limit: None,
        //     limit_by: [],
        //     offset: None,
        //     fetch: None,
        //     locks: [],
        //     for_clause: None,
        // });
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

    fn new_record_batch(arrays: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = arrays
            .iter()
            .map(|(name, array)| Field::new(name.to_string(), array.data_type().clone(), false))
            .collect();

        let schema = Schema::new(fields);

        let arrays = arrays.into_iter().map(|(_, array)| array).collect();

        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn make_record_batch(
        srcs: Vec<u64>,
        dsts: Vec<u64>,
        times: Vec<i64>,
        weights: Vec<f64>,
    ) -> RecordBatch {
        let src_array = UInt64Array::from(srcs);
        let dst_array = UInt64Array::from(dsts);
        let time_array = Int64Array::from(times);
        let weight_array = Float64Array::from(weights);

        new_record_batch(vec![
            ("src", Arc::new(src_array)),
            ("dst", Arc::new(dst_array)),
            ("time", Arc::new(time_array)),
            ("weight", Arc::new(weight_array)),
        ])
    }
}
