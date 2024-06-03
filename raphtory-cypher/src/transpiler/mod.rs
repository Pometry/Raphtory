use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::parser::ast::*;

use arrow_schema::{Fields, Schema};

use itertools::Itertools;
use raphtory::{
    arrow::graph_impl::DiskGraph,
    core::{
        entities::{edges::edge_ref::Dir, VID},
        Direction,
    },
    db::{api::properties::internal::ConstPropertiesOps, graph::node::NodeView},
    prelude::*,
};
use sqlparser::ast::{
    self as sql_ast, GroupByExpr, OrderByExpr, SetExpr, TableAlias, WildcardAdditionalOptions, With,
};

mod exprs;

pub fn to_sql(query: Query, graph: &DiskGraph) -> sql_ast::Statement {
    let query = bind_unbound_pattern_filters(query);
    let query = unbind_unused_binds(query);

    let rel_binds = rel_names(&query);

    let node_binds = query
        .node_patterns()
        .map(|node_pat| node_pat.name.clone())
        .collect::<Vec<_>>();

    let with = parse_rels_to_ctes(&query, graph);
    sql_ast::Statement::Query(Box::new(sql_ast::Query {
        // WITH (common table expressions, or CTEs)
        with: Some(with),
        // SELECT or UNION / EXCEPT / INTERSECT
        body: parse_select_body(&query, graph, &rel_binds, &node_binds),
        // ORDER BY
        order_by: parse_order_by(&query, &rel_binds, &node_binds),
        // `LIMIT { <N> | ALL }`
        limit: exprs::parse_limit(&query),

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

fn bind_unbound_pattern_filters(mut query: Query) -> Query {
    let mut c = max_bind_count(&query);

    for node_pat in query.node_patterns_mut() {
        c += 1;
        if node_pat.props.is_some() {
            node_pat.name = format!("a_{}", c);
        }
    }
    query
}

fn unbind_unused_binds(mut query: Query) -> Query {
    // find the max bound N from all nodes named n_<N>
    let max_n = max_bind_count(&query);

    let node_binds = query
        .node_patterns()
        .map(|node_pat| node_pat.name.clone())
        .filter(|name| is_bound_str(name))
        .collect::<HashSet<_>>();

    let nodes_in_return = query
        .clauses()
        .iter()
        .flat_map(|clause| match clause {
            Clause::Return(Return { items, .. }) => items
                .iter()
                .flat_map(|item| item.expr.binds())
                .filter(|return_bound| node_binds.contains(return_bound))
                .collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect::<HashSet<_>>();

    let nodes_in_where = query
        .clauses()
        .iter()
        .filter_map(|clause| match clause {
            Clause::Match(m) => m.where_clause.as_ref(),
            _ => None,
        })
        .flat_map(|expr| expr.binds())
        .filter(|where_bound| node_binds.contains(where_bound))
        .collect::<HashSet<_>>();

    let nodes_in_order_by = query
        .clauses()
        .iter()
        .flat_map(|clause| match clause {
            Clause::Return(Return {
                order_by: Some(items),
                ..
            }) => items
                .exprs
                .iter()
                .flat_map(|(expr, _)| expr.binds())
                .filter(|order_by_bound| node_binds.contains(order_by_bound))
                .collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect::<HashSet<_>>();

    let nodes_with_pattern_props = query
        .node_patterns()
        .filter(|node_pat| node_pat.props.is_some())
        .map(|node_pat| node_pat.name.clone())
        .collect::<HashSet<_>>();

    // unbind all nodes not in return
    query.node_patterns_mut().fold(
        (max_n, HashMap::new()),
        |(c, mut bind_table), node_pattern| {
            if is_bound_str(&node_pattern.name)
                && !nodes_in_return.contains(&node_pattern.name)
                && !nodes_in_where.contains(&node_pattern.name)
                && !nodes_with_pattern_props.contains(&node_pattern.name)
                && !nodes_in_order_by.contains(&node_pattern.name)
            {
                match bind_table.entry(node_pattern.name.clone()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let new_name = format!("n_{}", c + 1);
                        entry.insert(new_name.clone());

                        node_pattern.name = new_name;
                        (c + 1, bind_table)
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        node_pattern.name = entry.get().clone();
                        (c, bind_table)
                    }
                }
            } else {
                (c, bind_table)
            }
        },
    );

    query
}

fn max_bind_count(query: &Query) -> usize {
    query
        .node_patterns()
        .map(|node_pat| {
            if let Some(n) = node_pat.name.strip_prefix("n_") {
                n.parse::<usize>().ok()
            } else {
                None
            }
        })
        .flatten()
        .max()
        .unwrap_or(0)
}

fn parse_order_by(query: &Query, rel_binds: &[String], node_binds: &[String]) -> Vec<OrderByExpr> {
    query
        .clauses()
        .into_iter()
        .flat_map(|clause| match clause {
            Clause::Return(Return {
                order_by: Some(items),
                ..
            }) => items
                .exprs
                .iter()
                .map(|(expr, asc)| {
                    let sql_expr = cypher_to_sql_expr(expr, rel_binds, node_binds, false);
                    OrderByExpr {
                        expr: sql_expr,
                        asc: *asc,
                        nulls_first: None,
                    }
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}

fn scan_edges_as_sql_cte(
    layer_names: &[impl AsRef<str>],
    name: &impl AsRef<str>,
    graph: &DiskGraph,
) -> sql_ast::Cte {
    // fetch and merge the schemas

    let schemas = layer_names
        .iter()
        .filter_map(|layer| graph.as_ref().find_layer_id(layer.as_ref()))
        .filter_map(|layer_id| full_layer_fields(graph, layer_id))
        .map(Schema::new);

    // this is the schema that all layers must match, any missing columns will be filled with NULLs
    let schema = Schema::try_merge(schemas).expect("failed to merge schemas");

    let union_query = layer_names
        .iter()
        .map(|layer| {
            select_scan_query(
                layer.as_ref(),
                graph,
                Some(&schema).filter(|_| layer_names.len() > 1), // skip expanding the schema if there is only one layer
            )
        })
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
        // materialized: None,
    }
}

// TODO: this needs to match the schema from EdgeListTableProvider
fn full_layer_fields(graph: &DiskGraph, layer_id: usize) -> Option<Fields> {
    let dt = graph.as_ref().layer(layer_id).edges_props_data_type();
    let arr_dt: arrow_schema::DataType = dt.clone().into();
    match arr_dt {
        arrow_schema::DataType::Struct(fields) => {
            let mut all_fields = vec![];
            all_fields.push(Arc::new(arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::UInt64,
                false,
            )));
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
        with: None,
        body: Box::new(SetExpr::SetOperation {
            op: sql_ast::SetOperator::Union,
            set_quantifier: sql_ast::SetQuantifier::All,
            left: q1.body,
            right: q2.body,
        }),
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
    })
}

fn select_scan_query(
    layer_name: &str,
    graph: &DiskGraph,
    total_schema: Option<&Schema>,
) -> (usize, Box<sql_ast::Query>) {
    let layer_id = graph
        .as_ref()
        .find_layer_id(layer_name)
        .expect("layer not found");
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
        select_query_with_projection(projection, layer_name),
    )
}

fn select_query_with_projection(
    projection: Vec<sql_ast::SelectItem>,
    from_name: &str,
) -> Box<sql_ast::Query> {
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
                relation: table_from_name(from_name),
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
            // value_table_mode: None,
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
    })
}

fn parse_rels_to_ctes(query: &Query, graph: &DiskGraph) -> With {
    // each rel can become a CTE
    // inside the cte
    // if the pattern has no layers -[e]- and the graph has one layer then we just select * from the layer
    // if the pattern has one layer -[e:A]- then we just select * from A
    // if the pattern has no layers -[e]-> and the graph has multiple layers then we UNION ALL select * from each layer
    // if the pattern has multiple layers -[e:A:B]-> then we select * from A UNION ALL select * from B
    // we name these CTEs with the rel name
    // in each CTE where we UNION ALL we must merge the schemas and add missing columns with NULLs if they don't match.

    let mut cte_tables = vec![];

    let layer_names = graph.as_ref().layer_names();

    for rel in query.rel_patterns() {
        // rewrite the conditions in a nicer way
        if rel.rel_types.is_empty() {
            // select * from layer
            cte_tables.push(scan_edges_as_sql_cte(layer_names, &rel.name, graph))
        } else {
            // UNION ALL for all the layers of the relation pattern
            cte_tables.push(scan_edges_as_sql_cte(&rel.rel_types, &rel.name, graph))
        }
    }

    let mut seen: HashSet<String> = HashSet::new();

    for node in all_bound_nodes(query) {
        if !seen.contains(&node.name) {
            let cte = node_scan_cte(node);
            seen.insert(node.name.clone());
            cte_tables.push(cte)
        }
    }

    if cte_tables.is_empty() {
        // there are no edges and no bound nodes, this is probably a match (n) return(*) or match () statement
        if let Some(node) = query.node_patterns().next() {
            let cte = node_scan_cte(node);
            cte_tables.push(cte)
        }
    }

    With {
        recursive: false,
        cte_tables,
    }
}

fn node_scan_cte(node: &NodePattern) -> sql_ast::Cte {
    sql_ast::Cte {
        alias: TableAlias {
            name: sql_ast::Ident::new(&node.name),
            columns: vec![],
        },
        query: select_query_with_projection(
            vec![sql_ast::SelectItem::Wildcard(
                WildcardAdditionalOptions::default(),
            )],
            "nodes",
        ),
        from: None,
        // materialized: None,
    }
}

fn parse_select_body(
    query: &Query,
    _graph: &DiskGraph,
    rel_binds: &[String],
    node_binds: &[String],
) -> Box<SetExpr> {
    let order_by = vec![];

    let (from_tables, rel_uniqueness_filters) = parse_tables_2(query);

    Box::new(SetExpr::Select(Box::new(sql_ast::Select {
        distinct: None,
        // MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
        top: None,
        // projection expressions
        projection: sql_projection(query, &rel_binds, &node_binds),
        // INTO
        into: None,
        // FROM
        from: from_tables,
        // LATERAL VIEWs
        lateral_views: vec![],
        // WHERE
        selection: where_expr(query, rel_uniqueness_filters, &rel_binds, &node_binds),
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
        // value_table_mode: None,
    })))
}

fn rel_names(query: &Query) -> Vec<String> {
    query.rel_patterns().map(|rel| rel.name.clone()).collect()
}

fn all_bound_nodes(query: &Query) -> impl Iterator<Item = &NodePattern> + '_ {
    query.node_patterns().filter(|&node_pat| is_bound(node_pat))
}

fn parse_tables_2(query: &Query) -> (Vec<sql_ast::TableWithJoins>, Vec<Expr>) {
    let mut joins = vec![];
    let graph = query_to_graph(query);

    let first = query
        .node_patterns()
        .next()
        .expect("unexpected! no node patterns");
    // walk the graph in depth first fashion and add the nodes as joins
    let edge_counts = graph
        .nodes()
        .into_iter()
        .filter(|node| node.get_const_prop(0).is_some())
        .count();

    if edge_counts > 0 {
        let first_edge = query
            .rel_patterns()
            .next()
            .expect("unexpected! no edge patterns");

        let mut seen: HashSet<VID> = HashSet::new();

        let mut stack = vec![graph
            .node(first_edge.name.as_str())
            .expect("edge not found")];

        let mut last_edge_dir = first_edge.direction;
        let mut last_edge: Option<NodeView<Graph>> = None;

        let mut additional_filters = vec![];

        while !stack.is_empty() {
            let parent = stack.pop().unwrap();

            let mut child_edges = vec![];

            for n in parent
                .neighbours()
                .iter()
                .filter(|n| !seen.contains(&n.node))
            {
                let edge = graph
                    .edge(&parent.name(), &n.name())
                    .or_else(|| graph.edge(&n.name(), &parent.name()))
                    .expect("surprisingly edge not found!");

                let dir = if (edge.src().name(), edge.dst().name()) == (parent.name(), n.name()) {
                    Dir::Out
                } else if (edge.src().name(), edge.dst().name()) == (n.name(), parent.name()) {
                    Dir::Into
                } else {
                    panic!("unexpected edge direction");
                };

                if let Some(Prop::Bool(out)) = n.get_const_prop(0) {
                    // this is an edge

                    let current_edge_dir = if out { Direction::OUT } else { Direction::IN };
                    if !is_bound_str(&parent.name()) {
                        if let Some(ref last_edge) = last_edge {
                            let (from, to) = match (last_edge_dir, current_edge_dir) {
                                (Direction::OUT, Direction::OUT) => ("dst", "src"),
                                (Direction::OUT, Direction::IN) => ("dst", "dst"),
                                (Direction::IN, Direction::OUT) => ("src", "src"),
                                (Direction::IN, Direction::IN) => ("src", "dst"),
                                _ => unimplemented!("both direction not supported"),
                            };

                            joins.push(make_sql_join(&last_edge.name(), from, &n.name(), to));
                        }
                    } else {
                        // parent is node
                        match dir {
                            Dir::Out => {
                                joins.push(make_sql_join(&parent.name(), "id", &n.name(), "src"))
                            }
                            Dir::Into => {
                                joins.push(make_sql_join(&parent.name(), "id", &n.name(), "dst"))
                            }
                        }
                    }

                    if let Some(ref last_edge) = last_edge {
                        additional_filters.push(unique_edge_filter(&last_edge.name(), &n.name()));
                    }
                    child_edges.push(n.name());
                    last_edge_dir = current_edge_dir;
                } else {
                    // node with edge parent
                    if is_bound_str(&n.name()) {
                        match dir {
                            Dir::Out => {
                                joins.push(make_sql_join(&parent.name(), "dst", &n.name(), "id"))
                            }
                            Dir::Into => {
                                joins.push(make_sql_join(&parent.name(), "src", &n.name(), "id"))
                            }
                        }
                    }
                }

                stack.push(n);
            }

            let unique_edges = child_edges.iter().combinations(2).map(|perm_vec| {
                let (a, b) = (perm_vec[0], perm_vec[1]);
                unique_edge_filter(a, b)
            });

            additional_filters.extend(unique_edges);

            if parent.get_const_prop(0).is_some() {
                last_edge = Some(parent.clone());
            }
            seen.insert(parent.node);
        }

        let table = sql_ast::TableWithJoins {
            relation: table_from_name(&first_edge.name),
            joins,
        };
        (vec![table], additional_filters)
    } else {
        // matching only one node
        let node_table = sql_ast::TableWithJoins {
            relation: table_from_name(&first.name),
            joins,
        };
        (vec![node_table], vec![])
    }
}

fn unique_edge_filter(a: &str, b: &str) -> Expr {
    Expr::or(
        Expr::and(
            Expr::neq(Expr::var(a, ["id"]), Expr::var(b, ["id"])),
            Expr::eq(Expr::var(a, ["layer_id"]), Expr::var(b, ["layer_id"])),
        ),
        Expr::neq(Expr::var(a, ["layer_id"]), Expr::var(b, ["layer_id"])),
    )
}

/// walk the path parts of the query and build a graph from every node pattern and edge pattern
fn query_to_graph(query: &Query) -> Graph {
    let pattern_parts = query
        .clauses()
        .iter()
        .filter_map(|clause| match clause {
            Clause::Match(ma) => Some(ma),
            _ => None,
        })
        .flat_map(|clause| clause.pattern.0.iter());

    let graph = Graph::new();

    for PatternPart {
        node, rel_chain, ..
    } in pattern_parts
    {
        graph
            .add_node(0, node.name.as_str(), NO_PROPS, None)
            .expect("failed to add node");
        let mut last_node = node;

        for (
            RelPattern {
                name: edge,
                direction,
                ..
            },
            np @ NodePattern { name: u, .. },
        ) in rel_chain
        {
            match direction {
                Direction::OUT => {
                    graph
                        .add_edge(0, last_node.name.as_str(), edge.as_str(), NO_PROPS, None)
                        .expect("failed to add edge");

                    graph
                        .add_edge(0, edge.as_str(), u.as_str(), NO_PROPS, None)
                        .expect("failed to add node");
                }

                Direction::IN => {
                    graph
                        .add_edge(0, u.as_str(), edge.as_str(), NO_PROPS, None)
                        .expect("failed to add edge");

                    graph
                        .add_edge(0, edge.as_str(), last_node.name.as_str(), NO_PROPS, None)
                        .expect("failed to add node");
                }
                Direction::BOTH => unimplemented!("BOTH direction not supported"),
            }

            assert_ne!(direction, &Direction::BOTH, "BOTH direction not supported");
            let direction_flag = direction == &Direction::OUT;

            let edge_node = graph.node(edge.as_str()).expect("edge not found");
            edge_node
                .add_constant_properties([("direction", Prop::Bool(direction_flag))])
                .expect("failed to add properties");

            last_node = np;
        }
    }

    graph
}

fn is_bound(node: &NodePattern) -> bool {
    is_bound_str(&node.name)
}

fn is_bound_str(node: &str) -> bool {
    !node.starts_with("n_")
}

fn make_sql_join(
    left_table: &str,
    left_id: &str,
    right_table: &str,
    right_id: &str,
) -> sql_ast::Join {
    sql_ast::Join {
        relation: table_from_name(right_table),
        join_operator: sql_ast::JoinOperator::Inner(sql_ast::JoinConstraint::On(
            sql_ast::Expr::BinaryOp {
                left: Box::new(sql_ast::Expr::CompoundIdentifier(vec![
                    sql_ast::Ident::new(left_table),
                    sql_ast::Ident::new(left_id),
                ])),
                op: sql_ast::BinaryOperator::Eq,
                right: Box::new(sql_ast::Expr::CompoundIdentifier(vec![
                    sql_ast::Ident::new(right_table),
                    sql_ast::Ident::new(right_id),
                ])),
            },
        )),
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

fn sql_projection(
    query: &Query,
    rel_binds: &[String],
    node_binds: &[String],
) -> Vec<sql_ast::SelectItem> {
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
                    let expr = cypher_to_sql_expr(&ret_i.expr, rel_binds, node_binds, true);
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

fn where_expr(
    query: &Query,
    rel_uniqueness_filters: Vec<Expr>,
    rel_binds: &[String],
    node_binds: &[String],
) -> Option<sql_ast::Expr> {
    let rel_uniqueness_filters = rel_uniqueness_filters
        .iter()
        .map(|expr| cypher_to_sql_expr(expr, rel_binds, node_binds, false));

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
                        props.iter().map(|(prop, expr)| {
                            Expr::eq(
                                Expr::Var {
                                    var_name: rel.name.clone(),
                                    attrs: vec![prop.clone()],
                                },
                                expr.clone(),
                            )
                        })
                    })
                })
            }),
            _ => unreachable!(),
        })
        .map(|expr| cypher_to_sql_expr(&expr, rel_binds, node_binds, false));

    let node_exprs = query
        .node_patterns()
        .flat_map(|node_pat| {
            node_pat.props.iter().flat_map(|props| {
                props.iter().map(|(prop, expr)| {
                    Expr::eq(
                        Expr::Var {
                            var_name: node_pat.name.clone(),
                            attrs: vec![prop.clone()],
                        },
                        expr.clone(),
                    )
                })
            })
        })
        .map(|expr| cypher_to_sql_expr(&expr, rel_binds, node_binds, false));

    let where_exprs = query.clauses().iter().filter_map(|clause| match clause {
        Clause::Match(m) => m
            .where_clause
            .as_ref()
            .map(|expr| cypher_to_sql_expr(expr, rel_binds, node_binds, false)),
        _ => None,
    });

    where_exprs
        .chain(rel_exprs)
        .chain(rel_uniqueness_filters)
        .chain(node_exprs)
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

fn cypher_to_sql_expr(
    expr: &Expr,
    rel_binds: &[String],
    node_binds: &[String],
    allow_wildcard_edges: bool,
) -> sql_ast::Expr {
    match expr {
        Expr::Var { var_name, attrs } => {
            if attrs.is_empty() {
                if allow_wildcard_edges {
                    sql_ast::Expr::QualifiedWildcard(sql_ast::ObjectName(vec![
                        sql_ast::Ident::new(var_name),
                    ]))
                } else {
                    // this makes sure that non-wildcard edges are selected with their id when passed down to functions
                    if rel_binds.contains(&var_name) || node_binds.contains(&var_name) {
                        sql_ast::Expr::CompoundIdentifier(vec![
                            sql_ast::Ident::new(var_name),
                            sql_ast::Ident::new("id"),
                        ])
                    } else {
                        sql_ast::Expr::Identifier(sql_ast::Ident::new(var_name))
                    }
                }
            } else {
                if !attrs.is_empty() {
                    sql_ast::Expr::CompoundIdentifier(
                        std::iter::once(sql_ast::Ident::new(var_name))
                            .chain(attrs.iter().map(sql_ast::Ident::new))
                            .collect(),
                    )
                } else {
                    sql_ast::Expr::Identifier(sql_ast::Ident::new(var_name))
                }
            }
        }
        // contains
        Expr::BinOp {
            op: BinOpType::Contains,
            left,
            right,
        } => sql_like(right, left, |s| format!("%{}%", s), rel_binds, node_binds),
        // starts_with
        Expr::BinOp {
            op: BinOpType::StartsWith,
            left,
            right,
        } => sql_like(right, left, |s| format!("{}%", s), rel_binds, node_binds),
        // ends_with
        Expr::BinOp {
            op: BinOpType::EndsWith,
            left,
            right,
        } => sql_like(right, left, |s| format!("%{}", s), rel_binds, node_binds),
        // in
        Expr::BinOp {
            op: BinOpType::In,
            left,
            right,
        } => {
            let sql_list = match right.as_ref() {
                Expr::Literal(Literal::List(exprs)) => exprs
                    .iter()
                    .map(|lit| {
                        cypher_to_sql_expr(
                            &Expr::Literal(lit.clone()),
                            rel_binds,
                            node_binds,
                            false,
                        )
                    })
                    .collect::<Vec<_>>(),
                expr => unimplemented!("unexpected right hand side of IN operator {:?}", expr),
            };
            sql_ast::Expr::InList {
                expr: Box::new(cypher_to_sql_expr(left, rel_binds, node_binds, false)),
                list: sql_list,
                negated: false,
            }
        }
        Expr::BinOp { op, left, right } => sql_ast::Expr::BinaryOp {
            left: Box::new(cypher_to_sql_expr(left, rel_binds, node_binds, false)),
            op: cypher_binary_op_to_sql(op),
            right: Box::new(cypher_to_sql_expr(right, rel_binds, node_binds, false)),
        },
        Expr::UnaryOp { op, expr } => sql_ast::Expr::UnaryOp {
            op: cypher_unary_op_to_sql(op),
            expr: Box::new(cypher_to_sql_expr(expr, rel_binds, node_binds, false)),
        },
        Expr::CountAll => {
            if let Some(bind) = rel_binds.first() {
                sql_count_all(bind, "id")
            } else if let Some(bind) = node_binds.first() {
                sql_count_all(bind, "id")
            } else {
                unimplemented!("can't find matching bind for count all")
            }
        }

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
                    Expr::Var { var_name, .. } => sql_function_ast(
                        name,
                        &vec![Expr::var(var_name, vec!["layer_id"])],
                        rel_binds,
                        node_binds,
                        distinct,
                    ),
                    expr => unimplemented!(
                        "type function must have a variable as argument, found {:?}",
                        expr
                    ),
                }
            } else {
                sql_function_ast(name, args, rel_binds, node_binds, distinct)
            }
        }
        Expr::Nested(expr) => sql_ast::Expr::Nested(Box::new(cypher_to_sql_expr(
            expr,
            rel_binds,
            node_binds,
            allow_wildcard_edges & true,
        ))),
        _ => unimplemented!("unsupported expression {:?}", expr),
    }
}

fn sql_count_all(table: &str, attr: &str) -> sql_ast::Expr {
    sql_ast::Expr::Function(sql_ast::Function {
        name: sql_ast::ObjectName(vec![sql_ast::Ident::new("COUNT")]),
        args: vec![sql_ast::FunctionArg::Unnamed(
            sql_ast::FunctionArgExpr::Expr(sql_ast::Expr::CompoundIdentifier(
                vec![table.to_string(), attr.to_string()]
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
    })
}

fn sql_function_ast(
    name: &String,
    args: &Vec<Expr>,
    rel_binds: &[String],
    node_binds: &[String],
    distinct: &bool,
) -> sql_ast::Expr {
    sql_ast::Expr::Function(sql_ast::Function {
        name: sql_ast::ObjectName(vec![sql_ast::Ident::new(name)]),
        args: args
            .iter()
            .map(|arg| {
                sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(cypher_to_sql_expr(
                    arg, rel_binds, node_binds, false,
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

fn sql_like(
    right: &Expr,
    left: &Expr,
    pattern: impl Fn(&String) -> String,
    rel_binds: &[String],
    node_binds: &[String],
) -> sql_ast::Expr {
    match right {
        Expr::Literal(Literal::Str(s)) => sql_ast::Expr::Like {
            negated: false,
            expr: Box::new(cypher_to_sql_expr(left, rel_binds, node_binds, false)),
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

#[cfg(test)]
mod test {

    use crate::{parser, transpiler};

    use super::*;
    use raphtory::{
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use tempfile::tempdir;

    use pretty_assertions::assert_eq;

    #[test]
    fn count_all_nodes() {
        check_cypher_to_sql(
            "MATCH (n) RETURN COUNT(n)",
            "WITH n AS (SELECT * FROM nodes) SELECT COUNT(n.id) FROM n",
        );

        check_cypher_to_sql(
            "MATCH () RETURN COUNT(*)",
            "WITH n_0 AS (SELECT * FROM nodes) SELECT COUNT(n_0.id) FROM n_0",
        );
    }

    #[test]
    fn select_all_edges() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e",
        );
    }

    #[test]
    fn select_all_edges_order_by() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN e ORDER BY e.time",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e ORDER BY e.time",
        );
    }

    #[test]
    fn select_all_edges_order_by_nodes() {
        check_cypher_to_sql(
            "MATCH (m)-[e]->(n) RETURN e ORDER BY m.name ASC, n.name DESC",
            "WITH \
            e AS (SELECT * FROM _default), \
            m AS (SELECT * FROM nodes), \
            n AS (SELECT * FROM nodes) \
            SELECT e.* FROM e \
            JOIN m ON e.src = m.id \
            JOIN n ON e.dst = n.id \
            ORDER BY m.name ASC, n.name DESC",
        );
    }

    #[test]
    fn select_unused_node_binds() {
        check_cypher_to_sql(
            "MATCH (n)-[e]->(m) RETURN COUNT(e)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.id) FROM e",
        );
    }

    #[test]
    fn call_id_on_node_binds() {
        check_cypher_to_sql(
            "MATCH (n)-[e]->(m) WHERE n <> m RETURN COUNT(e)",
            "WITH \
             e AS (SELECT * FROM _default), \
             n AS (SELECT * FROM nodes), \
             m AS (SELECT * FROM nodes) \
             SELECT COUNT(e.id) \
             FROM e \
             JOIN n ON e.src = n.id \
             JOIN m ON e.dst = m.id \
             WHERE n.id <> m.id",
        )
    }

    #[test]
    fn select_edges_count() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN COUNT(e)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.id) FROM e",
        );
    }

    #[test]
    fn select_unnamed() {
        check_cypher_to_sql(
            "MATCH ()-[]->() RETURN *",
            "WITH r_1 AS (SELECT * FROM _default) SELECT * FROM r_1",
        );
    }

    #[test]
    fn select_wildcard_name() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN *",
            "WITH e AS (SELECT * FROM _default) SELECT * FROM e",
        );
    }

    #[test]
    fn select_projection() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN e.src, e.weight",
            "WITH e AS (SELECT * FROM _default) SELECT e.src, e.weight FROM e",
        );
    }

    #[test]
    fn select_wildcard_from_layer() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e:KNOWS]->() RETURN e",
            "WITH e AS (SELECT * FROM KNOWS) SELECT e.* FROM e",
            ["KNOWS"],
        );
    }

    #[test]
    fn select_wildcard_count_all() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN COUNT(*)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.id) FROM e",
        );
    }

    #[test]
    fn select_count_edges_ignore_unbound_with() {
        check_cypher_to_sql_layers(
            "MATCH (v)-[e]->(u) RETURN COUNT(e)",
            "WITH \
             e AS (\
             SELECT id, layer_id, src, dst, time FROM _default \
             UNION ALL \
             SELECT id, layer_id, src, dst, time FROM L1 \
             UNION ALL \
             SELECT id, layer_id, src, dst, time FROM L2) \
             SELECT COUNT(e.id) FROM e",
            ["L1", "L2"],
        )
    }

    #[test]
    fn select_one_col_count_items() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN COUNT(e.name)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(e.name) FROM e",
        );
    }

    #[test]
    fn select_one_col_count_items_distinct() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN COUNT(distinct e.name)",
            "WITH e AS (SELECT * FROM _default) SELECT COUNT(DISTINCT e.name) FROM e",
        );
    }

    #[test]
    fn select_with_limit() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() RETURN e LIMIT 2",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e LIMIT 2L",
        );
    }

    #[test]
    fn select_where_expr_1() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() where e.time > 10 RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE e.time > 10L",
        );
    }

    #[test]
    fn select_edge_with_type() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e]->() where e.time > 10 RETURN e,type(e)",
            "WITH \
             e AS (\
             SELECT id, layer_id, src, dst, time FROM _default \
             UNION ALL \
             SELECT id, layer_id, src, dst, time FROM L1 \
             UNION ALL SELECT id, layer_id, src, dst, time FROM L2) \
             SELECT e.*, type(e.layer_id) FROM e WHERE e.time > 10L",
            ["L1", "L2"],
        );
    }

    #[test]
    fn select_where_str_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() where e.name contains 'baa' RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE e.name LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_str_not_contains() {
        check_cypher_to_sql(
            "MATCH ()-[e]->() where NOT e.name contains 'baa' RETURN e",
            "WITH e AS (SELECT * FROM _default) SELECT e.* FROM e WHERE NOT e.name LIKE '%baa%'",
        );
    }

    #[test]
    fn select_where_expr_2() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]->() RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM _default) SELECT e.time, e.type FROM e WHERE e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_3() {
        check_cypher_to_sql(
            "MATCH ()-[e {type: 'one'}]->() where e.time < 5 RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM _default) SELECT e.time, e.type FROM e WHERE e.time < 5L AND e.type = 'one'",
        );
    }

    #[test]
    fn select_where_expr_4_layer() {
        check_cypher_to_sql_layers(
            "MATCH ()-[e :LAYER {time: 7}]->() where e.type = 'one' RETURN e.time, e.type",
            "WITH e AS (SELECT * FROM LAYER) SELECT e.time, e.type FROM e WHERE e.type = 'one' AND e.time = 7L",
            ["LAYER"]
        );
    }

    #[test]
    fn hop_two_times_out() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->() RETURN e1, e2",
            "WITH \
             e1 AS (SELECT * FROM _default), \
             e2 AS (SELECT * FROM _default) \
             SELECT e1.*, e2.* \
             FROM e1 \
             JOIN e2 ON e1.dst = e2.src \
             WHERE e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id",
        );
    }

    #[test]
    fn hop_once_bound_node() {
        // outbound
        check_cypher_to_sql(
            "MATCH (n)-[e]->() RETURN n.name, e",
            "WITH e AS (SELECT * FROM _default), n AS (SELECT * FROM nodes) SELECT n.name, e.* FROM e JOIN n ON e.src = n.id",
        );

        // inbound
        check_cypher_to_sql(
            "MATCH (n)<-[e]-() RETURN n.name, e",
            "WITH e AS (SELECT * FROM _default), n AS (SELECT * FROM nodes) SELECT n.name, e.* FROM e JOIN n ON e.dst = n.id",
        );
    }

    #[test]
    fn two_hops_out_with_nodes() {
        check_cypher_to_sql(
            "MATCH (n1)-[e1]->(n2)-[e2]->(n3) RETURN n1.name, n2.name, n3.name",
            "WITH \
             e1 AS (SELECT * FROM _default), \
             e2 AS (SELECT * FROM _default), \
             n1 AS (SELECT * FROM nodes), \
             n2 AS (SELECT * FROM nodes), \
             n3 AS (SELECT * FROM nodes) \
             SELECT n1.name, n2.name, n3.name \
             FROM e1 \
             JOIN n1 ON e1.src = n1.id \
             JOIN n2 ON e1.dst = n2.id \
             JOIN e2 ON n2.id = e2.src \
             JOIN n3 ON e2.dst = n3.id \
             WHERE e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id",
        );
    }

    #[test]
    fn two_hops_on_separate_parts() {
        check_cypher_to_sql(
            "MATCH (n1)-[e1]->(n2), (n2)-[e2]->(n3) RETURN n1.name, n2.name, n3.name",
            "WITH \
             e1 AS (SELECT * FROM _default), \
             e2 AS (SELECT * FROM _default), \
             n1 AS (SELECT * FROM nodes), \
             n2 AS (SELECT * FROM nodes), \
             n3 AS (SELECT * FROM nodes) \
             SELECT n1.name, n2.name, n3.name \
             FROM e1 \
             JOIN n1 ON e1.src = n1.id \
             JOIN n2 ON e1.dst = n2.id \
             JOIN e2 ON n2.id = e2.src \
             JOIN n3 ON e2.dst = n3.id \
             WHERE e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id",
        );
    }

    #[test]
    fn test_fork_in_path() {
        let expected = "WITH \
        e3 AS (SELECT * FROM _default), \
        e1 AS (SELECT * FROM _default), \
        e2 AS (SELECT * FROM _default), \
        b AS (SELECT * FROM nodes) \
        SELECT e1.src, e1.id, b.id, e2.id, e2.dst, e3.id, e3.dst \
        FROM e3 \
        JOIN b ON e3.src = b.id \
        JOIN e1 ON b.id = e1.dst \
        JOIN e2 ON b.id = e2.src \
        WHERE \
        e3.id <> e1.id AND \
        e3.layer_id = e1.layer_id OR e3.layer_id <> e1.layer_id \
        AND e3.id <> e2.id \
        AND e3.layer_id = e2.layer_id OR e3.layer_id <> e2.layer_id \
        AND e1.id <> e2.id \
        AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id";
        check_cypher_to_sql(
            "match (b)-[e3]->(), ()-[e1]->(b)-[e2]->() RETURN e1.src, e1.id, b.id, e2.id, e2.dst, e3.id, e3.dst",
             expected
        )
    }

    #[test]
    fn two_hops_with_self_loop() {
        let expected = "WITH \
        nf1 AS (SELECT * FROM Netflow), \
        login1 AS (SELECT * FROM Events2v), \
        prog1 AS (SELECT * FROM Events1v), \
        E AS (SELECT * FROM nodes), \
        B AS (SELECT * FROM nodes), \
        A AS (SELECT * FROM nodes) \
        SELECT E.name, B.name, A.name \
        FROM nf1 \
        JOIN E ON nf1.dst = E.id \
        JOIN B ON nf1.src = B.id \
        JOIN login1 ON B.id = login1.dst \
        JOIN prog1 ON B.id = prog1.src \
        JOIN A ON login1.src = A.id \
        WHERE \
        nf1.id <> login1.id AND \
        nf1.layer_id = login1.layer_id OR nf1.layer_id <> login1.layer_id AND \
        nf1.id <> prog1.id AND \
        nf1.layer_id = prog1.layer_id OR nf1.layer_id <> prog1.layer_id AND \
        login1.id <> prog1.id AND \
        login1.layer_id = prog1.layer_id OR login1.layer_id <> prog1.layer_id";
        check_cypher_to_sql_layers(
            "MATCH (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B) RETURN E.name, B.name, A.name",
             expected,
            ["Netflow", "Events2v", "Events1v"]
        );
    }

    #[test]
    fn two_hops_with_self_loop_ignore_nodes() {
        let expect = "WITH \
        nf1 AS (SELECT * FROM Netflow), \
        login1 AS (SELECT * FROM Events2v), \
        prog1 AS (SELECT * FROM Events1v), \
        B AS (SELECT * FROM nodes), \
        A AS (SELECT * FROM nodes) \
        SELECT COUNT(nf1.id) FROM nf1 \
        JOIN B ON nf1.src = B.id \
        JOIN login1 ON B.id = login1.dst \
        JOIN prog1 ON B.id = prog1.src \
        JOIN A ON login1.src = A.id \
        WHERE \
        A.id <> B.id AND \
        nf1.id <> login1.id AND \
        nf1.layer_id = login1.layer_id OR nf1.layer_id <> login1.layer_id AND \
        nf1.id <> prog1.id AND \
        nf1.layer_id = prog1.layer_id OR nf1.layer_id <> prog1.layer_id AND \
        login1.id <> prog1.id AND \
        login1.layer_id = prog1.layer_id OR login1.layer_id <> prog1.layer_id";
        check_cypher_to_sql_layers(
            "MATCH (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B) WHERE A <> B RETURN count(*)",
             expect,
           ["Netflow", "Events2v", "Events1v"])
    }

    #[test]
    fn filter_edges_by_nodes_not_bound() {
        check_cypher_to_sql(
            "MATCH ({name: 'bla'})-[e]->() RETURN e",
            "WITH \
            e AS (SELECT * FROM _default), \
            a_2 AS (SELECT * FROM nodes) \
            SELECT e.* FROM e JOIN a_2 ON e.src = a_2.id WHERE a_2.name = 'bla'",
        )
    }

    #[test]
    fn hop_once_bind_both_nodes() {
        check_cypher_to_sql(
            "MATCH (n1)-[e]->(n2) RETURN n1.name, e, n2.name",
            "WITH e AS (SELECT * FROM _default), n1 AS (SELECT * FROM nodes), n2 AS (SELECT * FROM nodes) SELECT n1.name, e.*, n2.name FROM e JOIN n1 ON e.src = n1.id JOIN n2 ON e.dst = n2.id",
        );
    }

    #[test]
    fn hop_twice_with_conditions_and_nodes() {
        check_cypher_to_sql(
            "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE a.name CONTAINS 'aa' AND b.name CONTAINS 'bb' AND c.name CONTAINS 'cc' AND r1.eprop2flt <= r2.eprop2flt AND r1.eprop2flt >= (r2.eprop2flt - 40) return a.name,type(r1),b.name,type(r2),c.name,r1.eprop2flt,r2.eprop2flt",
            "WITH \
             r1 AS (SELECT * FROM _default), \
             r2 AS (SELECT * FROM _default), \
             a AS (SELECT * FROM nodes), \
             b AS (SELECT * FROM nodes), \
             c AS (SELECT * FROM nodes) \
             SELECT a.name, type(r1.layer_id), b.name, type(r2.layer_id), c.name, r1.eprop2flt, r2.eprop2flt \
             FROM r1 \
             JOIN a ON r1.src = a.id \
             JOIN b ON r1.dst = b.id \
             JOIN r2 ON b.id = r2.src \
             JOIN c ON r2.dst = c.id \
             WHERE \
             a.name LIKE '%aa%' \
             AND b.name LIKE '%bb%' \
             AND c.name LIKE '%cc%' \
             AND r1.eprop2flt <= r2.eprop2flt \
             AND r1.eprop2flt >= (r2.eprop2flt - 40L) \
             AND r1.id <> r2.id AND r1.layer_id = r2.layer_id OR r1.layer_id <> r2.layer_id",
        );
    }

    #[test]
    fn hop_3_times_out() {
        let expected = "WITH \
        e1 AS (SELECT * FROM _default), \
        e2 AS (SELECT * FROM _default), \
        e3 AS (SELECT * FROM _default) \
        SELECT e1.*, e2.*, e3.* FROM e1 \
        JOIN e2 ON e1.dst = e2.src \
        JOIN e3 ON e2.dst = e3.src \
        WHERE \
        e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id AND \
        e2.id <> e3.id AND e2.layer_id = e3.layer_id OR e2.layer_id <> e3.layer_id";
        check_cypher_to_sql(
            "MATCH ()-[e1]->()-[e2]->()-[e3]->() RETURN e1, e2, e3",
            expected,
        );
    }

    #[test]
    fn hop_two_times_in() {
        check_cypher_to_sql(
            "MATCH ()<-[e1]-()<-[e2]-() RETURN e1, e2",
            "WITH \
             e1 AS (SELECT * FROM _default), \
             e2 AS (SELECT * FROM _default) \
             SELECT e1.*, e2.* \
             FROM e1 \
             JOIN e2 ON e1.src = e2.dst \
             WHERE e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id",
        );
    }

    #[test]
    fn hop_out_in() {
        check_cypher_to_sql(
            "MATCH ()-[e1]->()<-[e2]-() RETURN e1, e2",
            "WITH \
             e1 AS (SELECT * FROM _default), \
             e2 AS (SELECT * FROM _default) \
             SELECT e1.*, e2.* \
             FROM e1 \
             JOIN e2 ON e1.dst = e2.dst \
             WHERE e1.id <> e2.id AND e1.layer_id = e2.layer_id OR e1.layer_id <> e2.layer_id",
        );
    }

    #[test]
    fn respect_parens() {
        check_cypher_to_sql(
            "match ()-[a]->() where a.name = 'John' or (1 < a.age and a.age < 10) RETURN a",
            "WITH a AS (SELECT * FROM _default) SELECT a.* FROM a WHERE a.name = 'John' OR (1L < a.age AND a.age < 10L)",
        );
    }

    #[test]
    fn scan_nodes_properties() {
        check_cypher_to_sql(
            "MATCH (n) RETURN n",
            "WITH n AS (SELECT * FROM nodes) SELECT n.* FROM n",
        );
    }

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
        let graph = DiskGraph::from_graph(&g, graph_dir).unwrap();
        let sql = transpiler::to_sql(query, &graph);
        assert_eq!(sql.to_string(), expected.to_string());
    }

    fn check_cypher_to_sql(query: &str, expected: &str) {
        check_cypher_to_sql_layers::<[String; 0]>(query, expected, [])
    }
}
