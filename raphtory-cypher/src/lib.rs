use std::sync::Arc;

use datafusion::{
    execution::context::{SQLOptions, SessionContext},
    physical_plan::projection,
};
use executor::{table_provider::EdgeListTableProvider, ExecError};
use parser::ast::*;
use raphtory::arrow::graph_impl::ArrowGraph;
use sqlparser::ast::{self as sql_ast, Select};

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

fn parse_limit(_query: &Query) -> Option<sql_ast::Expr> {
    // TODO: implement actual limit
    None
}

fn parse_order_by(query: &Query) -> Vec<sql_ast::OrderByExpr> {
    vec![]
}

fn parse_select_body(query: &Query) -> Box<sql_ast::SetExpr> {
    let mut projection = vec![];
    let mut tables = vec![];
    let mut order_by = vec![];

    Box::new(sql_ast::SetExpr::Select(Box::new(sql_ast::Select {
        distinct: None,
        // MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
        top: None,
        // projection expressions
        projection: projection,
        // INTO
        into: None,
        // FROM
        from: tables,
        // LATERAL VIEWs
        lateral_views: vec![],
        // WHERE
        selection: parse_selection(query),
        // GROUP BY
        group_by: GroupByExpr,
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

pub async fn run_query(query: &Query, graph: &ArrowGraph) -> Result<(), ExecError> {
    let ctx = SessionContext::new();

    for layer in graph.layer_names() {
        let table = EdgeListTableProvider::new(layer, graph.clone())?;
        ctx.register_table(layer, Arc::new(table))?;
    }

    let query = cypher_to_sql(query);
    let plan = ctx
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            query,
        )))
        .await?;
    let opts = SQLOptions::new();
    opts.verify_plan(&plan)?;
    ctx.execute_logical_plan(plan).await?;

    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_cypher_to_sql_select() {
        let query = "MATCH ()-[e]->() RETURN e";
        let query = parser::parse_cypher(query).unwrap();
        let sql = cypher_to_sql(&query);

        assert_eq!(sql.to_string(), "SELECT e.* FROM graph AS e".to_string());
    }
}

// pub fn naive_query_to_pipeline(
//     sink: Box<dyn Sink>,
//     query: &Query,
//     graph: &ArrowGraph,
// ) -> Result<Pipeline, Box<dyn Error>> {
//     // only edge scan for now only on the first clause
//     let Query::SingleQuery(query) = query;

//     let mut source: Option<Box<dyn Source>> = None;

//     let mut operators: Vec<PhysicalOperator> = vec![];

//     let mut var_bind_count = 0;

//     for clause in query.clauses.iter() {
//         match clause {
//             Clause::Match(m) => {
//                 let mut exprs = vec![];
//                 if let Some(part) = m.pattern.0.first() {
//                     let PatternPart {
//                         node, rel_chain, ..
//                     } = part;

//                     eq_expr_on_path(&mut exprs, node);

//                     if let Some((
//                         RelPattern {
//                             name, rel_types, ..
//                         },
//                         _,
//                     )) = rel_chain.first()
//                     {
//                         let rel_type = rel_types.first().cloned().unwrap_or("_default".to_string());
//                         let layer_id = graph.find_layer_id(&rel_type).unwrap_or(0);

//                         let columns = extract_column_names_and_ids(
//                             &name,
//                             &mut var_bind_count,
//                             query,
//                             graph,
//                             layer_id,
//                         );
//                         source = Some(Box::new(EdgeScan::new(&name, rel_type, columns)));
//                     }
//                 }

//                 if let Some(expr) = &m.where_clause {
//                     exprs.push(expr.clone());
//                 }

//                 if !exprs.is_empty() {
//                     operators.push(PhysicalOperator::Filter(Filter::new(exprs)));
//                 }
//             }
//             Clause::Return(ret) => {
//                 let project_exprs = ret.items.iter().map(|ret_i| ret_i.expr.clone()).collect();
//                 operators.push(PhysicalOperator::Project(Project::new(project_exprs)));
//             }
//         }
//     }
//     let mut pipeline = Pipeline::new(source.unwrap(), sink);

//     for operator in operators {
//         pipeline.add_operator(operator);
//     }

//     Ok(pipeline)
// }

fn extract_column_names_and_ids(
    name: &str,
    _var_bind_count: &mut usize,
    query: &SingleQuery,
    graph: &ArrowGraph,
    layer_id: usize,
) -> Vec<(String, usize)> {
    let mut columns = vec![];

    if let Some(columns_iter) = query.clauses.iter().find_map(|clause| match clause {
        Clause::Return(ret) => {
            let iter = ret
                .items
                .iter()
                .flat_map(|ret_i| ret_i.expr.bindings().into_iter())
                .filter(|(n, _)| n == name)
                .flat_map(|(_, attrs)| attrs)
                .filter_map(|name| {
                    graph
                        .edge_property_id(&name, layer_id)
                        .map(|id| (name.clone(), id))
                });
            Some(iter)
        }
        _ => None,
    }) {
        columns.extend(columns_iter);
    }
    columns
}

fn eq_expr_on_path(predicates: &mut Vec<Expr>, node: &NodePattern) {
    let var_name = &node.name;

    predicates.extend(node.props.iter().flat_map(|props| {
        props
            .iter()
            .map(|(p_name, expr)| Expr::eq(Expr::var(var_name, [p_name]), expr.clone()))
    }));
}

// #[cfg(test)]
// mod test {

//     use self::executor::{ChannelSink, Executor};

//     use super::*;

//     #[test]
//     fn scan_edge_no_filters() {
//         let query = "MATCH ()-[r]->() where r.time < 4 RETURN r.src,r.time, r.weight, r.dst";
//         println!("{:?}", query);
//         let query = parser::parse_cypher(query).unwrap();
//         let edges = vec![
//             (0, 1, 0, 1.0),
//             (0, 1, 1, 2.0),
//             (0, 1, 2, 3.0),
//             (0, 2, 3, 4.0),
//             (0, 3, 4, 5.0),
//         ];
//         let graph_dir = tempfile::tempdir().unwrap();

//         let (send, recv) = std::sync::mpsc::channel();
//         let sink = ChannelSink::new(send);

//         let graph = ArrowGraph::make_simple_graph(graph_dir, &edges, 100, 100);
//         let pipeline = naive_query_to_pipeline(Box::new(sink), &query, &graph).unwrap();

//         println!("{:?}", pipeline);

//         let executor = Executor::new(graph, pipeline);

//         executor.execute_pipeline(1).expect("execution failed");

//         while let Ok(block) = recv.recv() {
//             println!("{:?}", block);
//         }
//     }
// }

// #[cfg(test)]
// mod test2 {

//     use arrow::{
//         array::*,
//         datatypes::{Int16Type, Int32Type},
//     };

//     #[test]
//     fn test_run_array_filter() {
//         let test = vec![1, 3, 6, 10];
//         let run_ends: PrimitiveArray<Int16Type> = test.into();
//         let values = Box::new(PrimitiveArray::<Int32Type>::from_iter_values([
//             10, 11, 12, 13,
//         ]));
//         let array: RunArray<Int16Type> = RunArray::try_new(&run_ends, values.as_ref()).unwrap();
//         println!("{:?}", array.data_type());

//         assert_eq!(array.len(), 10);
//         assert_eq!(array.null_count(), 0);

//         let mask = BooleanArray::from(vec![true, false, true, false]);

//         let boxed = Box::new(array);

//         let actual = arrow::compute::sort(boxed.as_ref(), None).expect("sort failed");
//         println!("{:?}", actual);
//         let actual = arrow::compute::filter(boxed.as_ref(), &mask).expect("filter failed");
//         println!("{:?}", actual);
//     }
// }
