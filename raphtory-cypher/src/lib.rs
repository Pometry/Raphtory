use std::{collections::HashMap, error::Error};

use executor::{operators::{Filter, Project}, Pipeline, Sink};
use parser::ast::*;
use raphtory::arrow::{graph, graph_impl::ArrowGraph};

use crate::executor::{
    operators::{EdgeScan, PhysicalOperator},
    Source,
};

mod executor;
pub mod parser;

pub fn run_cypher_query(query: &str) -> Result<(), Box<dyn Error>> {
    let query = parser::parse_cypher(query)?;

    Ok(())
}

fn naive_query_to_pipeline(query: &Query, graph: &ArrowGraph) -> Result<Pipeline, Box<dyn Error>> {
    // only edge scan for now only on the first clause
    let Query::SingleQuery(query) = query;

    let mut source: Option<Box<dyn Source>> = None;
    let mut sink: Option<Box<dyn Sink>> = None;

    let mut operators: Vec<PhysicalOperator> = vec![];

    let mut var_bind_count = 0;

    for clause in query.clauses.iter() {
        match clause {
            Clause::Match(m) => {
                let mut exprs = vec![];
                if let Some(part) = m.pattern.0.first() {
                    let PatternPart {
                        node, rel_chain, ..
                    } = part;

                    eq_expr_on_path(&mut var_bind_count, &mut exprs, node);

                    if let Some((
                        RelPattern {
                            name, rel_types, ..
                        },
                        _,
                    )) = rel_chain.first()
                    {
                        let rel_type = rel_types.first().cloned().unwrap_or("_default".to_string());
                        let layer_id = graph.find_layer_id(&rel_type).unwrap_or(0);

                        let name = name
                            .as_ref()
                            .cloned()
                            .unwrap_or_else(|| next_var_bind(&mut var_bind_count));

                        let columns = extract_column_names_and_ids(
                            &name,
                            &mut var_bind_count,
                            query,
                            graph,
                            layer_id,
                        );

                        source = Some(Box::new(EdgeScan::new(&name, rel_type, columns)));
                    }
                }

                if let Some(expr) = &m.where_clause {
                    exprs.push(expr.clone());
                }

                if !exprs.is_empty() {
                    operators.push(PhysicalOperator::Filter(Filter::new(exprs)));
                }
            }
            Clause::Return(ret) => {
                let project_exprs = ret.items.iter().map(|ret_i| ret_i.expr.clone()).collect();
                operators.push(PhysicalOperator::Project(Project::new(project_exprs)));
            }
        }
    }
    let mut pipeline = Pipeline::new(source.unwrap(), sink.unwrap());

    for operator in operators {
        pipeline.add_operator(operator);
    }

    Ok(pipeline)
}

fn extract_column_names_and_ids(
    name: &str,
    var_bind_count: &mut usize,
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

fn eq_expr_on_path(var_bind_count: &mut usize, predicates: &mut Vec<Expr>, node: &NodePattern) {
    let var_name = node
        .name
        .as_ref()
        .cloned()
        .unwrap_or_else(|| next_var_bind(var_bind_count));

    predicates.extend(node.props.iter().flat_map(|props| {
        props
            .iter()
            .map(|(p_name, expr)| Expr::eq(Expr::var(&var_name, [p_name]), expr.clone()))
    }));
}

fn next_var_bind(var_bind_count: &mut usize) -> String {
    let var = format!("var_{}", var_bind_count);
    *var_bind_count += 1;
    var
}
