use std::{collections::HashMap, error::Error};

use executor::Pipeline;
use parser::ast::*;
use raphtory::arrow::{graph, graph_impl::ArrowGraph};

use crate::executor::operators::{EdgeScan, PhysicalOperator};

mod executor;
pub mod parser;

pub fn run_cypher_query(query: &str) -> Result<(), Box<dyn Error>> {
    let query = parser::parse_cypher(query)?;

    Ok(())
}

fn naive_query_to_pipeline(query: &Query, graph: &ArrowGraph) -> Result<Pipeline, Box<dyn Error>> {
    // only edge scan for now only on the first clause
    let Query::SingleQuery(query) = query;

    let mut operators:Vec<PhysicalOperator> = vec![];

    for clause in query.clauses.iter() {
        match clause {
            Clause::Match(m) => {
                let mut exprs = HashMap::new();
                if let Some(part) = m.pattern.0.first() {
                    let PatternPart {
                        node, rel_chain, ..
                    } = part;

                    exprs.extend(node.props.iter().flat_map(|props| {
                        props
                            .iter()
                            .map(|(name, expr)| (name.clone(), expr.clone()))
                    }));

                    if let Some((
                        RelPattern {
                            name, rel_types, ..
                        },
                        _,
                    )) = rel_chain.first()
                    {
                        let rel_type = rel_types.first().cloned().unwrap_or("_default".to_string());
                        let layer_id = graph.find_layer_id(&rel_type).unwrap_or(0);

                        let mut columns = vec![];
                        if let Some(name) = name {
                            if let Some(columns_iter) =
                                query.clauses.iter().find_map(|clause| match clause {
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
                                })
                            {
                                columns.extend(columns_iter);
                            }
                        }
                        let scan = EdgeScan::new(rel_type, columns);
                    }
                }
            }
            Clause::Return(ret) => {}
            _ => {}
        }
    }

    todo!()
}
