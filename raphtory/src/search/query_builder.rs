use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{api::view::StaticGraphViewOps, graph::views::property_filter::ComparisonOperator},
    prelude::PropertyFilter,
    search::{create_tantivy_term, graph_index::GraphIndex, property_index::PropertyIndex},
};
use std::{collections::Bound, sync::Arc};
use tantivy::query::{
    AllQuery, BooleanQuery, Occur,
    Occur::{Must, MustNot, Should},
    Query, RangeQuery, TermQuery,
};

pub struct QueryBuilder<'a> {
    index: &'a GraphIndex,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
        Self { index }
    }

    pub(crate) fn build_query<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter,
    ) -> Result<(Arc<PropertyIndex>, Option<Box<dyn Query>>), GraphError> {
        let prop_name = &filter.prop_name;
        let prop_value = &filter.prop_value;
        let property_index = self
            .index
            .node_index
            .get_property_index(graph.node_meta(), prop_name)?;
        let prop_field = property_index.get_prop_field(prop_name)?;
        let prop_field_type = property_index.get_prop_field_type(prop_name)?;

        let query: Option<Box<dyn Query>> = match &filter.operator {
            ComparisonOperator::Eq => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(TermQuery::new(
                    term,
                    tantivy::schema::IndexRecordOption::Basic,
                )))
            }
            ComparisonOperator::Ne => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(BooleanQuery::new(vec![
                    // Include all documents
                    (Should, Box::new(AllQuery)),
                    // Exclude documents matching the term
                    (
                        MustNot,
                        Box::new(TermQuery::new(
                            term,
                            tantivy::schema::IndexRecordOption::Basic,
                        )),
                    ),
                ])))
            }
            ComparisonOperator::Lt => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(RangeQuery::new_term_bounds(
                    prop_name.to_string(),
                    prop_field_type,
                    &Bound::Unbounded,
                    &Bound::Excluded(term),
                )))
            }
            ComparisonOperator::Le => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(RangeQuery::new_term_bounds(
                    prop_name.to_string(),
                    prop_field_type,
                    &Bound::Unbounded,
                    &Bound::Included(term),
                )))
            }
            ComparisonOperator::Gt => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(RangeQuery::new_term_bounds(
                    prop_name.to_string(),
                    prop_field_type,
                    &Bound::Excluded(term),
                    &Bound::Unbounded,
                )))
            }
            ComparisonOperator::Ge => {
                let term = create_tantivy_term(prop_field, prop_value)?;
                Some(Box::new(RangeQuery::new_term_bounds(
                    prop_name.to_string(),
                    prop_field_type,
                    &Bound::Included(term),
                    &Bound::Unbounded,
                )))
            }
            ComparisonOperator::In => {
                if let Some(Prop::List(values)) = prop_value {
                    let sub_queries: Vec<(Occur, Box<dyn Query>)> = values
                        .iter()
                        .filter_map(|v| create_tantivy_term(prop_field, &Some(v.clone())).ok())
                        .map(|term| {
                            (
                                Should,
                                Box::new(TermQuery::new(
                                    term,
                                    tantivy::schema::IndexRecordOption::Basic,
                                )) as Box<dyn Query>,
                            )
                        })
                        .collect();

                    if !sub_queries.is_empty() {
                        Some(Box::new(BooleanQuery::new(sub_queries)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ComparisonOperator::NotIn => {
                if let Some(Prop::List(values)) = prop_value {
                    let sub_queries: Vec<(Occur, Box<dyn Query>)> = values
                        .iter()
                        .filter_map(|v| create_tantivy_term(prop_field, &Some(v.clone())).ok())
                        .map(|term| {
                            (
                                Should,
                                Box::new(TermQuery::new(
                                    term,
                                    tantivy::schema::IndexRecordOption::Basic,
                                )) as Box<dyn Query>,
                            )
                        })
                        .collect();

                    if !sub_queries.is_empty() {
                        Some(Box::new(BooleanQuery::new(vec![
                            (Must, Box::new(AllQuery)), // Include all documents
                            (
                                MustNot,
                                Box::new(BooleanQuery::new(sub_queries)), // Exclude matching terms
                            ),
                        ])))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ComparisonOperator::IsSome => Some(Box::new(AllQuery)),
            ComparisonOperator::IsNone => None,
            _ => return Err(GraphError::NotSupported),
        };

        Ok((property_index, query))
    }
}
