use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::view::StaticGraphViewOps,
        graph::views::property_filter::{
            FilterOperator, NodeFilter, NodeFilterValue, PropertyFilterValue,
        },
    },
    prelude::PropertyFilter,
    search::{graph_index::GraphIndex, node_index::NodeIndex, property_index::PropertyIndex},
};
use std::{
    collections::{Bound, HashSet},
    sync::Arc,
};
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

    pub(crate) fn build_property_query<G: StaticGraphViewOps>(
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

        let query: Option<Box<dyn Query>> = match prop_value {
            PropertyFilterValue::Single(prop_value) => match &filter.operator {
                FilterOperator::Eq => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(create_eq_query(term))
                }
                FilterOperator::Ne => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(create_ne_query(term))
                }
                FilterOperator::Lt => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(Box::new(RangeQuery::new_term_bounds(
                        prop_name.to_string(),
                        prop_field_type,
                        &Bound::Unbounded,
                        &Bound::Excluded(term),
                    )))
                }
                FilterOperator::Le => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(Box::new(RangeQuery::new_term_bounds(
                        prop_name.to_string(),
                        prop_field_type,
                        &Bound::Unbounded,
                        &Bound::Included(term),
                    )))
                }
                FilterOperator::Gt => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(Box::new(RangeQuery::new_term_bounds(
                        prop_name.to_string(),
                        prop_field_type,
                        &Bound::Excluded(term),
                        &Bound::Unbounded,
                    )))
                }
                FilterOperator::Ge => {
                    let term = create_property_tantivy_term(prop_field, prop_value)?;
                    Some(Box::new(RangeQuery::new_term_bounds(
                        prop_name.to_string(),
                        prop_field_type,
                        &Bound::Included(term),
                        &Bound::Unbounded,
                    )))
                }
                _ => unreachable!(),
            },
            PropertyFilterValue::Set(prop_values) => match &filter.operator {
                FilterOperator::In => {
                    let sub_queries = create_property_sub_queries(prop_field, prop_values);
                    create_in_query(sub_queries)
                }
                FilterOperator::NotIn => {
                    let sub_queries = create_property_sub_queries(prop_field, prop_values);
                    create_not_in_query(sub_queries)
                }
                _ => unreachable!(),
            },
            PropertyFilterValue::None => match &filter.operator {
                FilterOperator::IsSome => Some(Box::new(AllQuery)),
                FilterOperator::IsNone => None,
                _ => unreachable!(),
            },
        };

        Ok((property_index, query))
    }

    pub(crate) fn build_node_query<G: StaticGraphViewOps>(
        &self,
        filter: &NodeFilter,
    ) -> Result<(Arc<NodeIndex>, Option<Box<dyn Query>>), GraphError> {
        let field_name = &filter.field_name;
        let field_value = &filter.field_value;
        let node_index = &self.index.node_index;
        let node_field = node_index.get_node_field(field_name)?;

        let query: Option<Box<dyn Query>> = match field_value {
            NodeFilterValue::Single(node_value) => match &filter.operator {
                FilterOperator::Eq => {
                    let term = create_node_tantivy_term(node_field, node_value)?;
                    Some(create_eq_query(term))
                }
                FilterOperator::Ne => {
                    let term = create_node_tantivy_term(node_field, node_value)?;
                    Some(create_ne_query(term))
                }
                _ => unreachable!(),
            },
            NodeFilterValue::Set(node_values) => match &filter.operator {
                FilterOperator::In => {
                    let sub_queries = create_node_sub_queries(node_field, node_values);
                    create_in_query(sub_queries)
                }
                FilterOperator::NotIn => {
                    let sub_queries = create_node_sub_queries(node_field, node_values);
                    create_not_in_query(sub_queries)
                }
                _ => unreachable!(),
            },
        };

        Ok((Arc::from(node_index.clone()), query))
    }
}

fn create_property_tantivy_term(
    prop_field: tantivy::schema::Field,
    prop_value: &Prop,
) -> Result<tantivy::Term, GraphError> {
    match prop_value {
        Prop::Str(value) => Ok(tantivy::Term::from_field_text(prop_field, value.as_ref())),
        Prop::I32(value) => Ok(tantivy::Term::from_field_i64(prop_field, *value as i64)),
        Prop::I64(value) => Ok(tantivy::Term::from_field_i64(prop_field, *value)),
        Prop::U64(value) => Ok(tantivy::Term::from_field_u64(prop_field, *value)),
        Prop::F64(value) => Ok(tantivy::Term::from_field_f64(prop_field, *value)),
        Prop::Bool(value) => Ok(tantivy::Term::from_field_bool(prop_field, *value)),
        v => {
            println!("Unsupported value: {:?}", v);
            Err(GraphError::NotSupported)
        }
    }
}

fn create_property_sub_queries(
    prop_field: tantivy::schema::Field,
    prop_values: &HashSet<Prop>,
) -> Vec<(Occur, Box<dyn Query>)> {
    prop_values
        .iter()
        .filter_map(|v| create_property_tantivy_term(prop_field, v).ok())
        .map(|term| {
            (
                Should,
                Box::new(TermQuery::new(
                    term,
                    tantivy::schema::IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect()
}

fn create_node_tantivy_term(
    node_field: tantivy::schema::Field,
    node_value: &String,
) -> Result<tantivy::Term, GraphError> {
    Ok(tantivy::Term::from_field_text(
        node_field,
        node_value.as_ref(),
    ))
}

fn create_node_sub_queries(
    node_field: tantivy::schema::Field,
    node_values: &HashSet<String>,
) -> Vec<(Occur, Box<dyn Query>)> {
    node_values
        .iter()
        .filter_map(|value| create_node_tantivy_term(node_field, value).ok())
        .map(|term| {
            (
                Should,
                Box::new(TermQuery::new(
                    term,
                    tantivy::schema::IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect()
}

fn create_eq_query(term: tantivy::Term) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        term,
        tantivy::schema::IndexRecordOption::Basic,
    ))
}

fn create_ne_query(term: tantivy::Term) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(vec![
        (Should, Box::new(AllQuery)), // Include all documents
        (
            MustNot, // Exclude documents matching the term
            Box::new(TermQuery::new(
                term,
                tantivy::schema::IndexRecordOption::Basic,
            )),
        ),
    ]))
}

fn create_in_query(sub_queries: Vec<(Occur, Box<dyn Query>)>) -> Option<Box<dyn Query>> {
    if !sub_queries.is_empty() {
        Some(Box::new(BooleanQuery::new(sub_queries)))
    } else {
        None
    }
}

fn create_not_in_query(sub_queries: Vec<(Occur, Box<dyn Query>)>) -> Option<Box<dyn Query>> {
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
}
