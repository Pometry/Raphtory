use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::view::StaticGraphViewOps,
        graph::views::filter::{Filter, FilterOperator, FilterValue, PropertyFilterValue},
    },
    prelude::PropertyFilter,
    search::{
        edge_index::EdgeIndex, graph_index::GraphIndex, node_index::NodeIndex,
        property_index::PropertyIndex,
    },
};
use itertools::Itertools;
use std::{collections::Bound, ops::Deref, sync::Arc, vec};
use tantivy::{
    query::{
        AllQuery, BooleanQuery, EmptyQuery, Occur,
        Occur::{Must, MustNot, Should},
        Query, RangeQuery, TermQuery,
    },
    schema::{Field, FieldType, IndexRecordOption, Type},
    tokenizer::TokenizerManager,
    Term,
};

#[derive(Clone, Copy)]
pub struct QueryBuilder<'a> {
    index: &'a GraphIndex,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(index: &'a GraphIndex) -> Self {
        Self { index }
    }

    pub(crate) fn build_property_query<G: StaticGraphViewOps>(
        &self,
        property_index: &Arc<PropertyIndex>,
        filter: &PropertyFilter,
    ) -> Result<Option<Box<dyn Query>>, GraphError> {
        let prop_name = filter.prop_ref.name();
        let prop_value = &filter.prop_value;
        let prop_field_type = property_index.get_prop_field_type(prop_name)?;
        let query: Option<Box<dyn Query>> = match prop_value {
            PropertyFilterValue::Single(prop_value) => match &filter.operator {
                FilterOperator::Eq => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_eq_query(term)
                }
                FilterOperator::Ne => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_ne_query(term)
                }
                FilterOperator::Lt => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_lt_query(prop_name.to_string(), prop_field_type, term)
                }
                FilterOperator::Le => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_le_query(prop_name.to_string(), prop_field_type, term)
                }
                FilterOperator::Gt => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_gt_query(prop_name.to_string(), prop_field_type, term)
                }
                FilterOperator::Ge => {
                    let term =
                        create_property_exact_tantivy_term(property_index, prop_name, prop_value)?;
                    create_ge_query(prop_name.to_string(), prop_field_type, term)
                }
                FilterOperator::Contains => {
                    let terms = create_property_tokenized_tantivy_terms(
                        property_index,
                        prop_name,
                        prop_value,
                    )?;
                    create_contains_query(terms)
                }
                FilterOperator::ContainsNot => {
                    let terms = create_property_tokenized_tantivy_terms(
                        property_index,
                        prop_name,
                        prop_value,
                    )?;
                    create_contains_not_query(terms)
                }
                FilterOperator::FuzzySearch {
                    levenshtein_distance: _,
                    prefix_match: _,
                } => None,
                _ => unreachable!(),
            },
            PropertyFilterValue::Set(prop_values) => {
                let terms: Result<Vec<Term>, GraphError> = prop_values
                    .deref()
                    .into_iter()
                    .map(|value| {
                        create_property_exact_tantivy_term(property_index, prop_name, &value)
                    })
                    .collect();
                let terms = terms?;
                match &filter.operator {
                    FilterOperator::In => create_in_query(terms),
                    FilterOperator::NotIn => create_not_in_query(terms),
                    _ => unreachable!(),
                }
            }
            PropertyFilterValue::None => match &filter.operator {
                FilterOperator::IsSome => Some(Box::new(AllQuery)),
                FilterOperator::IsNone => None,
                _ => unreachable!(),
            },
        };

        Ok(query)
    }

    pub(crate) fn build_node_query(
        &self,
        filter: &Filter,
    ) -> Result<(Arc<NodeIndex>, Option<Box<dyn Query>>), GraphError> {
        let node_index = &self.index.node_index;
        let field_name = &filter.field_name;
        let filter_value = &filter.field_value;
        let operator = &filter.operator;

        let query = match filter_value {
            FilterValue::Single(node_value) => match operator {
                FilterOperator::Eq => {
                    let term = create_node_exact_tantivy_term(node_index, field_name, node_value)?;
                    create_eq_query(term)
                }
                FilterOperator::Ne => {
                    let term = create_node_exact_tantivy_term(node_index, field_name, node_value)?;
                    create_ne_query(term)
                }
                FilterOperator::Contains => {
                    let terms =
                        create_node_tokenized_tantivy_terms(node_index, field_name, node_value)?;
                    create_contains_query(terms)
                }
                FilterOperator::ContainsNot => {
                    let terms =
                        create_node_tokenized_tantivy_terms(node_index, field_name, node_value)?;
                    create_contains_not_query(terms)
                }
                FilterOperator::FuzzySearch {
                    levenshtein_distance: _,
                    prefix_match: _,
                } => None,
                _ => unreachable!(),
            },
            FilterValue::Set(node_values) => {
                let terms: Result<Vec<Term>, GraphError> = node_values
                    .deref()
                    .into_iter()
                    .map(|value| create_node_exact_tantivy_term(node_index, field_name, &value))
                    .collect();
                let terms = terms?;
                match operator {
                    FilterOperator::In => create_in_query(terms),
                    FilterOperator::NotIn => create_not_in_query(terms),
                    _ => unreachable!(),
                }
            }
        };

        Ok((Arc::from(node_index.clone()), query))
    }

    pub(crate) fn build_edge_query(
        &self,
        filter: &Filter,
    ) -> Result<(Arc<EdgeIndex>, Option<Box<dyn Query>>), GraphError> {
        let edge_index = &self.index.edge_index;
        let field_name = &filter.field_name;
        let filter_value = &filter.field_value;
        let operator = &filter.operator;

        let query = match filter_value {
            FilterValue::Single(node_value) => match operator {
                FilterOperator::Eq => {
                    let term = create_edge_exact_tantivy_term(edge_index, field_name, node_value)?;
                    create_eq_query(term)
                }
                FilterOperator::Ne => {
                    let term = create_edge_exact_tantivy_term(edge_index, field_name, node_value)?;
                    create_ne_query(term)
                }
                FilterOperator::Contains => {
                    let terms =
                        create_edge_tokenized_tantivy_terms(edge_index, field_name, node_value)?;
                    create_contains_query(terms)
                }
                FilterOperator::ContainsNot => {
                    let terms =
                        create_edge_tokenized_tantivy_terms(edge_index, field_name, node_value)?;
                    create_contains_not_query(terms)
                }
                FilterOperator::FuzzySearch {
                    levenshtein_distance: _,
                    prefix_match: _,
                } => None,
                _ => unreachable!(),
            },
            FilterValue::Set(edge_values) => {
                let terms: Result<Vec<Term>, GraphError> = edge_values
                    .deref()
                    .into_iter()
                    .map(|value| create_edge_exact_tantivy_term(edge_index, field_name, &value))
                    .collect();
                let terms = terms?;
                match operator {
                    FilterOperator::In => create_in_query(terms),
                    FilterOperator::NotIn => create_not_in_query(terms),
                    _ => unreachable!(),
                }
            }
        };

        Ok((Arc::from(edge_index.clone()), query))
    }
}

pub fn get_str_field_tokens(
    tokenizer_manager: &TokenizerManager,
    field_type: &FieldType,
    field_value: &str,
) -> Result<Vec<String>, GraphError> {
    let indexing_options = match field_type {
        FieldType::Str(str_options) => str_options
            .get_indexing_options()
            .ok_or(GraphError::UnsupportedFieldTypeForTokenization),
        _ => Err(GraphError::UnsupportedFieldTypeForTokenization),
    }?;

    let tokenizer_name = indexing_options.tokenizer();

    let mut tokenizer = tokenizer_manager
        .get(tokenizer_name)
        .ok_or_else(|| GraphError::NotSupported)?;

    let mut token_stream = tokenizer.token_stream(field_value);
    let mut tokens = Vec::new();
    while let Some(token) = token_stream.next() {
        tokens.push(token.text.clone());
    }

    if tokens.len() < 1 {
        return Err(GraphError::NoTokensFound);
    }

    Ok(tokens)
}

fn create_property_exact_tantivy_term(
    property_index: &Arc<PropertyIndex>,
    prop_name: &str,
    prop_value: &Prop,
) -> Result<Term, GraphError> {
    let prop_field = property_index.get_prop_field(prop_name)?;
    match prop_value {
        Prop::Str(value) => Ok(Term::from_field_text(prop_field, value.as_ref())),
        Prop::I32(value) => Ok(Term::from_field_i64(prop_field, *value as i64)),
        Prop::I64(value) => Ok(Term::from_field_i64(prop_field, *value)),
        Prop::U64(value) => Ok(Term::from_field_u64(prop_field, *value)),
        Prop::F64(value) => Ok(Term::from_field_f64(prop_field, *value)),
        Prop::Bool(value) => Ok(Term::from_field_bool(prop_field, *value)),
        v => Err(GraphError::UnsupportedValue(v.to_string())),
    }
}

fn create_property_tokenized_tantivy_terms(
    property_index: &PropertyIndex,
    prop_name: &str,
    prop_value: &Prop,
) -> Result<Vec<Term>, GraphError> {
    match prop_value {
        Prop::Str(value) => {
            let prop_field = property_index.get_tokenized_prop_field(prop_name)?;
            let schema = property_index.index.schema();
            let field_entry = schema.get_field_entry(prop_field.clone());
            let field_type = field_entry.field_type();
            let tokens =
                get_str_field_tokens(property_index.index.tokenizers(), field_type, value)?;
            create_terms_from_tokens(prop_field, tokens)
        }
        Prop::I32(value) => {
            let prop_field = property_index.get_prop_field(prop_name)?;
            Ok(vec![Term::from_field_i64(prop_field, *value as i64)])
        }
        Prop::I64(value) => {
            let prop_field = property_index.get_prop_field(prop_name)?;
            Ok(vec![Term::from_field_i64(prop_field, *value)])
        }
        Prop::U64(value) => {
            let prop_field = property_index.get_prop_field(prop_name)?;
            Ok(vec![Term::from_field_u64(prop_field, *value)])
        }
        Prop::F64(value) => {
            let prop_field = property_index.get_prop_field(prop_name)?;
            Ok(vec![Term::from_field_f64(prop_field, *value)])
        }
        Prop::Bool(value) => {
            let prop_field = property_index.get_prop_field(prop_name)?;
            Ok(vec![Term::from_field_bool(prop_field, *value)])
        }
        v => Err(GraphError::UnsupportedValue(v.to_string())),
    }
}

fn create_sub_queries(terms: Vec<Term>) -> Vec<(Occur, Box<dyn Query>)> {
    terms
        .into_iter()
        .map(|term| {
            (
                Should,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
            )
        })
        .collect()
}

fn create_node_exact_tantivy_term(
    node_index: &NodeIndex,
    field_name: &str,
    field_value: &str,
) -> Result<Term, GraphError> {
    let field = node_index.get_node_field(field_name)?;
    Ok(Term::from_field_text(field, field_value))
}

fn create_node_tokenized_tantivy_terms(
    node_index: &NodeIndex,
    field_name: &str,
    field_value: &str,
) -> Result<Vec<Term>, GraphError> {
    let index = &node_index.entity_index.index;
    let schema = &index.schema();
    let tokenizer_manager = index.tokenizers();
    let field = node_index.get_tokenized_node_field(field_name)?;
    let field_entry = schema.get_field_entry(field.clone());
    let field_type = field_entry.field_type();
    let tokens = get_str_field_tokens(tokenizer_manager, &field_type, field_value)?;
    create_terms_from_tokens(field, tokens)
}

fn create_edge_exact_tantivy_term(
    edge_index: &EdgeIndex,
    field_name: &str,
    field_value: &str,
) -> Result<Term, GraphError> {
    let field = edge_index.get_edge_field(field_name)?;
    Ok(Term::from_field_text(field, field_value))
}

fn create_edge_tokenized_tantivy_terms(
    edge_index: &EdgeIndex,
    field_name: &str,
    field_value: &str,
) -> Result<Vec<Term>, GraphError> {
    let index = &edge_index.entity_index.index;
    let schema = &index.schema();
    let tokenizer_manager = index.tokenizers();
    let field = edge_index.get_tokenized_edge_field(field_name)?;
    let field_entry = schema.get_field_entry(field.clone());
    let field_type = field_entry.field_type();
    let tokens = get_str_field_tokens(tokenizer_manager, &field_type, field_value)?;
    create_terms_from_tokens(field, tokens)
}

fn create_terms_from_tokens(field: Field, tokens: Vec<String>) -> Result<Vec<Term>, GraphError> {
    Ok(tokens
        .into_iter()
        .map(|value| Term::from_field_text(field, value.as_ref()))
        .collect_vec())
}

fn create_eq_query(term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
}

fn create_ne_query(term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(BooleanQuery::new(vec![
        (Should, Box::new(AllQuery)),
        (
            MustNot,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ),
    ])))
}

fn create_lt_query(prop_name: String, prop_field_type: Type, term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(RangeQuery::new_term_bounds(
        prop_name,
        prop_field_type,
        &Bound::Unbounded,
        &Bound::Excluded(term),
    )))
}

fn create_le_query(prop_name: String, prop_field_type: Type, term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(RangeQuery::new_term_bounds(
        prop_name.to_string(),
        prop_field_type,
        &Bound::Unbounded,
        &Bound::Included(term),
    )))
}

fn create_gt_query(prop_name: String, prop_field_type: Type, term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(RangeQuery::new_term_bounds(
        prop_name.to_string(),
        prop_field_type,
        &Bound::Excluded(term),
        &Bound::Unbounded,
    )))
}

fn create_ge_query(prop_name: String, prop_field_type: Type, term: Term) -> Option<Box<dyn Query>> {
    Some(Box::new(RangeQuery::new_term_bounds(
        prop_name.to_string(),
        prop_field_type,
        &Bound::Included(term),
        &Bound::Unbounded,
    )))
}

fn create_in_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    if !terms.is_empty() {
        let sub_queries = create_sub_queries(terms);
        Some(Box::new(BooleanQuery::new(sub_queries)))
    } else {
        Some(Box::new(EmptyQuery))
    }
}

fn create_not_in_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    if !terms.is_empty() {
        let sub_queries = create_sub_queries(terms);
        Some(Box::new(BooleanQuery::new(vec![
            (Must, Box::new(AllQuery)), // Include all documents
            (
                MustNot,
                Box::new(BooleanQuery::new(sub_queries)), // Exclude matching terms
            ),
        ])))
    } else {
        Some(Box::new(EmptyQuery))
    }
}

fn create_contains_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    if !terms.is_empty() {
        let sub_queries = create_sub_queries(terms);
        Some(Box::new(BooleanQuery::new(sub_queries)))
    } else {
        Some(Box::new(EmptyQuery))
    }
}

fn create_contains_not_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    if !terms.is_empty() {
        let sub_queries = create_sub_queries(terms);
        Some(Box::new(BooleanQuery::new(vec![
            (Must, Box::new(AllQuery)), // Include all documents
            (
                MustNot,
                Box::new(BooleanQuery::new(sub_queries)), // Exclude matching terms
            ),
        ])))
    } else {
        Some(Box::new(EmptyQuery))
    }
}
