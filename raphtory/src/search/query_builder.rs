use crate::{
    core::{utils::errors::GraphError, Prop},
    db::{
        api::view::StaticGraphViewOps,
        graph::views::property_filter::{Filter, FilterOperator, FilterValue, PropertyFilterValue},
    },
    prelude::PropertyFilter,
    search::{
        edge_index::EdgeIndex, graph_index::GraphIndex, node_index::NodeIndex,
        property_index::PropertyIndex,
    },
};
use itertools::Itertools;
use std::{
    collections::{Bound, HashSet},
    sync::Arc,
    vec,
};
use tantivy::{
    query::{
        AllQuery, BooleanQuery, FuzzyTermQuery, Occur,
        Occur::{Must, MustNot, Should},
        PhraseQuery, Query, RangeQuery, TermQuery,
    },
    schema::{Field, FieldType, IndexRecordOption, Schema, Type},
    tokenizer::TokenizerManager,
    Index, Term,
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
        property_index: Arc<PropertyIndex>,
        filter: &PropertyFilter,
    ) -> Result<(Arc<PropertyIndex>, Option<Box<dyn Query>>), GraphError> {
        let prop_name = &filter.prop_name;
        let prop_value = &filter.prop_value;
        let prop_field = property_index.get_prop_field(prop_name)?;
        let prop_field_type = property_index.get_prop_field_type(prop_name)?;

        let query: Option<Box<dyn Query>> = match prop_value {
            PropertyFilterValue::Single(prop_value) => {
                let terms = create_property_tantivy_terms(&property_index, prop_field, prop_value)?;
                match &filter.operator {
                    FilterOperator::Eq => create_eq_query(terms),
                    FilterOperator::Ne => create_ne_query(terms),
                    FilterOperator::Lt => {
                        create_lt_query(prop_name.to_string(), prop_field_type, terms)
                    }
                    FilterOperator::Le => {
                        create_le_query(prop_name.to_string(), prop_field_type, terms)
                    }
                    FilterOperator::Gt => {
                        create_gt_query(prop_name.to_string(), prop_field_type, terms)
                    }
                    FilterOperator::Ge => {
                        create_ge_query(prop_name.to_string(), prop_field_type, terms)
                    }
                    FilterOperator::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => create_fuzzy_search_query(terms, levenshtein_distance, prefix_match),
                    _ => unreachable!(),
                }
            }
            PropertyFilterValue::Set(prop_values) => {
                let sub_queries =
                    create_property_sub_queries(&property_index, prop_field, prop_values);
                match &filter.operator {
                    FilterOperator::In => create_in_query(sub_queries),
                    FilterOperator::NotIn => create_not_in_query(sub_queries),
                    _ => unreachable!(),
                }
            }
            PropertyFilterValue::None => match &filter.operator {
                FilterOperator::IsSome => Some(Box::new(AllQuery)),
                FilterOperator::IsNone => None,
                _ => unreachable!(),
            },
        };

        Ok((property_index, query))
    }

    fn build_query_generic(
        &self,
        index: &Index,
        node_field: Field,
        filter_value: &FilterValue,
        operator: &FilterOperator,
    ) -> Result<Option<Box<dyn Query>>, GraphError> {
        let schema = &index.schema();
        let tokenizer_manager = index.tokenizers();
        let query = match filter_value {
            FilterValue::Single(node_value) => {
                let terms =
                    create_tantivy_terms(schema, tokenizer_manager, node_field, node_value)?;
                match operator {
                    FilterOperator::Eq => create_eq_query(terms),
                    FilterOperator::Ne => create_ne_query(terms),
                    FilterOperator::FuzzySearch {
                        levenshtein_distance,
                        prefix_match,
                    } => create_fuzzy_search_query(terms, levenshtein_distance, prefix_match),
                    _ => unreachable!(),
                }
            }
            FilterValue::Set(node_values) => {
                let sub_queries =
                    create_sub_queries(schema, tokenizer_manager, node_field, node_values);
                match operator {
                    FilterOperator::In => create_in_query(sub_queries),
                    FilterOperator::NotIn => create_not_in_query(sub_queries),
                    _ => unreachable!(),
                }
            }
        };

        Ok(query)
    }

    pub(crate) fn build_node_query(
        &self,
        filter: &Filter,
    ) -> Result<(Arc<NodeIndex>, Option<Box<dyn Query>>), GraphError> {
        let node_index = &self.index.node_index;
        let index = &node_index.index;
        let field_name = &filter.field_name;
        let field = node_index.get_node_field(field_name)?;
        let filter_value = &filter.field_value;
        let operator = &filter.operator;

        let query = self.build_query_generic(index, field, filter_value, operator)?;
        Ok((Arc::from(node_index.clone()), query))
    }

    pub(crate) fn build_edge_query(
        &self,
        filter: &Filter,
    ) -> Result<(Arc<EdgeIndex>, Option<Box<dyn Query>>), GraphError> {
        let edge_index = &self.index.edge_index;
        let index = &edge_index.index;
        let field_name = &filter.field_name;
        let field = edge_index.get_edge_field(field_name)?;
        let filter_value = &filter.field_value;
        let operator = &filter.operator;

        let query = self.build_query_generic(index, field, filter_value, operator)?;
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

fn create_property_tantivy_terms(
    property_index: &PropertyIndex,
    prop_field: Field,
    prop_value: &Prop,
) -> Result<Vec<Term>, GraphError> {
    match prop_value {
        Prop::Str(value) => {
            let schema = property_index.index.schema();
            let field_entry = schema.get_field_entry(prop_field.clone());
            let field_type = field_entry.field_type();
            let tokens =
                get_str_field_tokens(property_index.index.tokenizers(), field_type, value)?;
            create_terms_from_tokens(prop_field, tokens)
        }
        Prop::I32(value) => Ok(vec![Term::from_field_i64(prop_field, *value as i64)]),
        Prop::I64(value) => Ok(vec![Term::from_field_i64(prop_field, *value)]),
        Prop::U64(value) => Ok(vec![Term::from_field_u64(prop_field, *value)]),
        Prop::F64(value) => Ok(vec![Term::from_field_f64(prop_field, *value)]),
        Prop::Bool(value) => Ok(vec![Term::from_field_bool(prop_field, *value)]),
        v => {
            println!("Unsupported value: {:?}", v);
            Err(GraphError::NotSupported)
        }
    }
}

fn create_sub_queries_generic<F, T>(
    field_values: &HashSet<T>,
    create_terms_fn: F,
) -> Vec<(Occur, Box<dyn Query>)>
where
    F: Fn(&T) -> Result<Vec<Term>, GraphError>,
    T: Eq + std::hash::Hash,
{
    field_values
        .iter()
        .filter_map(|value| create_terms_fn(value).ok())
        .flat_map(|terms| {
            terms.into_iter().map(|term| {
                (
                    Should,
                    Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
                )
            })
        })
        .collect()
}

fn create_property_sub_queries(
    property_index: &PropertyIndex,
    prop_field: Field,
    prop_values: &HashSet<Prop>,
) -> Vec<(Occur, Box<dyn Query>)> {
    create_sub_queries_generic(prop_values, |value| {
        create_property_tantivy_terms(property_index, prop_field, value)
    })
}

fn create_tantivy_terms(
    schema: &Schema,
    tokenizer_manager: &TokenizerManager,
    field: Field,
    field_value: &str,
) -> Result<Vec<Term>, GraphError> {
    let field_entry = schema.get_field_entry(field.clone());
    let field_type = field_entry.field_type();
    let tokens = get_str_field_tokens(tokenizer_manager, &field_type, field_value)?;
    create_terms_from_tokens(field, tokens)
}

fn create_sub_queries(
    schema: &Schema,
    tokenizer_manager: &TokenizerManager,
    field: Field,
    field_values: &HashSet<String>,
) -> Vec<(Occur, Box<dyn Query>)> {
    create_sub_queries_generic(field_values, |value| {
        create_tantivy_terms(schema, tokenizer_manager, field, value)
    })
}

fn create_terms_from_tokens(field: Field, tokens: Vec<String>) -> Result<Vec<Term>, GraphError> {
    Ok(tokens
        .into_iter()
        .map(|value| Term::from_field_text(field, value.as_ref()))
        .collect_vec())
}

// Creates a Tantivy query from a vector of terms.
// - For multiple terms (e.g., when tokenizing a string field with multiple tokens),
//   a `PhraseQuery` is created to match documents containing the terms in sequence.
// - For a single term (e.g., from a non-string field or a single-token string field),
//   a `TermQuery` is created to match documents containing the exact term.
// - If no terms are provided, the function panics, as a valid query cannot be constructed.
fn create_eq_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        1 => Some(Box::new(TermQuery::new(
            terms[0].clone(),
            IndexRecordOption::Basic,
        ))),
        _ => Some(Box::new(PhraseQuery::new(terms))),
    }
}

fn create_ne_query(terms: Vec<Term>) -> Option<Box<dyn Query>> {
    if terms.is_empty() {
        return None;
    }

    let must_not_queries: Vec<(Occur, Box<dyn Query>)> = terms
        .into_iter()
        .map(|term| {
            (
                MustNot,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)) as Box<dyn Query>,
            )
        })
        .collect();

    Some(Box::new(BooleanQuery::new(
        vec![(Should, Box::new(AllQuery) as Box<dyn Query>)] // Include all documents
            .into_iter()
            .chain(must_not_queries) // Exclude documents matching any term
            .collect(),
    )))
}

fn create_lt_query(
    prop_name: String,
    prop_field_type: Type,
    terms: Vec<Term>,
) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        _ => Some(Box::new(RangeQuery::new_term_bounds(
            prop_name,
            prop_field_type,
            &Bound::Unbounded,
            &Bound::Excluded(terms.get(0).unwrap().clone()),
        ))),
    }
}

fn create_le_query(
    prop_name: String,
    prop_field_type: Type,
    terms: Vec<Term>,
) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        _ => Some(Box::new(RangeQuery::new_term_bounds(
            prop_name.to_string(),
            prop_field_type,
            &Bound::Unbounded,
            &Bound::Included(terms.get(0).unwrap().clone()),
        ))),
    }
}

fn create_gt_query(
    prop_name: String,
    prop_field_type: Type,
    terms: Vec<Term>,
) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        _ => Some(Box::new(RangeQuery::new_term_bounds(
            prop_name.to_string(),
            prop_field_type,
            &Bound::Excluded(terms.get(0).unwrap().clone()),
            &Bound::Unbounded,
        ))),
    }
}

fn create_ge_query(
    prop_name: String,
    prop_field_type: Type,
    terms: Vec<Term>,
) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        _ => Some(Box::new(RangeQuery::new_term_bounds(
            prop_name.to_string(),
            prop_field_type,
            &Bound::Included(terms.get(0).unwrap().clone()),
            &Bound::Unbounded,
        ))),
    }
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

fn create_fuzzy_search_query(
    terms: Vec<Term>,
    levenshtein_distance: &usize,
    prefix_match: &bool,
) -> Option<Box<dyn Query>> {
    match terms.len() {
        0 => None,
        1 => {
            if *prefix_match {
                Some(Box::new(FuzzyTermQuery::new_prefix(
                    terms[0].clone(),
                    (*levenshtein_distance) as u8,
                    true,
                )))
            } else {
                Some(Box::new(FuzzyTermQuery::new(
                    terms[0].clone(),
                    (*levenshtein_distance) as u8,
                    true,
                )))
            }
        }
        _ => Some(Box::new(PhraseQuery::new(terms))), // TODO: Refer composite filter fuzzy searching based on strsim::levenshtein
    }
}
