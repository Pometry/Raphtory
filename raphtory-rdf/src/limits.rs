//! The size limit of a SPARQL query (see [`SparqlOptions`](crate::SparqlOptions)).
use crate::{
    algebra::{walk, Node},
    RdfError,
};
use spargebra::{
    algebra::{GraphPattern, PropertyPathExpression},
    Query,
};

/// How many rows of `VALUES` count as much as a triple pattern (see [`count_triple_patterns`]).
pub(crate) const VALUES_ROWS_PER_PATTERN: usize = 100;

/// The size of `query` as [`with_max_triple_patterns`] counts it.
///
/// [`with_max_triple_patterns`]: crate::SparqlOptions::with_max_triple_patterns
pub(crate) fn count_triple_patterns(query: &Query) -> usize {
    let mut count = 0usize;
    walk(query, |node| {
        let weight = match node {
            Node::Pattern(GraphPattern::Bgp { patterns }) => patterns.len(),
            Node::Pattern(GraphPattern::Extend { .. }) => 1,
            Node::Pattern(GraphPattern::Values {
                variables,
                bindings,
            }) => 1usize
                .saturating_add(variables.len())
                .saturating_add(bindings.len() / VALUES_ROWS_PER_PATTERN),
            Node::Path(
                PropertyPathExpression::NamedNode(_)
                | PropertyPathExpression::NegatedPropertySet(_),
            ) => 1,
            _ => 0,
        };
        count = count.saturating_add(weight);
    });
    count
}

/// Fails with [`RdfError::TooManyPatterns`] if `query` counts more than `max` (see
/// [`count_triple_patterns`]).
pub(crate) fn check_triple_patterns(query: &Query, max: Option<usize>) -> Result<(), RdfError> {
    let Some(max) = max else {
        return Ok(());
    };
    let count = count_triple_patterns(query);
    if count > max {
        Err(RdfError::TooManyPatterns { count, max })
    } else {
        Ok(())
    }
}
