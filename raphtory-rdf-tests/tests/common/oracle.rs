//! The oracle the suites compare Raphtory with: spareval over an `oxrdf::Dataset`, with the
//! `raphtory:` prefix registered as `RdfView::sparql` registers it.
//!
//! Included by `bear.rs` with `#[path]`.
use oxigraph::{
    model::Dataset,
    sparql::{QueryResults, SparqlEvaluator},
};
use raphtory_rdf::{SparqlResults, RESERVED_NS};
use std::collections::HashSet;

/// The results of `query` on `dataset`, collected as `RdfView::sparql` collects them (the
/// triples of `CONSTRUCT` and `DESCRIBE` without duplicates).
pub fn evaluate(dataset: &Dataset, query: &str) -> Result<SparqlResults, String> {
    let results = SparqlEvaluator::new()
        .with_prefix("raphtory", RESERVED_NS)
        .map_err(|e| e.to_string())?
        .parse_query(query)
        .map_err(|e| e.to_string())?
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(|e| e.to_string())?;
    let collected = match results {
        QueryResults::Solutions(solutions) => {
            let variables = solutions.variables().to_vec();
            let rows = solutions
                .map(|solution| solution.map(|solution| solution.values().to_vec()))
                .collect::<Result<_, _>>();
            rows.map(|rows| SparqlResults::Solutions { variables, rows })
        }
        QueryResults::Boolean(value) => Ok(SparqlResults::Boolean(value)),
        QueryResults::Graph(triples) => {
            let mut seen = HashSet::new();
            triples
                .filter(|triple| triple.as_ref().map_or(true, |t| seen.insert(t.clone())))
                .collect::<Result<_, _>>()
                .map(SparqlResults::Graph)
        }
    };
    collected.map_err(|e| e.to_string())
}
