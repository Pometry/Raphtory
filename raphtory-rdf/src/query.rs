//! Running SPARQL queries over graph views.
use crate::{
    algebra::{walk, Node},
    dataset::RaphtoryDataset,
    functions::temporal_functions_of,
    limits::{check_query_length, check_triple_patterns},
    time_graph::time_graphs_in,
    RdfError, TimeGraph, RESERVED_NS,
};
use indexmap::IndexSet;
use oxigraph::{
    model::{NamedNode, Term, Triple},
    sparql::{
        DefaultServiceHandler, QueryEvaluationError, QueryResults, QuerySolutionIter,
        SparqlEvaluator, Variable,
    },
};
use oxiri::Iri;
use raphtory::db::api::view::DynamicGraph;
use rayon::ThreadPoolBuilder;
use rustc_hash::{FxBuildHasher, FxHashSet};
use spargebra::{
    algebra::{Expression, Function, GraphPattern},
    term::NamedNodePattern,
    Query, SparqlParser,
};
use std::{
    io,
    panic::{self, AssertUnwindSafe},
};

// refuses `SERVICE` calls so no query makes a network request, whatever oxigraph features are on
pub(crate) fn evaluator() -> SparqlEvaluator {
    SparqlEvaluator::new().with_default_service_handler(NoService)
}

struct NoService;

impl DefaultServiceHandler for NoService {
    type Error = QueryEvaluationError;

    fn handle(
        &self,
        service_name: &NamedNode,
        _pattern: &GraphPattern,
        _base_iri: Option<&Iri<String>>,
    ) -> Result<QuerySolutionIter<'static>, Self::Error> {
        Err(QueryEvaluationError::UnsupportedService(
            service_name.clone(),
        ))
    }
}

/// The fully collected results of a SPARQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparqlResults {
    /// `SELECT`: one row per solution, `None` where a variable is unbound.
    Solutions {
        variables: Vec<Variable>,
        rows: Vec<Vec<Option<Term>>>,
    },
    /// `ASK`.
    Boolean(bool),
    /// `CONSTRUCT` or `DESCRIBE`: distinct triples, in the order first built.
    Graph(Vec<Triple>),
}

impl SparqlResults {
    pub(crate) fn from_query_results(results: QueryResults<'_>) -> Result<Self, RdfError> {
        Ok(match results {
            QueryResults::Solutions(solutions) => {
                let variables = solutions.variables().to_vec();
                let rows = solutions
                    .map(|solution| solution.map(|solution| solution.values().to_vec()))
                    .collect::<Result<_, _>>()
                    .map_err(RdfError::from)?;
                Self::Solutions { variables, rows }
            }
            QueryResults::Boolean(value) => Self::Boolean(value),
            QueryResults::Graph(triples) => Self::Graph(
                triples
                    .collect::<Result<IndexSet<_, FxBuildHasher>, _>>()
                    .map_err(RdfError::from)?
                    .into_iter()
                    .collect(),
            ),
        })
    }
}

/// The form of a SPARQL query, which decides what its results are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryForm {
    Select,
    Ask,
    Construct,
    Describe,
}

impl QueryForm {
    pub(crate) fn of(query: &Query) -> Self {
        match query {
            Query::Select { .. } => Self::Select,
            Query::Ask { .. } => Self::Ask,
            Query::Construct { .. } => Self::Construct,
            Query::Describe { .. } => Self::Describe,
        }
    }

    /// The keyword of the form, such as `SELECT`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Ask => "ASK",
            Self::Construct => "CONSTRUCT",
            Self::Describe => "DESCRIBE",
        }
    }

    /// Whether the results are triples (`CONSTRUCT` and `DESCRIBE`).
    pub fn is_graph(self) -> bool {
        matches!(self, Self::Construct | Self::Describe)
    }
}

/// What a SPARQL query asks for, read from the query without running it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOutline {
    pub form: QueryForm,
    /// Whether the query may read a time graph; one only passed to a function is not read.
    pub reads_time_graphs: bool,
}

impl QueryOutline {
    /// Parses `query` as [`RdfView::sparql`](crate::RdfView::sparql) does, with the same errors.
    pub fn parse(query: &str, options: &SparqlOptions) -> Result<Self, RdfError> {
        check_query_length(query, options.max_query_length)?;
        on_stack("the query", query.len(), || {
            let query = parse(query)?;
            let from_names_time_graph = query
                .dataset()
                .into_iter()
                .flat_map(|dataset| dataset.default.iter().chain(dataset.named.iter().flatten()))
                .any(|iri| matches!(TimeGraph::parse(iri), Ok(Some(_))));
            let (mut has_graph, mut graph_var, mut builds_iri) = (false, false, false);
            walk(&query, |node| match node {
                Node::Pattern(GraphPattern::Graph { name, .. }) => {
                    has_graph = true;
                    graph_var |= matches!(name, NamedNodePattern::Variable(_));
                }
                Node::Expression(Expression::FunctionCall(Function::Iri, _)) => builds_iri = true,
                _ => {}
            });
            Ok(Self {
                form: QueryForm::of(&query),
                reads_time_graphs: from_names_time_graph
                    || (has_graph && !time_graphs_in(&query).is_empty())
                    || (graph_var && builds_iri),
            })
        })
    }
}

/// Bounds on the work of a SPARQL query; the default has none.
#[derive(Clone, Debug, Default)]
pub struct SparqlOptions {
    pub(crate) max_query_length: Option<usize>,
    pub(crate) max_triple_patterns: Option<usize>,
    pub(crate) max_result_bytes: Option<usize>,
}

impl SparqlOptions {
    /// The longest query, in bytes; a longer one fails before it is parsed.
    pub fn with_max_query_length(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_query_length = max.into();
        self
    }

    /// The most triple patterns a query may have; more fails before planning.
    /// Path predicates, `BIND`s and `(.. AS ?v)` count one each; `VALUES` 1 + variables + rows/100.
    pub fn with_max_triple_patterns(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_triple_patterns = max.into();
        self
    }

    /// The most bytes of results [`sparql_to_writer`](crate::RdfView::sparql_to_writer) writes.
    pub fn with_max_result_bytes(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_result_bytes = max.into();
        self
    }
}

// spareval does not dedup triples with blank nodes (stored ones repeat), and forgets after 2^20
pub(crate) fn dedup_triples<'a>(
    triples: impl Iterator<Item = Result<Triple, QueryEvaluationError>> + 'a,
) -> impl Iterator<Item = Result<Triple, QueryEvaluationError>> + 'a {
    let mut seen = FxHashSet::default();
    triples.filter(move |triple| match triple {
        Ok(triple) => seen.insert(triple.clone()),
        Err(_) => true,
    })
}

// a stack overflow aborts the process, so recursion as deep as `len` input units needs room;
// oxigraph recurses per nesting level and flat-chain term, rudof per shapes graph nesting level.
// A one-thread pool rather than a thread, as rudof's validator uses `par_iter`.
pub(crate) fn on_stack<R: Send>(
    what: &str,
    len: usize,
    f: impl FnOnce() -> Result<R, RdfError> + Send,
) -> Result<R, RdfError> {
    ThreadPoolBuilder::new()
        .num_threads(1)
        .thread_name(|_| "raphtory-rdf".to_owned())
        .stack_size((16usize << 20).saturating_add(len.saturating_mul(32 << 10)))
        .build()
        .map_err(io::Error::other)?
        .install(|| panic::catch_unwind(AssertUnwindSafe(f)))
        .unwrap_or_else(|panic| Err(RdfError::panicked(what, panic)))
}

pub(crate) fn parse(query: &str) -> Result<Query, RdfError> {
    Ok(SparqlParser::new()
        .with_prefix("raphtory", RESERVED_NS)?
        .parse_query(query)?)
}

pub(crate) fn execute<R>(
    view: &DynamicGraph,
    query: Query,
    options: &SparqlOptions,
    f: impl FnOnce(QueryResults<'_>) -> Result<R, RdfError>,
) -> Result<R, RdfError> {
    check_triple_patterns(&query, options.max_triple_patterns)?;
    // time graphs cannot be enumerated, so an unbound `GRAPH ?g` visits those the query names
    let dataset = RaphtoryDataset::new(view.clone()).with_time_graphs(time_graphs_in(&query));
    let results = temporal_functions_of(evaluator(), view.clone())
        .for_query(query)
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(RdfError::from)?;
    f(results)
}

pub(crate) fn sparql(
    view: &DynamicGraph,
    query: &str,
    options: &SparqlOptions,
) -> Result<SparqlResults, RdfError> {
    check_query_length(query, options.max_query_length)?;
    on_stack("the query", query.len(), || {
        execute(
            view,
            parse(query)?,
            options,
            SparqlResults::from_query_results,
        )
    })
}
