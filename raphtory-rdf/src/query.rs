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
use rustc_hash::{FxBuildHasher, FxHashSet};
use spargebra::{
    algebra::{Expression, Function, GraphPattern},
    term::NamedNodePattern,
    Query, SparqlParser,
};
use std::thread;

/// A [`SparqlEvaluator`] that refuses SPARQL `SERVICE` calls with
/// [`QueryEvaluationError::UnsupportedService`] (`SERVICE SILENT` gives one empty solution), so
/// no query makes a network request, whatever oxigraph features are on.
pub(crate) fn evaluator() -> SparqlEvaluator {
    SparqlEvaluator::new().with_default_service_handler(NoService)
}

/// The default `SERVICE` handler of [`evaluator`]: it refuses every call.
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
    /// The solutions of a `SELECT` query: one row per solution, with one value per variable
    /// (`None` where the variable is unbound).
    Solutions {
        variables: Vec<Variable>,
        rows: Vec<Vec<Option<Term>>>,
    },
    /// The result of an `ASK` query.
    Boolean(bool),
    /// The triples built by a `CONSTRUCT` or `DESCRIBE` query, without duplicates, in the order
    /// they were first built.
    Graph(Vec<Triple>),
}

impl SparqlResults {
    /// Collects query results, failing on the first evaluation error.
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
    /// `SELECT`: solutions.
    Select,
    /// `ASK`: a boolean.
    Ask,
    /// `CONSTRUCT`: triples.
    Construct,
    /// `DESCRIBE`: triples.
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

    /// Whether the results are triples (`CONSTRUCT` and `DESCRIBE`), written in an RDF format
    /// rather than a SPARQL results format.
    pub fn is_graph(self) -> bool {
        matches!(self, Self::Construct | Self::Describe)
    }
}

/// What a SPARQL query asks for, read from the query without running it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOutline {
    /// The form of the query.
    pub form: QueryForm,
    /// Whether the query reads a time graph: one is in `FROM` or `FROM NAMED`, the query names
    /// one and has a `GRAPH` pattern, or it has a `GRAPH ?g` pattern and builds an IRI with
    /// `IRI()`, as in `IRI(CONCAT("raphtory:asof:", ?t))`. A time graph that is only the
    /// argument of a function, such as the fourth argument of `raphtory:validFrom`, is not read.
    pub reads_time_graphs: bool,
}

impl QueryOutline {
    /// Parses `query` as [`RdfView::sparql`](crate::RdfView::sparql) does with `options`, and
    /// fails with the same length and syntax errors.
    pub fn parse(query: &str, options: &SparqlOptions) -> Result<Self, RdfError> {
        check_query_length(query, options.max_query_length)?;
        on_query_stack(query, || {
            let query = parse(query)?;
            let (Query::Select { dataset, .. }
            | Query::Ask { dataset, .. }
            | Query::Construct { dataset, .. }
            | Query::Describe { dataset, .. }) = &query;
            let from_names_time_graph = dataset
                .iter()
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

/// Options for running a SPARQL query, passed to [`RdfView::sparql`](crate::RdfView::sparql)
/// and [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer).
///
/// Build it with `SparqlOptions::default()` and the `with_*` methods.
///
/// # Bounding the work of a query
///
/// By default a query runs until it is done. To run queries you do not control, bound them:
/// - [`with_max_query_length`](Self::with_max_query_length) rejects a query that is too long
///   before it is parsed,
/// - [`with_max_triple_patterns`](Self::with_max_triple_patterns) rejects a query that is too
///   large before it is planned (planning grows steeply with the number of joined patterns),
/// - [`with_max_result_bytes`](Self::with_max_result_bytes) stops writing results that are too
///   large.
///
/// # Example
/// ```
/// use raphtory::prelude::*;
/// use raphtory_rdf::{RdfGraph, SparqlOptions};
///
/// let rg = RdfGraph::new();
/// rg.graph().add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
///
/// // bounded queries, as a server would run them
/// let options = SparqlOptions::default()
///     .with_max_query_length(10_000)
///     .with_max_triple_patterns(100)
///     .with_max_result_bytes(1 << 20);
/// assert!(rg.sparql("ASK { ?s raphtory:knows ?o }", &options).is_ok());
/// ```
#[derive(Clone, Debug, Default)]
pub struct SparqlOptions {
    pub(crate) max_query_length: Option<usize>,
    pub(crate) max_triple_patterns: Option<usize>,
    pub(crate) max_result_bytes: Option<usize>,
}

impl SparqlOptions {
    /// The longest query, in bytes, or `None` for no limit (the default); a longer one fails
    /// with [`RdfError::QueryTooLong`] before it is parsed.
    pub fn with_max_query_length(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_query_length = max.into();
        self
    }

    /// The most triple patterns a query may have, or `None` for no limit (the default); more
    /// fails with [`RdfError::TooManyPatterns`] before planning. Every triple pattern anywhere
    /// in the query counts one (including those from collections and blank node property
    /// lists); a property path counts one per predicate; a `BIND` or `(.. AS ?v)` counts one; a
    /// `VALUES` block counts one, plus one per variable and per 100 rows. The `CONSTRUCT`
    /// template does not count.
    pub fn with_max_triple_patterns(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_triple_patterns = max.into();
        self
    }

    /// The most bytes of results [`sparql_to_writer`](crate::RdfView::sparql_to_writer) writes,
    /// or `None` for no limit (the default); larger results fail with
    /// [`RdfError::ResultsTooLarge`], and the writer holds at most that many bytes of them.
    /// [`sparql`](crate::RdfView::sparql), which writes nothing, ignores it.
    pub fn with_max_result_bytes(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_result_bytes = max.into();
        self
    }
}

/// Removes the duplicates of a CONSTRUCT or DESCRIBE result, keeping the first of each triple.
///
/// spareval does not deduplicate triples with blank nodes (it assumes the template minted
/// them, but blank nodes stored in the graph repeat) and forgets after 2^20 triples. This keeps
/// every triple it returned in memory.
pub(crate) fn dedup_triples<'a>(
    triples: impl Iterator<Item = Result<Triple, QueryEvaluationError>> + 'a,
) -> impl Iterator<Item = Result<Triple, QueryEvaluationError>> + 'a {
    let mut seen = FxHashSet::default();
    triples.filter(move |triple| match triple {
        Ok(triple) => seen.insert(triple.clone()),
        Err(_) => true,
    })
}

/// The deepest that the brackets `(`, `{` and `[` of a SPARQL query may nest. A query that
/// nests them deeper fails with [`RdfError::SparqlTooDeep`] before it is parsed.
///
/// The parser and evaluator recurse per level, so deeper nesting could overflow the stack,
/// which aborts the process.
pub const MAX_SPARQL_NESTING: usize = 128;

/// Runs `f`, which parses and evaluates `query`, on a thread with a stack of 16 MiB plus 8 KiB
/// per byte of the query; a panic becomes [`RdfError::Panicked`]. A stack overflow
/// aborts the process, and long flat queries (such as `?s ?p (1 1 1 ...)`) recurse deeply too,
/// so a query of a few kilobytes can overflow a default stack. Only the stack the query uses is
/// memory.
///
/// A query whose brackets nest deeper than [`MAX_SPARQL_NESTING`] fails before the thread is
/// spawned.
pub(crate) fn on_query_stack<R: Send>(
    query: &str,
    f: impl FnOnce() -> Result<R, RdfError> + Send,
) -> Result<R, RdfError> {
    check_nesting(query)?;
    let stack_size = (16usize << 20).saturating_add(query.len().saturating_mul(8 << 10));
    thread::scope(|scope| {
        thread::Builder::new()
            .name("raphtory-sparql".to_owned())
            .stack_size(stack_size)
            .spawn_scoped(scope, f)?
            .join()
            .unwrap_or_else(|panic| Err(RdfError::panicked("the query", panic)))
    })
}

/// Fails with [`RdfError::SparqlTooDeep`] if the brackets of `query` might nest deeper than
/// [`MAX_SPARQL_NESTING`] where the parser reads them.
///
/// Brackets in strings, IRIs and comments do not nest, but `<` may start an IRI or be
/// less-than, so the scan follows every possible reading at once and fails if any nests too
/// deep. It never counts less than the parser nests.
///
/// Relies on spargebra's `standard-unicode-escaping` feature being off, so `\uXXXX` is not
/// decoded outside strings and IRIs.
fn check_nesting(query: &str) -> Result<(), RdfError> {
    // Where a reading is: in code, an IRI, a comment, or a short or long string quoted with
    // `'` or `"`. Each holds the deepest nesting of the readings there, or `None`.
    const CODE: usize = 0;
    const IRI: usize = 1;
    const COMMENT: usize = 2;
    const SHORT: [usize; 2] = [3, 4];
    const LONG: [usize; 2] = [5, 6];
    type States = [Option<usize>; 7];

    let bytes = query.as_bytes();
    // The states at the next 3 positions: a token is at most 3 bytes long (`'''`).
    let mut pending: [States; 3] = [[None; 7]; 3];
    pending[0][CODE] = Some(0);
    let quote = |q: u8| usize::from(q == b'"');
    for (pos, &byte) in bytes.iter().enumerate() {
        let states = pending[0];
        pending = [pending[1], pending[2], [None; 7]];
        // the reading goes on `len` bytes later, in `mode`
        let mut next = |len: usize, mode: usize, depth: usize| {
            let state = &mut pending[len - 1][mode];
            *state = Some(state.map_or(depth, |d| d.max(depth)));
        };
        let at = |i: usize| bytes.get(pos + i).copied();
        if let Some(depth) = states[CODE] {
            match byte {
                b'(' | b'{' | b'[' => {
                    if depth >= MAX_SPARQL_NESTING {
                        let before = &query[..pos];
                        let line = before.matches('\n').count() + 1;
                        let column = before.rsplit('\n').next().unwrap_or("").chars().count() + 1;
                        return Err(RdfError::SparqlTooDeep {
                            max: MAX_SPARQL_NESTING,
                            line,
                            column,
                        });
                    }
                    next(1, CODE, depth + 1)
                }
                b')' | b'}' | b']' => next(1, CODE, depth.saturating_sub(1)),
                b'#' => next(1, COMMENT, depth),
                // `\(` is a character of a prefixed name such as `ex:a\(b`
                b'\\' => next(2, CODE, depth),
                b'\'' | b'"' => {
                    // `'''` starts a long string, or is the empty string `''` and a quote
                    if at(1) == Some(byte) && at(2) == Some(byte) {
                        next(3, LONG[quote(byte)], depth);
                    }
                    next(1, SHORT[quote(byte)], depth)
                }
                b'<' => {
                    next(1, CODE, depth);
                    next(1, IRI, depth)
                }
                _ => next(1, CODE, depth),
            }
        }
        if let Some(depth) = states[IRI] {
            match byte {
                b'>' => next(1, CODE, depth),
                // characters no IRI can hold end this reading
                0..=b' ' | 0x7f | b'<' | b'"' | b'{' | b'}' | b'|' | b'^' | b'`' => {}
                _ => next(1, IRI, depth),
            }
        }
        if let Some(depth) = states[COMMENT] {
            match byte {
                b'\n' | b'\r' => next(1, CODE, depth),
                _ => next(1, COMMENT, depth),
            }
        }
        for q in [b'\'', b'"'] {
            if let Some(depth) = states[SHORT[quote(q)]] {
                match byte {
                    b'\\' => next(2, SHORT[quote(q)], depth),
                    _ if byte == q => next(1, CODE, depth),
                    // a short string cannot hold a line break
                    b'\n' | b'\r' => {}
                    _ => next(1, SHORT[quote(q)], depth),
                }
            }
            if let Some(depth) = states[LONG[quote(q)]] {
                match byte {
                    b'\\' => next(2, LONG[quote(q)], depth),
                    _ if byte == q && at(1) == Some(q) && at(2) == Some(q) => next(3, CODE, depth),
                    _ => next(1, LONG[quote(q)], depth),
                }
            }
        }
    }
    Ok(())
}

/// Parses a query, with the `raphtory:` prefix registered, so queries can write
/// `raphtory:Alice` for the node `Alice` (see [`term_of`](crate::term_of)).
pub(crate) fn parse(query: &str) -> Result<Query, RdfError> {
    Ok(SparqlParser::new()
        .with_prefix("raphtory", RESERVED_NS)?
        .parse_query(query)?)
}

/// Evaluates a parsed query on the triples visible in `view` and hands the (lazy)
/// results to `f`, applying the limits of `options`.
pub(crate) fn execute<R>(
    view: &DynamicGraph,
    query: Query,
    options: &SparqlOptions,
    f: impl FnOnce(QueryResults<'_>) -> Result<R, RdfError>,
) -> Result<R, RdfError> {
    check_triple_patterns(&query, options.max_triple_patterns)?;
    // `GRAPH ?g` with `?g` unbound visits the time graphs the query names (unless it has
    // `FROM NAMED`).
    let dataset = RaphtoryDataset::new(view.clone()).with_time_graphs(time_graphs_in(&query));
    let results = temporal_functions_of(evaluator(), view.clone())
        .for_query(query)
        .on_queryable_dataset(dataset)
        .execute()
        .map_err(RdfError::from)?;
    f(results)
}

/// Body of [`RdfView::sparql`](crate::RdfView::sparql).
pub(crate) fn sparql(
    view: &DynamicGraph,
    query: &str,
    options: &SparqlOptions,
) -> Result<SparqlResults, RdfError> {
    check_query_length(query, options.max_query_length)?;
    on_query_stack(query, || {
        execute(
            view,
            parse(query)?,
            options,
            SparqlResults::from_query_results,
        )
    })
}
