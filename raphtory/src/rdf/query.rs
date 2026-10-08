//! Running SPARQL queries over graph views.
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    rdf::{
        dataset::RaphtoryDataset,
        functions::with_temporal_functions,
        limits::{check_triple_patterns, run_interruptible, Interrupt},
        time_graph::time_graphs_in,
        RdfError, RESERVED_NS,
    },
};
use oxigraph::{
    model::{NamedNode, Term, Triple},
    sparql::{
        CancellationToken, DefaultServiceHandler, QueryEvaluationError, QueryResults,
        QuerySolutionIter, QueryTripleIter, SparqlEvaluator, Variable,
    },
};
use oxiri::Iri;
use rustc_hash::FxHashSet;
use spargebra::{algebra::GraphPattern, Query, SparqlParser};
use std::{fmt, sync::Arc, time::Duration};

/// A [`SparqlEvaluator`] with the `raphtory:` prefix registered, so queries can write
/// `raphtory:Alice` for the node `Alice` (see [`term_of`](crate::rdf::term_of)), and with
/// SPARQL `SERVICE` calls refused.
///
/// A `SERVICE` call fails with [`QueryEvaluationError::Service`] (`SERVICE SILENT` gives one
/// empty solution), so no query makes a network request, whatever oxigraph features are on.
/// Register handlers with [`SparqlEvaluator::with_service_handler`] to answer some services
/// ([`SparqlEvaluator::with_default_service_handler`] replaces the refusal for the others).
///
/// Use it with a [`RaphtoryDataset`] for what [`RdfViewOps::sparql`](crate::rdf::RdfViewOps::sparql)
/// does not offer (custom functions, service handlers, substitution, streaming, explanations).
/// Unlike `sparql`, it does not list the time graphs the query names (see
/// [`RaphtoryDataset::with_time_graphs`]) and has no temporal functions (add them with
/// [`with_temporal_functions`](crate::rdf::with_temporal_functions)).
///
/// # Example
/// ```
/// use raphtory::{
///     prelude::*,
///     rdf::{evaluator, QueryResults, RaphtoryDataset},
/// };
///
/// let g = Graph::new();
/// g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
/// let results = evaluator()
///     .parse_query("ASK { raphtory:Alice raphtory:_default raphtory:Bob }")
///     .unwrap()
///     .on_queryable_dataset(RaphtoryDataset::new(g.clone()))
///     .execute()
///     .unwrap();
/// assert!(matches!(results, QueryResults::Boolean(true)));
/// ```
pub fn evaluator() -> SparqlEvaluator {
    SparqlEvaluator::new()
        .with_prefix("raphtory", RESERVED_NS)
        .expect("`raphtory:` is a valid IRI")
        .with_default_service_handler(NoService)
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
        Err(QueryEvaluationError::Service(
            format!(
                "SERVICE <{}> is not supported: Raphtory never sends SPARQL queries to other \
                 endpoints",
                service_name.as_str()
            )
            .into(),
        ))
    }
}

/// The fully collected results of a SPARQL query. Unlike [`QueryResults`] they are `Send`.
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
    pub fn from_query_results(results: QueryResults<'_>) -> Result<Self, GraphError> {
        Ok(match results {
            QueryResults::Solutions(solutions) => {
                let variables = solutions.variables().to_vec();
                let rows = solutions
                    .map(|solution| solution.map(|solution| solution.values().to_vec()))
                    .collect::<Result<_, _>>()?;
                Self::Solutions { variables, rows }
            }
            QueryResults::Boolean(value) => Self::Boolean(value),
            QueryResults::Graph(triples) => {
                Self::Graph(dedup_triples(triples).collect::<Result<_, _>>()?)
            }
        })
    }
}

/// Options for running a SPARQL query, passed to
/// [`RdfViewOps::sparql_with`](crate::rdf::RdfViewOps::sparql_with) and
/// [`RdfViewOps::sparql_to_writer_with`](crate::rdf::RdfViewOps::sparql_to_writer_with).
///
/// Build it with `SparqlOptions::default()` (what the methods without options use) and the
/// `with_*` methods.
///
/// # Bounding the work of a query
///
/// By default a query runs until it is done. To run queries you do not control, bound them:
/// - [`max_triple_patterns`](Self::max_triple_patterns) rejects a query that is too large
///   before it is planned (planning cannot be interrupted and grows steeply with the number of
///   joined patterns).
/// - [`timeout`](Self::timeout) stops evaluation once it has run that long, with
///   [`RdfError::Timeout`].
/// - [`cancellation_token`](Self::cancellation_token) lets another thread stop it, with
///   [`RdfError::Cancelled`].
///
/// A stopped query releases no partial results: [`sparql_with`] fails, and
/// [`sparql_to_writer_with`] fails after writing part of the document. Evaluation checks
/// between triples read, terms looked up, solutions built and results returned, so it stops
/// soon after its deadline, also during in-memory joins and sorts. Planning is not
/// interrupted, and in a build with `panic = "abort"` neither is work that reads nothing from
/// the graph (and a query stopped while sorting by an `EXISTS` key can then abort the process).
///
/// [`sparql_with`]: crate::rdf::RdfViewOps::sparql_with
/// [`sparql_to_writer_with`]: crate::rdf::RdfViewOps::sparql_to_writer_with
/// [`RdfError::Timeout`]: crate::rdf::RdfError::Timeout
/// [`RdfError::Cancelled`]: crate::rdf::RdfError::Cancelled
///
/// # Example
/// ```
/// use raphtory::{
///     errors::GraphError,
///     prelude::*,
///     rdf::{QueryResultsFormat, RdfError, RdfViewOps, SparqlOptions},
/// };
/// use std::time::Duration;
///
/// let g = PersistentGraph::new();
/// g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("knows")).unwrap();
/// let query = "SELECT (raphtory:validFromTime(raphtory:Alice, raphtory:knows, raphtory:Bob) AS ?t) {}";
///
/// let mut csv = Vec::new();
/// g.sparql_to_writer(query, &mut csv, QueryResultsFormat::Csv).unwrap();
/// assert_eq!(String::from_utf8(csv).unwrap(), "t\r\n1\r\n");
///
/// // without the temporal functions, the query fails
/// let options = SparqlOptions::default().with_temporal_functions(false);
/// assert!(g
///     .sparql_to_writer_with(query, &mut Vec::new(), QueryResultsFormat::Csv, &options)
///     .is_err());
///
/// // bounded queries, as a server would run them
/// let options = SparqlOptions::default()
///     .with_timeout(Duration::from_secs(30))
///     .with_max_triple_patterns(100);
/// assert!(g.sparql_with("ASK { ?s raphtory:knows ?o }", &options).is_ok());
/// let star: String = (0..101).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
/// let error = g.sparql_with(&format!("ASK {{ {star} }}"), &options).unwrap_err();
/// assert!(matches!(
///     error,
///     GraphError::Rdf(RdfError::TooManyPatterns { count: 101, max: 100 })
/// ));
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct SparqlOptions {
    /// Whether queries can call the temporal functions [`VALID_FROM`], [`VALID_TO`],
    /// [`VALID_FROM_TIME`] and [`VALID_TO_TIME`] (see
    /// [`with_temporal_functions`](crate::rdf::with_temporal_functions)). Defaults to `true`.
    /// Without them, a query that calls one fails with [`RdfError::SparqlEvaluation`].
    ///
    /// They read a triple's full history without the view's filters (but never report a triple
    /// the view hides), so a server that restricts callers with filters may turn them off.
    ///
    /// [`VALID_FROM`]: crate::rdf::VALID_FROM
    /// [`VALID_TO`]: crate::rdf::VALID_TO
    /// [`VALID_FROM_TIME`]: crate::rdf::VALID_FROM_TIME
    /// [`VALID_TO_TIME`]: crate::rdf::VALID_TO_TIME
    /// [`RdfError::SparqlEvaluation`]: crate::rdf::RdfError::SparqlEvaluation
    pub temporal_functions: bool,

    /// How long a query may run, including planning and writing results, before it is stopped
    /// with [`RdfError::Timeout`] (see
    /// [Bounding the work of a query](Self#bounding-the-work-of-a-query)). A timer thread,
    /// started by the first query with a timeout, keeps the deadlines. Defaults to `None`.
    ///
    /// [`RdfError::Timeout`]: crate::rdf::RdfError::Timeout
    pub timeout: Option<Duration>,

    /// The most triple patterns a query may have; more fails with
    /// [`RdfError::TooManyPatterns`] before planning. Every triple pattern anywhere in the query
    /// counts one (including those from collections and blank node property lists); a property
    /// path counts one per predicate; a `BIND` or `(.. AS ?v)` counts one; a `VALUES` block counts
    /// one, plus one per variable and per 100 rows. The `CONSTRUCT` template does not count.
    /// Defaults to `None`, no limit.
    ///
    /// [`RdfError::TooManyPatterns`]: crate::rdf::RdfError::TooManyPatterns
    pub max_triple_patterns: Option<usize>,

    /// A token that stops the query with [`RdfError::Cancelled`] when cancelled (also if
    /// cancelled before it starts). The query never cancels the token itself, so it can be shared
    /// and reused. Defaults to `None`.
    ///
    /// [`RdfError::Cancelled`]: crate::rdf::RdfError::Cancelled
    pub cancellation_token: Option<CancellationToken>,

    /// The RDF dataset of the query, replacing the one its `FROM` and `FROM NAMED` clauses
    /// give, as the `default-graph-uri` and `named-graph-uri` parameters of the SPARQL 1.1
    /// Protocol do. Defaults to `None`: the query's own dataset.
    pub dataset: Option<SparqlDataset>,
}

/// An RDF dataset for [`SparqlOptions::dataset`]: the graphs merged into the default graph and
/// the named graphs `GRAPH` can match. Only the default graph of the view and the time graphs
/// `<raphtory:asof:T>` hold triples; other IRIs name empty graphs.
///
/// # Example
/// ```
/// use raphtory::{
///     prelude::*,
///     rdf::{model::NamedNode, RdfFormat, SparqlDataset, SparqlOptions, SparqlResults},
/// };
///
/// let pg = PersistentGraph::new();
/// let doc = "<http://ex/alice> <http://ex/knows> <http://ex/bob> .";
/// pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
/// pg.retract_rdf(5, doc.as_bytes(), RdfFormat::NTriples, None).unwrap();
///
/// // the default graph is the graph as of 3
/// let options = SparqlOptions::default().with_dataset(SparqlDataset {
///     default_graphs: vec![NamedNode::new("raphtory:asof:3").unwrap()],
///     named_graphs: vec![],
/// });
/// let ask = "ASK { ?s ?p ?o }";
/// assert_eq!(pg.sparql_with(ask, &options).unwrap(), SparqlResults::Boolean(true));
/// assert_eq!(pg.sparql(ask).unwrap(), SparqlResults::Boolean(false));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SparqlDataset {
    /// The graphs merged into the default graph; none is an empty default graph.
    pub default_graphs: Vec<NamedNode>,
    /// The graphs a `GRAPH` pattern can match; none means `GRAPH` matches nothing.
    pub named_graphs: Vec<NamedNode>,
}

impl Default for SparqlOptions {
    fn default() -> Self {
        Self {
            temporal_functions: true,
            timeout: None,
            max_triple_patterns: None,
            cancellation_token: None,
            dataset: None,
        }
    }
}

impl fmt::Debug for SparqlOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SparqlOptions")
            .field("temporal_functions", &self.temporal_functions)
            .field("timeout", &self.timeout)
            .field("max_triple_patterns", &self.max_triple_patterns)
            .field(
                "cancellation_token",
                &self.cancellation_token.as_ref().map(|token| {
                    if token.is_cancelled() {
                        "cancelled"
                    } else {
                        "live"
                    }
                }),
            )
            .field("dataset", &self.dataset)
            .finish()
    }
}

impl SparqlOptions {
    /// Sets [`temporal_functions`](Self::temporal_functions).
    pub fn with_temporal_functions(mut self, on: bool) -> Self {
        self.temporal_functions = on;
        self
    }

    /// Sets [`timeout`](Self::timeout): a [`Duration`], or `None` for no limit.
    pub fn with_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }

    /// Sets [`max_triple_patterns`](Self::max_triple_patterns): a number, or `None` for no
    /// limit.
    pub fn with_max_triple_patterns(mut self, max: impl Into<Option<usize>>) -> Self {
        self.max_triple_patterns = max.into();
        self
    }

    /// Sets [`cancellation_token`](Self::cancellation_token). Keep a clone of the token to
    /// cancel the query with.
    ///
    /// # Example
    /// ```
    /// use raphtory::{
    ///     errors::GraphError,
    ///     prelude::*,
    ///     rdf::{CancellationToken, RdfError, RdfViewOps, SparqlOptions},
    /// };
    ///
    /// let g = Graph::new();
    /// g.add_edge(1, "Alice", "Bob", NO_PROPS, None).unwrap();
    /// let token = CancellationToken::new();
    /// let options = SparqlOptions::default().with_cancellation_token(token.clone());
    /// assert!(g.sparql_with("SELECT * { ?s ?p ?o }", &options).is_ok());
    ///
    /// token.cancel(); // from any thread
    /// let error = g.sparql_with("SELECT * { ?s ?p ?o }", &options).unwrap_err();
    /// assert!(matches!(error, GraphError::Rdf(RdfError::Cancelled)));
    /// ```
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Sets [`dataset`](Self::dataset): a [`SparqlDataset`], or `None` for the query's own.
    pub fn with_dataset(mut self, dataset: impl Into<Option<SparqlDataset>>) -> Self {
        self.dataset = dataset.into();
        self
    }
}

/// The time limit of `seconds` seconds, for [`SparqlOptions::with_timeout`]: `None` if
/// `seconds` is negative, NaN or infinite, and [`Duration::MAX`] if too large for a `Duration`.
///
/// # Example
/// ```
/// use raphtory::rdf::timeout_from_secs;
/// use std::time::Duration;
///
/// assert_eq!(timeout_from_secs(1.5), Some(Duration::from_millis(1500)));
/// assert_eq!(timeout_from_secs(0.0), Some(Duration::ZERO));
/// assert_eq!(timeout_from_secs(1e20), Some(Duration::MAX));
/// assert_eq!(timeout_from_secs(-1.0), None);
/// assert_eq!(timeout_from_secs(f64::INFINITY), None);
/// assert_eq!(timeout_from_secs(f64::NAN), None);
/// ```
pub fn timeout_from_secs(seconds: f64) -> Option<Duration> {
    if seconds.is_finite() && seconds >= Duration::MAX.as_secs_f64() {
        Some(Duration::MAX)
    } else {
        Duration::try_from_secs_f64(seconds).ok()
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

/// The stack size for a thread that runs a SPARQL query of `length` bytes: 16 MiB, plus 8 KiB
/// for every byte of the query.
///
/// Long flat queries (such as `?s ?p (1 1 1 ...)`) also make deep recursion, so a query of a
/// few kilobytes can overflow a default stack. Run queries you do not control on a thread with
/// this stack size (reserved address space: only what the query uses is memory).
pub fn sparql_stack_size(length: usize) -> usize {
    (16usize << 20).saturating_add(length.saturating_mul(8 << 10))
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

/// Parses a query, with the `raphtory:` prefix registered. A query whose brackets nest deeper
/// than [`MAX_SPARQL_NESTING`] fails before it is parsed.
pub(crate) fn parse(query: &str) -> Result<Query, GraphError> {
    check_nesting(query)?;
    Ok(SparqlParser::new()
        .with_prefix("raphtory", RESERVED_NS)
        .expect("`raphtory:` is a valid IRI")
        .parse_query(query)?)
}

/// Evaluates a parsed query on the triples visible in `view.valid()` and hands the (lazy)
/// results to `f`, applying the limits of `options`. A stopped query fails with
/// [`RdfError::Timeout`] or [`RdfError::Cancelled`], whatever `f` returned.
pub(crate) fn execute<G: StaticGraphViewOps + IntoDynamic, R>(
    view: &G,
    query: Query,
    options: &SparqlOptions,
    f: impl FnOnce(QueryResults<'_>) -> Result<R, GraphError>,
) -> Result<R, GraphError> {
    check_triple_patterns(&query, options.max_triple_patterns)?;
    let interrupt =
        (options.timeout.is_some() || options.cancellation_token.is_some()).then(|| {
            Arc::new(Interrupt::new(
                options.cancellation_token.clone(),
                options.timeout,
            ))
        });
    // a cancelled token or a zero timeout stops any query
    if let Some(error) = interrupt
        .as_deref()
        .filter(|interrupt| interrupt.fired())
        .and_then(Interrupt::error)
    {
        return Err(error.into());
    }
    // `GRAPH ?g` with `?g` unbound visits the time graphs the query names (unless it has
    // `FROM NAMED`).
    let dataset = RaphtoryDataset::new(view.clone())
        .with_time_graphs(time_graphs_in(&query))
        .with_interrupt(interrupt.clone());
    // spareval gets no cancellation token (it would make `EXISTS` see errors as matches); the
    // dataset checks the interrupt instead.
    let evaluator = if options.temporal_functions {
        with_temporal_functions(evaluator(), view.clone())
    } else {
        evaluator()
    };
    let mut prepared = evaluator.for_query(query);
    if let Some(graphs) = &options.dataset {
        let spec = prepared.dataset_mut();
        spec.set_default_graph(
            graphs
                .default_graphs
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
        );
        spec.set_available_named_graphs(
            graphs
                .named_graphs
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
        );
    }
    let checked = interrupt.clone();
    let run = move || {
        let results = prepared.on_queryable_dataset(dataset).execute()?;
        f(match checked {
            Some(interrupt) => interruptible(results, interrupt),
            None => results,
        })
    };
    let Some(interrupt) = interrupt else {
        return run();
    };
    let result = run_interruptible(&interrupt, run);
    // A stopped query may have lost triples, so whatever it returned is discarded.
    match (interrupt.error(), result) {
        (Some(error), _) => Err(error.into()),
        (None, Some(result)) => result,
        (None, None) => unreachable!("a query is only unwound once its interrupt has fired"),
    }
}

/// `results`, checking `interrupt` before each solution or triple.
fn interruptible(results: QueryResults<'_>, interrupt: Arc<Interrupt>) -> QueryResults<'_> {
    let check = move || {
        if interrupt.fired() {
            Err(QueryEvaluationError::Cancelled)
        } else {
            Ok(())
        }
    };
    match results {
        QueryResults::Solutions(solutions) => {
            let variables: Arc<[Variable]> = solutions.variables().into();
            QueryResults::Solutions(QuerySolutionIter::new(
                variables,
                solutions.map(move |solution| {
                    check()?;
                    solution
                }),
            ))
        }
        QueryResults::Graph(triples) => {
            QueryResults::Graph(QueryTripleIter::new(triples.map(move |triple| {
                check()?;
                triple
            })))
        }
        boolean @ QueryResults::Boolean(_) => boolean,
    }
}

/// Body of [`RdfViewOps::sparql_with`](crate::rdf::RdfViewOps::sparql_with).
pub(crate) fn sparql<G: StaticGraphViewOps + IntoDynamic>(
    view: &G,
    query: &str,
    options: &SparqlOptions,
) -> Result<SparqlResults, GraphError> {
    execute(
        view,
        parse(query)?,
        options,
        SparqlResults::from_query_results,
    )
}
