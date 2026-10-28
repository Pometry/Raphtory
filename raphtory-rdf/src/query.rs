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
        on_query_stack(query, || {
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

/// The deepest the brackets of a SPARQL query may nest; deeper fails before parsing.
/// The parser and evaluator recurse per level, and a stack overflow aborts the process.
pub const MAX_SPARQL_NESTING: usize = 128;

// long flat queries recurse deeply too, so the stack grows with the query
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

// follows every reading of `<` at once; assumes spargebra's standard-unicode-escaping is off
fn check_nesting(query: &str) -> Result<(), RdfError> {
    // the deepest nesting of the readings in each mode, or `None`
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
    on_query_stack(query, || {
        execute(
            view,
            parse(query)?,
            options,
            SparqlResults::from_query_results,
        )
    })
}
