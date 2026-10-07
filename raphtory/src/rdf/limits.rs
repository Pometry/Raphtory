//! Bounds on the work a SPARQL query does: its size, its time and cancellation (see
//! [`SparqlOptions`](crate::rdf::SparqlOptions)).
use crate::rdf::RdfError;
use oxigraph::sparql::CancellationToken;
use parking_lot::{Condvar, Mutex};
use spargebra::{
    algebra::{
        AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
    },
    Query,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    panic::{self, AssertUnwindSafe},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Relaxed},
        Arc, Weak,
    },
    thread,
    time::{Duration, Instant},
};

/// The state of an [`Interrupt`] while its query may run on.
const RUNNING: u8 = 0;
/// The state of an [`Interrupt`] whose caller's token was cancelled.
const CANCELLED: u8 = 1;
/// The state of an [`Interrupt`] whose deadline has passed.
const TIMED_OUT: u8 = 2;

/// Says when a running query must stop: once the caller's [`CancellationToken`] is cancelled,
/// or once its timeout has passed.
///
/// It never cancels the caller's token. The deadline is kept by the [`Timer`] thread, so a
/// check is a couple of atomic loads.
///
/// Checks are never made while a storage lock is held. Under [`run_interruptible`] (builds that
/// unwind) a check that fires unwinds at once, so a stop never alters what spareval computes
/// (an error turned into an unbound `ORDER BY` key could make its sort panic). Without
/// unwinding, scans end early and the dataset returns an error. Either way the query ends with
/// [`error`](Self::error).
pub(crate) struct Interrupt {
    /// The caller's token: once it is cancelled, the query must stop.
    caller: Option<CancellationToken>,
    /// [`RUNNING`], [`CANCELLED`] or [`TIMED_OUT`] (set by the timer at the deadline).
    state: Arc<AtomicU8>,
    /// Whether a check saw that the query must stop (then its results are not complete).
    seen: AtomicBool,
    /// The time limit of the query.
    timeout: Option<Duration>,
    /// The deadline in the timer, removed from it when the query is done.
    _timer: Option<TimerEntry>,
    /// The deadline, if no timer thread could be started (checks then read the clock).
    polled: Option<Instant>,
    /// The number of checks (tests count them).
    #[cfg(test)]
    checks: std::sync::atomic::AtomicU32,
}

impl Interrupt {
    /// An interrupt that fires when `caller` is cancelled, or `timeout` from now.
    pub(crate) fn new(caller: Option<CancellationToken>, timeout: Option<Duration>) -> Self {
        let state = Arc::new(AtomicU8::new(RUNNING));
        let (mut timer, mut polled) = (None, None);
        let now = Instant::now();
        // a timeout too long to add to `now` never fires
        if let Some(deadline) = timeout.and_then(|timeout| now.checked_add(timeout)) {
            if deadline <= now {
                state.store(TIMED_OUT, Relaxed);
            } else if let Some(t) = Timer::get() {
                timer = Some(t.schedule(deadline, &state));
            } else {
                polled = Some(deadline);
            }
        }
        Self {
            caller,
            state,
            seen: AtomicBool::new(false),
            timeout,
            _timer: timer,
            polled,
            #[cfg(test)]
            checks: Default::default(),
        }
    }

    /// Whether the query must stop.
    pub(crate) fn fired(&self) -> bool {
        #[cfg(test)]
        self.checks.fetch_add(1, Relaxed);
        let fired = self.state.load(Relaxed) != RUNNING
            || (self
                .caller
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
                && self.stop(CANCELLED))
            || (self
                .polled
                .is_some_and(|deadline| Instant::now() >= deadline)
                && self.stop(TIMED_OUT));
        if fired {
            self.seen.store(true, Relaxed);
        }
        fired
    }

    /// Marks the query stopped for `reason`, unless it is stopped already. Returns `true`.
    fn stop(&self, reason: u8) -> bool {
        let _ = self
            .state
            .compare_exchange(RUNNING, reason, Relaxed, Relaxed);
        true
    }

    /// Whether the query must stop, for the checks of the scans: if it must and
    /// [`run_interruptible`] runs it on this thread, this unwinds there instead of returning.
    pub(crate) fn must_stop(&self) -> bool {
        if !self.fired() {
            return false;
        }
        unwind_if_running(self);
        true
    }

    /// Unwinds to [`run_interruptible`] if the query must stop and runs under it on this thread;
    /// otherwise does nothing, for checks that must not change a value spareval computes with.
    pub(crate) fn unwind_if_fired(&self) {
        if ARMED.with(Cell::get) && self.fired() {
            unwind_if_running(self);
        }
    }

    /// The error a query that was stopped ends with: [`RdfError::Timeout`] if its deadline
    /// passed, [`RdfError::Cancelled`] if the caller's token was cancelled, or `None` if no
    /// check saw that it must stop (its results are complete).
    pub(crate) fn error(&self) -> Option<RdfError> {
        if !self.seen.load(Relaxed) {
            return None;
        }
        Some(match self.state.load(Relaxed) {
            TIMED_OUT => RdfError::Timeout {
                timeout: self.timeout.unwrap_or_default(),
            },
            _ => RdfError::Cancelled,
        })
    }

    /// The number of checks so far.
    #[cfg(test)]
    pub(crate) fn checks(&self) -> u32 {
        self.checks.load(Relaxed)
    }

    /// Whether the deadline of the interrupt is in the timer, as a check that outlives it.
    #[cfg(test)]
    pub(crate) fn deadline_in_timer(&self) -> impl Fn() -> bool + use<> {
        let entry = self
            ._timer
            .as_ref()
            .map(|entry| (entry.timer.clone(), entry.key));
        move || {
            entry
                .as_ref()
                .is_some_and(|(timer, key)| timer.deadlines.lock().contains_key(key))
        }
    }
}

/// Fires the deadlines of the queries that run with a timeout: one thread for the process,
/// started by the first such query, which sleeps until the earliest deadline.
struct Timer {
    /// The process that started the thread; a forked process starts its own.
    pid: u32,
    /// The deadlines, earliest first, each with the state of its interrupt.
    deadlines: Mutex<BTreeMap<(Instant, u64), Weak<AtomicU8>>>,
    /// Numbers the deadlines, so that equal instants are kept apart.
    next_id: AtomicU64,
    /// Wakes the thread when a deadline earlier than all others is added.
    wake: Condvar,
}

/// The timer of the process, once started.
static TIMER: Mutex<Option<Arc<Timer>>> = Mutex::new(None);

impl Timer {
    /// The timer of the process, started if need be, or `None` if its thread cannot be
    /// started.
    fn get() -> Option<Arc<Self>> {
        let pid = std::process::id();
        let mut current = TIMER.lock();
        if let Some(timer) = current.as_ref().filter(|timer| timer.pid == pid) {
            return Some(timer.clone());
        }
        let timer = Arc::new(Self {
            pid,
            deadlines: Mutex::default(),
            next_id: AtomicU64::new(0),
            wake: Condvar::new(),
        });
        let run = timer.clone();
        thread::Builder::new()
            .name("raphtory-sparql-timer".to_owned())
            .spawn(move || run.run())
            .ok()?;
        *current = Some(timer.clone());
        Some(timer)
    }

    /// Fires the deadlines as they pass, for as long as the process runs.
    fn run(&self) {
        let mut deadlines = self.deadlines.lock();
        loop {
            let now = Instant::now();
            while let Some(entry) = deadlines.first_entry() {
                if entry.key().0 > now {
                    break;
                }
                // gone if the query is done
                if let Some(state) = entry.remove().upgrade() {
                    let _ = state.compare_exchange(RUNNING, TIMED_OUT, Relaxed, Relaxed);
                }
            }
            match deadlines
                .first_key_value()
                .map(|(&(deadline, _), _)| deadline)
            {
                Some(deadline) => {
                    self.wake.wait_until(&mut deadlines, deadline);
                }
                None => self.wake.wait(&mut deadlines),
            }
        }
    }

    /// Sets `state` to [`TIMED_OUT`] at `deadline`, unless the returned entry is dropped
    /// before.
    fn schedule(self: Arc<Self>, deadline: Instant, state: &Arc<AtomicU8>) -> TimerEntry {
        let key = (deadline, self.next_id.fetch_add(1, Relaxed));
        let mut deadlines = self.deadlines.lock();
        let earliest = deadlines
            .first_key_value()
            .is_none_or(|(first, _)| key < *first);
        deadlines.insert(key, Arc::downgrade(state));
        drop(deadlines);
        if earliest {
            self.wake.notify_one();
        }
        TimerEntry { timer: self, key }
    }
}

/// A deadline in the [`Timer`], removed from it when dropped.
struct TimerEntry {
    timer: Arc<Timer>,
    key: (Instant, u64),
}

impl Drop for TimerEntry {
    fn drop(&mut self) {
        self.timer.deadlines.lock().remove(&self.key);
    }
}

/// How many clones of internal terms check the [`Interrupt`] of the thread once.
const CHECK_EVERY_CLONES: u32 = 256;

thread_local! {
    /// The interrupt of the query this thread evaluates under [`run_interruptible`].
    static ACTIVE: RefCell<Option<Arc<Interrupt>>> = const { RefCell::new(None) };
    /// Whether `ACTIVE` is set (and no stop is unwinding): the one thing every clone reads.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    /// Clones since `ACTIVE` was last checked.
    static CLONES: Cell<u32> = const { Cell::new(0) };
}

/// The payload of the unwinding that stops a query.
struct Stopped;

/// Unwinds to [`run_interruptible`] if it runs `interrupt` on this thread (with
/// [`panic::resume_unwind`], which does not run the panic hook, so nothing is printed).
fn unwind_if_running(interrupt: &Interrupt) {
    if !ARMED.with(Cell::get) || thread::panicking() {
        return;
    }
    let running = ACTIVE.with_borrow(|active| {
        active
            .as_deref()
            .is_some_and(|active| ptr::eq(active, interrupt))
    });
    if running {
        // once: the clones and checks made while unwinding must not unwind again
        ARMED.with(|armed| armed.set(false));
        panic::resume_unwind(Box::new(Stopped));
    }
}

/// Called for every clone of an [`RdfTerm`](crate::rdf::RdfTerm).
///
/// Under [`run_interruptible`], every [`CHECK_EVERY_CLONES`] clones check the interrupt and
/// unwind once it has fired, which stops in-memory joins that read nothing from the dataset.
/// Unwinding is safe here: no storage lock is held where terms are cloned, the graph is only
/// read, and everything half done is dropped.
#[inline]
pub(crate) fn on_term_clone() {
    if ARMED.with(Cell::get) {
        check_on_clone();
    }
}

#[inline(never)]
fn check_on_clone() {
    let clones = CLONES.with(|clones| {
        let n = clones.get().wrapping_add(1);
        clones.set(n);
        n
    });
    if !clones.is_multiple_of(CHECK_EVERY_CLONES) {
        return;
    }
    let fired = ACTIVE.with_borrow(|active| active.as_deref().is_some_and(Interrupt::fired));
    if fired && !thread::panicking() {
        ARMED.with(|armed| armed.set(false));
        panic::resume_unwind(Box::new(Stopped));
    }
}

/// Restores the interrupt the thread had before [`run_interruptible`] set its own.
struct Restore(Option<Arc<Interrupt>>);

impl Drop for Restore {
    fn drop(&mut self) {
        let previous = self.0.take();
        ARMED.with(|armed| armed.set(previous.is_some()));
        ACTIVE.with(|active| *active.borrow_mut() = previous);
    }
}

/// Runs `f` (the evaluation of a query) so that the checks of `interrupt` stop it by unwinding
/// here. Returns `None` if it was stopped that way ([`Interrupt::error`] says why); other panics
/// go on unwinding. With `panic = "abort"` it just runs `f`.
pub(crate) fn run_interruptible<R>(interrupt: &Arc<Interrupt>, f: impl FnOnce() -> R) -> Option<R> {
    if !cfg!(panic = "unwind") {
        return Some(f());
    }
    let previous = ACTIVE.with(|active| active.borrow_mut().replace(interrupt.clone()));
    ARMED.with(|armed| armed.set(true));
    let restore = Restore(previous);
    let result = panic::catch_unwind(AssertUnwindSafe(f));
    drop(restore);
    match result {
        Ok(result) => Some(result),
        Err(payload) if payload.is::<Stopped>() => None,
        Err(payload) => panic::resume_unwind(payload),
    }
}

/// How many rows of `VALUES` count as much as a triple pattern (see [`count_triple_patterns`]).
pub(crate) const VALUES_ROWS_PER_PATTERN: usize = 100;

/// The size of a query that [`SparqlOptions::max_triple_patterns`] bounds: its triple patterns
/// and what binds their variables, which planning cost grows with.
///
/// [`SparqlOptions::max_triple_patterns`]: crate::rdf::SparqlOptions::max_triple_patterns
///
/// - every triple pattern anywhere in the query counts one (including `EXISTS`, sub-queries
///   and patterns the parser expands from collections and blank node property lists);
/// - a property path counts one per predicate it names (`p/q|^r` counts 3, `!(p|q)` 1);
/// - a `BIND`, and an expression `(.. AS ?v)` of `SELECT` or `GROUP BY`, counts one;
/// - a `VALUES` block counts one, plus one per variable, plus one per
///   [`VALUES_ROWS_PER_PATTERN`] rows.
///
/// The `CONSTRUCT` template is not counted. It walks the query with its own stack, so a query
/// of any length needs little of the thread's stack.
pub(crate) fn count_triple_patterns(query: &Query) -> usize {
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    let mut count = 0usize;
    let mut patterns = vec![pattern];
    let mut expressions: Vec<&Expression> = Vec::new();
    let mut paths: Vec<&PropertyPathExpression> = Vec::new();
    while !patterns.is_empty() || !expressions.is_empty() || !paths.is_empty() {
        if let Some(pattern) = patterns.pop() {
            match pattern {
                GraphPattern::Bgp { patterns } => count = count.saturating_add(patterns.len()),
                GraphPattern::Path { path, .. } => paths.push(path),
                GraphPattern::Join { left, right }
                | GraphPattern::Lateral { left, right }
                | GraphPattern::Union { left, right }
                | GraphPattern::Minus { left, right } => {
                    patterns.push(left);
                    patterns.push(right);
                }
                GraphPattern::LeftJoin {
                    left,
                    right,
                    expression,
                } => {
                    patterns.push(left);
                    patterns.push(right);
                    expressions.extend(expression);
                }
                GraphPattern::Filter { expr, inner } => {
                    expressions.push(expr);
                    patterns.push(inner);
                }
                GraphPattern::Extend {
                    inner, expression, ..
                } => {
                    count = count.saturating_add(1);
                    expressions.push(expression);
                    patterns.push(inner);
                }
                GraphPattern::Values {
                    variables,
                    bindings,
                } => {
                    count = count
                        .saturating_add(1)
                        .saturating_add(variables.len())
                        .saturating_add(bindings.len() / VALUES_ROWS_PER_PATTERN)
                }
                GraphPattern::OrderBy { inner, expression } => {
                    patterns.push(inner);
                    expressions.extend(expression.iter().map(|order| match order {
                        OrderExpression::Asc(e) | OrderExpression::Desc(e) => e,
                    }));
                }
                GraphPattern::Group {
                    inner, aggregates, ..
                } => {
                    patterns.push(inner);
                    expressions.extend(aggregates.iter().filter_map(
                        |(_, aggregate)| match aggregate {
                            AggregateExpression::CountSolutions { .. } => None,
                            AggregateExpression::FunctionCall { expr, .. } => Some(expr),
                        },
                    ));
                }
                GraphPattern::Graph { inner, .. }
                | GraphPattern::Project { inner, .. }
                | GraphPattern::Distinct { inner }
                | GraphPattern::Reduced { inner }
                | GraphPattern::Slice { inner, .. }
                | GraphPattern::Service { inner, .. } => patterns.push(inner),
            }
        } else if let Some(expression) = expressions.pop() {
            match expression {
                Expression::NamedNode(_)
                | Expression::Literal(_)
                | Expression::Variable(_)
                | Expression::Bound(_) => {}
                Expression::Or(a, b)
                | Expression::And(a, b)
                | Expression::Equal(a, b)
                | Expression::SameTerm(a, b)
                | Expression::Greater(a, b)
                | Expression::GreaterOrEqual(a, b)
                | Expression::Less(a, b)
                | Expression::LessOrEqual(a, b)
                | Expression::Add(a, b)
                | Expression::Subtract(a, b)
                | Expression::Multiply(a, b)
                | Expression::Divide(a, b) => {
                    expressions.push(a);
                    expressions.push(b);
                }
                Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
                    expressions.push(a)
                }
                Expression::In(a, list) => {
                    expressions.push(a);
                    expressions.extend(list);
                }
                Expression::If(a, b, c) => expressions.extend([&**a, &**b, &**c]),
                Expression::Coalesce(list) | Expression::FunctionCall(_, list) => {
                    expressions.extend(list)
                }
                Expression::Exists(pattern) => patterns.push(pattern),
            }
        } else if let Some(path) = paths.pop() {
            match path {
                PropertyPathExpression::NamedNode(_)
                | PropertyPathExpression::NegatedPropertySet(_) => count = count.saturating_add(1),
                PropertyPathExpression::Reverse(p)
                | PropertyPathExpression::ZeroOrMore(p)
                | PropertyPathExpression::OneOrMore(p)
                | PropertyPathExpression::ZeroOrOne(p) => paths.push(p),
                PropertyPathExpression::Sequence(a, b)
                | PropertyPathExpression::Alternative(a, b) => {
                    paths.push(a);
                    paths.push(b);
                }
            }
        }
    }
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
