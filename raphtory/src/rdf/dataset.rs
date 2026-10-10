//! The SPARQL dataset of a graph view.
//!
//! [`RaphtoryDataset`] implements spareval's [`QueryableDataset`] on top of a graph view. The
//! default graph holds exactly the `(edge, layer)` pairs visible in the view, the same set
//! [`RdfView::to_rdf`](crate::rdf::RdfView::to_rdf) writes: each one is the quad
//! `term_of(src) layer_term(layer) term_of(dst)`. Named graphs are never stored; the only named
//! graphs are the virtual time graphs `<raphtory:asof:T>` ([`TimeGraph`]), which hold the same
//! quads for `view.snapshot_at(T)`. They cannot be enumerated, so the dataset lists only the
//! ones it is given ([`RaphtoryDataset::with_time_graphs`]).
use crate::{
    core::entities::{nodes::node_ref::NodeRef, GidRef},
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::{
        limits::{self, Interrupt},
        mapping::{layer_name_of, layer_term, name_of, term_of},
        scan::{in_layer, view_layers, Dir, EdgeScan},
        time_graph::TimeGraph,
        RdfError,
    },
};
use oxigraph::model::Term;
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, VID};
use raphtory_storage::graph::nodes::node_storage_ops::NodeStorageOps;
use rustc_hash::{FxHashMap, FxHashSet};
use spareval::{InternalQuad, QueryableDataset};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

/// Maximum number of entries in each of the per-dataset term caches.
const CACHE_CAP: usize = 1 << 20;

/// Maximum number of views in the per-dataset view cache (one per time and layer used).
const VIEW_CACHE_CAP: usize = 1 << 12;

/// Maximum number of triples a dataset keeps of the full scans it repeats (24 bytes each).
const SCAN_MEMO_CAP: usize = 1 << 20;

// A dataset can be built on one thread and evaluated on another.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<RaphtoryDataset>();
};

/// The internal form of an RDF term while a SPARQL query runs on a [`RaphtoryDataset`].
///
/// Every RDF term has exactly one internal value, so equality of internal terms is SPARQL
/// `sameTerm`. In order of precedence, a term is:
/// 1. [`Node`](RdfTerm::Node) if it is the term of a node name ([`term_of`]),
/// 2. otherwise [`Layer`](RdfTerm::Layer) if it is the term of a layer name ([`layer_term`]),
/// 3. otherwise [`TimeGraph`](RdfTerm::TimeGraph) if it is an IRI under
///    [`ASOF_NS`](crate::rdf::ASOF_NS) (no node or layer name maps to such an IRI). One whose
///    time does not parse is an error, [`RdfError::InvalidTimeGraph`], which aborts the query,
/// 4. otherwise [`Other`](RdfTerm::Other).
///
/// So a predicate IRI that is also a node name is the `Node` in every position, and the two join.
/// The internal value of each layer's term is fixed when the [`RaphtoryDataset`] is built.
#[derive(PartialEq, Eq, Hash, Debug)]
pub(crate) enum RdfTerm {
    /// A node of the graph. Its RDF term is `term_of(node_name)`.
    Node(VID),
    /// A layer whose term is not also the term of a node. Its RDF term is
    /// `layer_term(layer_name)`.
    Layer(LayerId),
    /// A time graph `<raphtory:asof:T>`. Its RDF term is its IRI, as written in the query.
    TimeGraph(Arc<TimeGraph>),
    /// A term that names neither a node nor a layer of the graph nor a time graph, for example
    /// a constant of the query that is not in the data.
    Other(Term),
}

/// Cloning also checks, every few hundred clones, whether an interrupted query must stop
/// (spareval clones terms even where it reads nothing from the dataset).
impl Clone for RdfTerm {
    #[inline]
    fn clone(&self) -> Self {
        limits::on_term_clone();
        match self {
            Self::Node(v) => Self::Node(*v),
            Self::Layer(l) => Self::Layer(*l),
            Self::TimeGraph(graph) => Self::TimeGraph(graph.clone()),
            Self::Other(term) => Self::Other(term.clone()),
        }
    }
}

/// A graph view as a SPARQL dataset, for use with an [`evaluator`](crate::rdf::query::evaluator)
/// (`evaluator().parse_query(q)?.on_queryable_dataset(RaphtoryDataset::new(view))`).
///
/// - The default graph holds one triple per `(edge, layer)` pair visible in the view:
///   `term_of(src) layer_term(layer) term_of(dst)`. So `rg.valid()` is the dataset of the
///   current state and `rg.at(t)` the dataset "as of `t`".
/// - The only named graphs are the virtual, read-only time graphs `<raphtory:asof:T>`
///   ([`TimeGraph`]): `GRAPH <raphtory:asof:T> { .. }` (or `FROM <raphtory:asof:T>`) matches
///   against `view.snapshot_at(T)` (the view's window and filters apply, `valid()` included,
///   see [`TimeGraph`]). Time
///   graphs cannot be enumerated, so `GRAPH ?g` with `?g` unbound visits the `FROM NAMED`
///   graphs or else those given to [`with_time_graphs`](Self::with_time_graphs). A `?g`
///   bound elsewhere is only reliably matched if listed, named with `FROM NAMED`, or
///   inside `LATERAL { .. }` after the binding. Several `FROM` time graphs are concatenated, so
///   a triple visible in several is matched once per graph (use `DISTINCT`).
/// - Generalized triples (literal subject, or non-IRI layer term) are matched like any other.
///   `CONSTRUCT` drops them, and `DESCRIBE` can be incomplete on such views.
/// - No storage lock is held between results, so the graph can be written to during a query,
///   with no snapshot isolation: writes may or may not be seen, and triples in layers created
///   after the dataset was built are skipped. For a consistent result query a view of
///   `graph.read_only()` (writers wait until it is dropped; writing on the same thread
///   deadlocks).
///
/// A dataset is meant for one query. It caches internal terms and views, so a term keeps the
/// same internal value for the whole query even while the graph is written to, and keeps the
/// triples of a full scan (no subject or object) the query repeats (up to a cap), so repeats
/// replay the same triples.
pub(crate) struct RaphtoryDataset {
    /// The caller's view.
    base: DynamicGraph,
    preds: Arc<PredTable>,
    /// The time graphs `GRAPH ?g` visits when `?g` is unbound, without duplicate IRIs.
    named: Vec<Arc<TimeGraph>>,
    /// `view(at, layer)`, memoised (cleared when it reaches [`VIEW_CACHE_CAP`] entries).
    views: RefCell<FxHashMap<ViewKey, DynamicGraph>>,
    /// `internalize_term`, memoised (up to [`CACHE_CAP`] entries).
    memo: RefCell<FxHashMap<Term, RdfTerm>>,
    /// External terms of nodes and layers (cleared when it reaches [`CACHE_CAP`] entries).
    ext: RefCell<FxHashMap<RdfTerm, Term>>,
    /// The full scans (no subject or object) read so far, by time and layer.
    scans: RefCell<FxHashMap<ViewKey, ScanMemo>>,
    /// The number of triples in `scans`.
    scanned: Cell<usize>,
    /// The maximum number of triples in `scans`.
    scan_memo_cap: usize,
    /// Stops the query between triples and term lookups.
    interrupt: Option<Arc<Interrupt>>,
}

/// What a [`RaphtoryDataset`] remembers of a full scan.
#[derive(Clone)]
enum ScanMemo {
    /// The scan was read once.
    Once,
    /// The triples of the scan, read when it was asked for again.
    Triples(Arc<[(VID, LayerId, VID)]>),
    /// The scan was asked for again, but has too many triples to keep.
    TooLarge,
}

/// The time (`None`: the base view itself) and the layer (`None`: all) of a view.
type ViewKey = (Option<i64>, Option<LayerId>);

/// The predicate term of every layer of the view, fixed when the dataset is built.
#[derive(Default)]
struct PredTable {
    /// `Node(v)` if a node has the layer's term, else `Layer(l)` (the same precedence as
    /// `internalize_term`).
    term: FxHashMap<LayerId, RdfTerm>,
    /// Layers whose term is a node's term, by that node.
    by_node: FxHashMap<VID, LayerId>,
}

impl PredTable {
    fn new(g: &DynamicGraph) -> Self {
        let mut table = Self::default();
        for (l, name) in view_layers(g) {
            let node = name_of(layer_term(&name).as_ref()).and_then(|node| lookup_node(g, &node));
            let term = match node {
                Some(v) => {
                    table.by_node.insert(v, l);
                    RdfTerm::Node(v)
                }
                None => RdfTerm::Layer(l),
            };
            table.term.insert(l, term);
        }
        table
    }
}

/// The node named `name`, ignoring the filters of the view.
pub(crate) fn lookup_node(g: &DynamicGraph, name: &str) -> Option<VID> {
    let v = g.internalise_node(NodeRef::External(GidRef::Str(name)))?;
    // A locked view may not hold a node whose name was resolved after it was locked (reading
    // its name would panic).
    let node = g.core_graph().try_core_node(v)?;
    // On a u64-id graph a string that is not a number is looked up by its hash, so check the
    // name of what was found.
    (node.as_ref().name() == name).then_some(v)
}

/// The bound terms of a quad pattern, resolved: the subject and object nodes and the layer of
/// the predicate (`None`: unbound).
#[derive(Clone, Copy)]
struct Bound {
    subject: Option<VID>,
    layer: Option<LayerId>,
    object: Option<VID>,
}

impl RaphtoryDataset {
    /// Builds the dataset of `view`, listing no time graph.
    pub(crate) fn new<G: StaticGraphViewOps + IntoDynamic>(view: G) -> Self {
        let base = view.into_dynamic();
        let preds = Arc::new(PredTable::new(&base));
        Self {
            base,
            preds,
            named: Vec::new(),
            views: RefCell::default(),
            memo: RefCell::default(),
            ext: RefCell::default(),
            scans: RefCell::default(),
            scanned: Cell::new(0),
            scan_memo_cap: SCAN_MEMO_CAP,
            interrupt: None,
        }
    }

    /// The dataset of a query that stops once `interrupt` has fired.
    pub(crate) fn with_interrupt(mut self, interrupt: Option<Arc<Interrupt>>) -> Self {
        self.interrupt = interrupt;
        self
    }

    /// Unwinds if the query must stop; it never returns an error, because spareval turns term
    /// lookup errors into unbound values.
    #[inline]
    fn unwind_if_stopped(&self) {
        if let Some(interrupt) = &self.interrupt {
            interrupt.unwind_if_fired();
        }
    }

    /// The dataset, keeping at most `cap` triples of the full scans it repeats.
    #[cfg(test)]
    pub(super) fn with_scan_memo_cap(mut self, cap: usize) -> Self {
        self.scan_memo_cap = cap;
        self
    }

    /// Lists `graphs` (skipping duplicate IRIs) as the named graphs `GRAPH ?g { .. }` visits
    /// with `?g` unbound when the query has no `FROM NAMED`. Unlisted time graphs still match
    /// when named explicitly.
    pub(crate) fn with_time_graphs(mut self, graphs: impl IntoIterator<Item = TimeGraph>) -> Self {
        let mut listed: FxHashSet<_> = self.named.iter().map(|graph| graph.iri.clone()).collect();
        for graph in graphs {
            if listed.insert(graph.iri.clone()) {
                self.named.push(Arc::new(graph));
            }
        }
        self
    }

    /// The base view (as of `at`), optionally restricted to one layer, memoised.
    fn view(&self, at: Option<i64>, layer: Option<LayerId>) -> DynamicGraph {
        if let Some(view) = self.views.borrow().get(&(at, layer)) {
            return view.clone();
        }
        let g = match at {
            None => self.base.clone(),
            Some(t) => self.base.snapshot_at(t).into_dynamic(),
        };
        let g = match layer {
            None => g,
            Some(l) => in_layer(&g, l),
        };
        let mut views = self.views.borrow_mut();
        if views.len() >= VIEW_CACHE_CAP {
            views.clear();
        }
        views.insert((at, layer), g.clone());
        g
    }

    /// The layer a bound predicate stands for, if any.
    fn layer_of(&self, predicate: &RdfTerm) -> Option<LayerId> {
        match predicate {
            RdfTerm::Layer(l) => Some(*l),
            RdfTerm::Node(v) => self.preds.by_node.get(v).copied(),
            RdfTerm::TimeGraph(_) | RdfTerm::Other(_) => None,
        }
    }

    /// The bound terms of a pattern, or `None` if it can match nothing: a bound subject or
    /// object must be a node, and a bound predicate a layer of the view.
    fn bind(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
    ) -> Option<Bound> {
        Some(Bound {
            subject: match subject {
                Some(t) => Some(node_of(t)?),
                None => None,
            },
            layer: match predicate {
                Some(t) => Some(self.layer_of(t)?),
                None => None,
            },
            object: match object {
                Some(t) => Some(node_of(t)?),
                None => None,
            },
        })
    }

    /// The quads matching a pattern in the default graph (`graph = None`) or in a time graph.
    fn scan(&self, bound: Bound, graph: Option<&Arc<TimeGraph>>) -> QuadIter {
        let Bound {
            subject,
            layer,
            object,
        } = bound;
        // `GRAPH <raphtory:asof:T>` is the view as of `T`.
        let at = graph.map(|graph| graph.at);
        let view = self.view(at, layer);
        let interruptible = |scan: EdgeScan| scan.interruptible(self.interrupt.clone());
        let triples = match (subject, object) {
            (Some(s), Some(o)) => {
                Triples::Scan(interruptible(EdgeScan::between(view, s, o, layer)))
            }
            (Some(s), None) => {
                Triples::Scan(interruptible(EdgeScan::around(view, s, Dir::Out, layer)))
            }
            (None, Some(o)) => {
                Triples::Scan(interruptible(EdgeScan::around(view, o, Dir::In, layer)))
            }
            (None, None) => self.full_scan((at, layer), view, layer),
        };
        QuadIter {
            triples,
            preds: self.preds.clone(),
            graph_name: graph.map(|graph| RdfTerm::TimeGraph(graph.clone())),
            interrupt: self.interrupt.clone(),
            stopped: false,
        }
    }

    /// A full scan of `view`. On its second request the triples are kept (up to
    /// `scan_memo_cap` per dataset) and later requests replay them.
    fn full_scan(&self, key: ViewKey, view: DynamicGraph, layer: Option<LayerId>) -> Triples {
        let scan = || EdgeScan::all(view, layer).interruptible(self.interrupt.clone());
        // `scans` is not borrowed while the graph is read
        let memo = self.scans.borrow().get(&key).cloned();
        match memo {
            None => {
                self.scans.borrow_mut().insert(key, ScanMemo::Once);
                Triples::Scan(scan())
            }
            Some(ScanMemo::Triples(triples)) => Triples::Replay(triples, 0),
            Some(ScanMemo::TooLarge) => Triples::Scan(scan()),
            Some(ScanMemo::Once) => {
                // one triple more than there is room for tells whether the scan fits
                let room = self.scan_memo_cap.saturating_sub(self.scanned.get());
                let mut scan = scan();
                let mut read: Vec<_> = scan.by_ref().take(room.saturating_add(1)).collect();
                // a scan that was stopped is not complete (the query fails at the next triple)
                let stopped = self.interrupt.as_deref().is_some_and(Interrupt::fired);
                if read.len() <= room && !stopped {
                    self.scanned.set(self.scanned.get() + read.len());
                    // node-scan order, so early-stopping plans read as far as a real scan would
                    read.sort_unstable_by_key(|&(s, l, o)| (s, o, l));
                    let triples: Arc<[_]> = read.into();
                    self.scans
                        .borrow_mut()
                        .insert(key, ScanMemo::Triples(triples.clone()));
                    Triples::Replay(triples, 0)
                } else {
                    // too large: the triples read, then the rest of the scan
                    self.scans.borrow_mut().insert(key, ScanMemo::TooLarge);
                    Triples::Prefix(read.into_iter(), scan)
                }
            }
        }
    }

    /// The number of full scans kept in memory.
    #[cfg(test)]
    pub(super) fn num_kept_scans(&self) -> usize {
        self.scans
            .borrow()
            .values()
            .filter(|memo| matches!(memo, ScanMemo::Triples(_)))
            .count()
    }

    /// The quads matching a pattern: one scan per graph it reads.
    fn quads(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
        graph_name: Option<Option<&RdfTerm>>,
    ) -> Vec<QuadIter> {
        let Some(bound) = self.bind(subject, predicate, object) else {
            return Vec::new();
        };
        match graph_name {
            Some(None) => vec![self.scan(bound, None)],
            Some(Some(RdfTerm::TimeGraph(graph))) => vec![self.scan(bound, Some(graph))],
            // Other named graphs are never stored.
            Some(Some(_)) => Vec::new(),
            // Every named graph: time graphs cannot be enumerated, so the listed ones.
            None => self
                .named
                .iter()
                .map(|graph| self.scan(bound, Some(graph)))
                .collect(),
        }
    }

    /// The external term of a node or layer, cached.
    fn cached_external(&self, term: RdfTerm, external: impl FnOnce() -> Term) -> Term {
        if let Some(external) = self.ext.borrow().get(&term) {
            return external.clone();
        }
        let external = external();
        let mut ext = self.ext.borrow_mut();
        if ext.len() >= CACHE_CAP {
            ext.clear();
        }
        ext.insert(term, external.clone());
        external
    }

    fn internalize(&self, term: &Term) -> Result<RdfTerm, RdfError> {
        let layer = layer_name_of(term.as_ref())
            .and_then(|name| self.base.get_layer_id(&name))
            .filter(|l| *l != STATIC_GRAPH_LAYER_ID);
        // A layer's predicate term is fixed at build time and wins over a newer node.
        if let Some(predicate) = layer.and_then(|l| self.preds.term.get(&l)) {
            return Ok(predicate.clone());
        }
        if let Some(v) = name_of(term.as_ref()).and_then(|name| lookup_node(&self.base, &name)) {
            return Ok(RdfTerm::Node(v));
        }
        if let Some(l) = layer {
            return Ok(RdfTerm::Layer(l));
        }
        // `name_of` and `layer_name_of` are `None` for every IRI under `ASOF_NS`, so a time graph
        // is never a node or a layer.
        if let Term::NamedNode(iri) = term {
            if let Some(graph) = TimeGraph::parse(iri)? {
                return Ok(RdfTerm::TimeGraph(Arc::new(graph)));
            }
        }
        // Unknown terms are accepted: they are constants or computed values of the query.
        Ok(RdfTerm::Other(term.clone()))
    }
}

fn node_of(term: &RdfTerm) -> Option<VID> {
    match term {
        RdfTerm::Node(v) => Some(*v),
        _ => None,
    }
}

/// Turns scanned `(src, layer, dst)` items into quads. Owns everything it uses.
struct QuadIter {
    triples: Triples,
    preds: Arc<PredTable>,
    graph_name: Option<RdfTerm>,
    /// Once it has fired, the query stops: the iterator unwinds (see [`Interrupt`]), or gives
    /// [`RdfError::Cancelled`] and ends.
    interrupt: Option<Arc<Interrupt>>,
    /// Whether it gave [`RdfError::Cancelled`].
    stopped: bool,
}

/// Where the `(src, layer, dst)` items of a [`QuadIter`] come from.
enum Triples {
    /// A scan of the graph.
    Scan(EdgeScan),
    /// The kept triples of a full scan, from a position.
    Replay(Arc<[(VID, LayerId, VID)]>, usize),
    /// The first triples of a scan, read already, then the rest of it.
    Prefix(std::vec::IntoIter<(VID, LayerId, VID)>, EdgeScan),
}

impl Iterator for Triples {
    type Item = (VID, LayerId, VID);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Scan(scan) => scan.next(),
            Self::Replay(triples, pos) => {
                let item = triples.get(*pos).copied();
                *pos += 1;
                item
            }
            Self::Prefix(read, scan) => read.next().or_else(|| scan.next()),
        }
    }
}

impl Iterator for QuadIter {
    type Item = Result<InternalQuad<RdfTerm>, RdfError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.stopped {
                return None;
            }
            let next = self.triples.next();
            // checked after the scan, which also ends early when the query must stop
            if self.interrupt.as_deref().is_some_and(Interrupt::must_stop) {
                self.stopped = true;
                return Some(Err(RdfError::Cancelled));
            }
            let (s, l, o) = next?;
            // A layer created after the dataset was built has no predicate term: skip it.
            if let Some(predicate) = self.preds.term.get(&l) {
                return Some(Ok(InternalQuad {
                    subject: RdfTerm::Node(s),
                    predicate: predicate.clone(),
                    object: RdfTerm::Node(o),
                    graph_name: self.graph_name.clone(),
                }));
            }
        }
    }
}

impl<'a> QueryableDataset<'a> for RaphtoryDataset {
    type InternalTerm = RdfTerm;
    type Error = RdfError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
        graph_name: Option<Option<&RdfTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<RdfTerm>, RdfError>> + use<'a> {
        self.quads(subject, predicate, object, graph_name)
            .into_iter()
            .flatten()
    }

    /// The listed time graphs: time graphs cannot be enumerated.
    fn internal_named_graphs(&self) -> impl Iterator<Item = Result<RdfTerm, RdfError>> + use<'a> {
        let graphs: Vec<_> = self
            .named
            .iter()
            .map(|graph| Ok(RdfTerm::TimeGraph(graph.clone())))
            .collect();
        graphs.into_iter()
    }

    /// Every time graph exists (it may be empty); no other named graph does.
    fn contains_internal_graph_name(&self, graph_name: &RdfTerm) -> Result<bool, RdfError> {
        Ok(matches!(graph_name, RdfTerm::TimeGraph(_)))
    }

    fn internalize_term(&self, term: Term) -> Result<RdfTerm, RdfError> {
        self.unwind_if_stopped();
        if let Some(internal) = self.memo.borrow().get(&term) {
            return Ok(internal.clone());
        }
        let internal = self.internalize(&term)?;
        let mut memo = self.memo.borrow_mut();
        if memo.len() < CACHE_CAP {
            memo.insert(term, internal.clone());
        }
        Ok(internal)
    }

    fn externalize_term(&self, term: RdfTerm) -> Result<Term, RdfError> {
        self.unwind_if_stopped();
        Ok(match term {
            RdfTerm::Node(v) => {
                self.cached_external(RdfTerm::Node(v), || term_of(&self.base.node_name(v)))
            }
            RdfTerm::Layer(l) => self.cached_external(RdfTerm::Layer(l), || {
                layer_term(&self.base.get_layer_name(l))
            }),
            RdfTerm::TimeGraph(graph) => graph.iri.clone().into(),
            RdfTerm::Other(t) => t,
        })
    }
}
