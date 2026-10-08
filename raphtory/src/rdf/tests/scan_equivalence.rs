//! `EdgeScan::only_valid` on a view yields exactly the valid `(edge, layer)` pairs of
//! `view.valid()`.
//!
//! The reference is Raphtory's own edge iterator over `view.valid()`, exploded into layers.
//! Every full, node and edge scan, with and without a layer, in both directions, is checked
//! against its subset of those pairs on persistent and event graphs and on stacks of views.
//! Full scans that read the edge storage are also compared with `EdgeScan::all_by_nodes`, in
//! chunks of every size and on graphs with tiny segments.
use super::select;
use crate::{
    db::{
        api::view::{
            filter_ops::Filter as _,
            internal::{CoreGraphOps, InternalLayerOps},
            DynamicGraph, IntoDynamic, StaticGraphViewOps,
        },
        graph::views::filter::model::{
            degree_filter::DegreeFilterFactory, exploded_edge_filter::ExplodedEdgeFilter,
            ComposableFilter, EdgeViewFilterOps, NodeViewFilterOps, PropertyFilterFactory,
        },
    },
    errors::GraphError,
    prelude::*,
    rdf::{
        model::NamedNode,
        scan::{Dir, EdgeScan},
        term_of, RaphtoryDataset, RdfFormat, RdfTerm, RdfViewOps, TimeGraph,
    },
};
use either::Either;
use proptest::prelude::*;
use raphtory_api::core::entities::{
    properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds, VID,
};
use raphtory_storage::graph::edges::edge_storage_ops::EdgeStorageOps;
use spareval::QueryableDataset;
use std::collections::BTreeSet;

const LAYERS: [&str; 3] = ["p", "q", "r"];

/// A `(src, layer, dst)` pair by index.
type Pair = (usize, usize, usize);

/// The items of a scan, sorted; fails on duplicates.
fn scan_set(scan: impl Iterator<Item = (VID, LayerId, VID)>) -> Vec<Pair> {
    let mut items: Vec<Pair> = scan.map(|(s, l, o)| (s.0, l.0, o.0)).collect();
    items.sort();
    let len = items.len();
    items.dedup();
    assert_eq!(items.len(), len, "duplicate scan items");
    items
}

/// The valid `(src, layer, dst)` pairs of a view, from Raphtory's own edge iterator over
/// `view.valid()` (without `EdgeScan`), sorted.
fn valid_pairs(view: &DynamicGraph) -> Vec<Pair> {
    let pairs: BTreeSet<Pair> = view
        .valid()
        .edges()
        .iter()
        .flat_map(|e| e.explode_layers())
        .filter_map(|e| Some((e.edge.src().0, e.edge.layer()?.0, e.edge.dst().0)))
        .filter(|&(_, l, _)| l != STATIC_GRAPH_LAYER_ID.0)
        .collect();
    pairs.into_iter().collect()
}

/// The number of edges of the storage of `view` that have `layer` or (for `None`) a layer of
/// the view: those a full scan of the edge storage reads, whatever the window.
fn edges_with_layers(view: &DynamicGraph, layer: Option<LayerId>) -> usize {
    let layers = layer.map_or_else(|| view.layer_ids().clone(), LayerIds::One);
    view.core_graph()
        .edge_segment_counts()
        .into_iter()
        .filter(|&eid| {
            view.core_edge(Either::Left(eid))
                .as_ref()
                .has_layer(&layers)
        })
        .count()
}

/// The pairs of `pairs` that `keep` accepts, in layer `layer` (all layers for `None`).
fn subset(pairs: &[Pair], layer: Option<LayerId>, keep: impl Fn(&Pair) -> bool) -> Vec<Pair> {
    pairs
        .iter()
        .filter(|&&(_, l, _)| layer.is_none_or(|layer| layer.0 == l))
        .filter(|pair| keep(pair))
        .copied()
        .collect()
}

/// The sorted N-Triples lines `to_rdf` writes for a view.
fn exported<G: RdfViewOps>(view: &G) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, RdfFormat::NTriples).unwrap();
    let mut lines: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

/// What [`assert_scans_match`] checked.
struct Checked {
    /// The number of valid pairs.
    pairs: usize,
    /// The number of full scans (of a layer or of all) that read the edge storage.
    by_edge_storage: usize,
    /// The number of full scans of a layer of the view, or of all layers of a view with some.
    with_layers: usize,
}

/// Checks every scan of `view` with `only_valid` against its subset of [`valid_pairs`] and
/// against the same scan of `view.valid()`, and export and SPARQL against [`valid_pairs`]. Full
/// scans are checked when read from the edge storage (in chunks of every size) and from the
/// nodes.
fn assert_scans_match(name: &str, view: &DynamicGraph) -> Checked {
    let reference = valid_pairs(view);
    let valid = view.valid().into_dynamic();
    // every node of the storage, also those the view hides
    let vids: Vec<VID> = view
        .core_graph()
        .node_segment_counts()
        .into_iter()
        .collect();
    let layer_ids: Vec<Option<LayerId>> = std::iter::once(None)
        .chain(LAYERS.iter().map(|l| view.get_layer_id(l)))
        .chain([Some(STATIC_GRAPH_LAYER_ID)])
        .collect();
    let check = |what: &dyn Fn() -> String,
                 fast: EdgeScan,
                 expected: Vec<Pair>,
                 scan_of_valid: EdgeScan| {
        let fast = scan_set(fast);
        if fast != expected {
            panic!(
                "{name}: {}: only_valid {fast:?} != valid().edges() {expected:?}",
                what()
            );
        }
        let scan_of_valid = scan_set(scan_of_valid);
        if fast != scan_of_valid {
            panic!(
                "{name}: {}: only_valid {fast:?} != scan of valid() {scan_of_valid:?}",
                what()
            );
        }
    };
    let mut by_edge_storage = 0;
    let mut with_layers = 0;
    for &layer in &layer_ids {
        // the private layer has no triples
        let layer_ref = layer.filter(|&l| l != STATIC_GRAPH_LAYER_ID);
        let empty = layer.is_some() && layer_ref.is_none();
        let expect = |keep: &dyn Fn(&Pair) -> bool| {
            if empty {
                Vec::new()
            } else {
                subset(&reference, layer_ref, keep)
            }
        };
        let all = EdgeScan::all(view.clone(), layer);
        by_edge_storage += usize::from(all.reads_edge_storage());
        with_layers += usize::from(match layer {
            None => !matches!(view.layer_ids(), LayerIds::None),
            Some(l) => l != STATIC_GRAPH_LAYER_ID && view.layer_ids().contains(&l),
        });
        check(
            &|| format!("all {layer:?}"),
            all.only_valid(),
            expect(&|_| true),
            EdgeScan::all(valid.clone(), layer),
        );
        // a scan of the edge storage reads only the edges with the layer it scans, or with a
        // layer of the view
        let mut stored = EdgeScan::all(view.clone(), layer).only_valid();
        if stored.reads_edge_storage() {
            stored.by_ref().for_each(drop);
            assert_eq!(
                stored.edges_read(),
                Some(edges_with_layers(view, layer)),
                "{name}: edges read by all {layer:?}"
            );
        }
        check(
            &|| format!("all by nodes {layer:?}"),
            EdgeScan::all_by_nodes(view.clone(), layer).only_valid(),
            expect(&|_| true),
            EdgeScan::all(valid.clone(), layer),
        );
        for first_chunk in [1, 2] {
            check(
                &|| format!("all in chunks from {first_chunk} {layer:?}"),
                EdgeScan::all_in_chunks(view.clone(), layer, first_chunk).only_valid(),
                expect(&|_| true),
                EdgeScan::all_by_nodes(valid.clone(), layer),
            );
        }
        for &v in &vids {
            for dir in [Dir::Out, Dir::In] {
                let around = EdgeScan::around(view.clone(), v, dir, layer).only_valid();
                let expected = expect(&|&(s, _, o)| match dir {
                    Dir::Out => s == v.0,
                    Dir::In => o == v.0,
                });
                // a node that `view.valid()` hides has no valid edges
                if valid.node(v).is_none() {
                    assert!(
                        expected.is_empty(),
                        "{name}: {v:?} hidden with {expected:?}"
                    );
                }
                check(
                    &|| format!("around {v:?} {dir:?} {layer:?}"),
                    around,
                    expected,
                    EdgeScan::around(valid.clone(), v, dir, layer),
                );
            }
            for &o in &vids {
                check(
                    &|| format!("between {v:?} {o:?} {layer:?}"),
                    EdgeScan::between(view.clone(), v, o, layer).only_valid(),
                    expect(&|&(s, _, d)| s == v.0 && d == o.0),
                    EdgeScan::between(valid.clone(), v, o, layer),
                );
            }
        }
    }
    // export and SPARQL see exactly the valid pairs
    let mut lines: Vec<String> = reference
        .iter()
        .map(|&(s, l, o)| {
            format!(
                "{} {} {} .",
                term_of(&view.node_name(VID(s))),
                term_of(&view.get_layer_name(LayerId(l))),
                term_of(&view.node_name(VID(o)))
            )
        })
        .collect();
    lines.sort();
    assert_eq!(exported(view), lines, "{name}: to_rdf");
    let rows = select(view, "SELECT ?s ?p ?o { ?s ?p ?o }");
    assert_eq!(rows.len(), reference.len(), "{name}: SPARQL");
    // and each predicate-only pattern exactly the pairs of its layer
    for (l, layer) in LAYERS.iter().enumerate() {
        let expected = view.get_layer_id(layer).map_or(0, |id| {
            reference
                .iter()
                .filter(|&&(_, pair_layer, _)| pair_layer == id.0)
                .count()
        });
        let rows = select(
            view,
            &format!("SELECT ?s ?o {{ ?s {} ?o }}", term_of(layer)),
        );
        assert_eq!(rows.len(), expected, "{name}: SPARQL on layer {l}");
    }
    Checked {
        pairs: reference.len(),
        by_edge_storage,
        with_layers,
    }
}

/// One step of a view stack.
#[derive(Debug, Clone)]
enum ViewOp {
    Window(i64, i64),
    SnapshotAt(i64),
    SnapshotLatest,
    Latest,
    Before(i64),
    After(i64),
    Layers(Vec<usize>),
    ExcludeLayers(Vec<usize>),
    Subgraph(Vec<usize>),
    ExcludeNodes(Vec<usize>),
    NodeTypes,
    NodeNameNe(usize),
    NodePropertyGt(i64),
    EdgePropertyGt(i64),
    EdgeSrcNe(usize),
    /// An exploded-edge filter on the edge property `w` (per update).
    ExplodedWeightGt(i64),
    ExplodedIsValid,
    EdgeIsValid,
    EdgeIsDeleted,
    EdgeIsActive,
    /// A node filter that depends on the edges of the node.
    NodeDegreeGt(u64),
    NodeIsActive,
    /// `is_deleted() | src != n`
    EdgeDeletedOrSrcNe(usize),
    /// `degree > k & name != n`
    NodeDegreeGtAndNameNe(u64, usize),
    /// `degree > k & exploded w > w`
    NodeDegreeGtAndExplodedWeightGt(u64, i64),
    Valid,
}

fn node(i: usize) -> String {
    format!("n{i}")
}

fn layers(ls: &[usize]) -> Vec<&'static str> {
    ls.iter().map(|&l| LAYERS[l % LAYERS.len()]).collect()
}

impl ViewOp {
    fn apply(&self, view: DynamicGraph) -> DynamicGraph {
        match self {
            Self::Window(a, b) => view.window(*a, *b).into_dynamic(),
            Self::SnapshotAt(t) => view.snapshot_at(*t).into_dynamic(),
            Self::SnapshotLatest => view.snapshot_latest().into_dynamic(),
            Self::Latest => view.latest().into_dynamic(),
            Self::Before(t) => view.before(*t).into_dynamic(),
            Self::After(t) => view.after(*t).into_dynamic(),
            Self::Layers(ls) => view.valid_layers(layers(ls)).into_dynamic(),
            Self::ExcludeLayers(ls) => view.exclude_valid_layers(layers(ls)).into_dynamic(),
            Self::Subgraph(ns) => view.subgraph(ns.iter().map(|&n| node(n))).into_dynamic(),
            Self::ExcludeNodes(ns) => view
                .exclude_nodes(ns.iter().map(|&n| node(n)))
                .into_dynamic(),
            Self::NodeTypes => view.subgraph_node_types(["typed"]).into_dynamic(),
            Self::NodeNameNe(n) => view
                .filter(NodeFilter::name().ne(node(*n)))
                .unwrap()
                .into_dynamic(),
            Self::NodePropertyGt(k) => {
                let filtered = view.filter(NodeFilter.property("k").gt(*k));
                or_unfiltered(&view, filtered.map(IntoDynamic::into_dynamic))
            }
            Self::EdgePropertyGt(w) => {
                let filtered = view.filter(EdgeFilter.property("w").gt(*w));
                or_unfiltered(&view, filtered.map(IntoDynamic::into_dynamic))
            }
            Self::EdgeSrcNe(n) => view
                .filter(EdgeFilter::src().name().ne(node(*n)))
                .unwrap()
                .into_dynamic(),
            Self::ExplodedWeightGt(w) => {
                let filtered = view.filter(ExplodedEdgeFilter.property("w").gt(*w));
                or_unfiltered(&view, filtered.map(IntoDynamic::into_dynamic))
            }
            Self::ExplodedIsValid => view
                .filter(ExplodedEdgeFilter.is_valid())
                .unwrap()
                .into_dynamic(),
            Self::EdgeIsValid => view.filter(EdgeFilter.is_valid()).unwrap().into_dynamic(),
            Self::EdgeIsDeleted => view.filter(EdgeFilter.is_deleted()).unwrap().into_dynamic(),
            Self::EdgeIsActive => view.filter(EdgeFilter.is_active()).unwrap().into_dynamic(),
            Self::NodeDegreeGt(k) => view
                .filter(NodeFilter.degree().gt(*k))
                .unwrap()
                .into_dynamic(),
            Self::NodeIsActive => view.filter(NodeFilter.is_active()).unwrap().into_dynamic(),
            Self::EdgeDeletedOrSrcNe(n) => view
                .filter(
                    EdgeFilter
                        .is_deleted()
                        .or(EdgeFilter::src().name().ne(node(*n))),
                )
                .unwrap()
                .into_dynamic(),
            Self::NodeDegreeGtAndNameNe(k, n) => view
                .filter(
                    NodeFilter
                        .degree()
                        .gt(*k)
                        .and(NodeFilter::name().ne(node(*n))),
                )
                .unwrap()
                .into_dynamic(),
            Self::NodeDegreeGtAndExplodedWeightGt(k, w) => {
                let filtered = view.filter(
                    NodeFilter
                        .degree()
                        .gt(*k)
                        .and(ExplodedEdgeFilter.property("w").gt(*w)),
                );
                or_unfiltered(&view, filtered.map(IntoDynamic::into_dynamic))
            }
            Self::Valid => view.valid().into_dynamic(),
        }
    }
}

/// The result of a property filter, or `view` itself if the property does not exist (a random
/// graph without it).
fn or_unfiltered(view: &DynamicGraph, filtered: Result<DynamicGraph, GraphError>) -> DynamicGraph {
    match filtered {
        Ok(filtered) => filtered,
        Err(GraphError::PropertyMissingError(_)) => view.clone(),
        Err(err) => panic!("{err}"),
    }
}

/// One write of a random history.
#[derive(Debug, Clone)]
enum Write {
    Assert(i64, usize, usize, usize),
    /// An assertion with the edge property `w`.
    AssertWithWeight(i64, usize, usize, usize, i64),
    Retract(i64, usize, usize, usize),
    /// A retraction and an assertion at the same time (the assertion is written later, so it
    /// wins).
    RetractThenAssert(i64, usize, usize, usize),
    /// A node update with the property `k` (even nodes have the type `typed`).
    Node(i64, usize, i64),
}

impl Write {
    fn apply(&self, pg: &PersistentGraph) {
        let layer = |l: &usize| Some(LAYERS[*l]);
        match self {
            Self::Assert(t, s, o, l) => {
                pg.add_edge(*t, node(*s), node(*o), NO_PROPS, layer(l))
                    .unwrap();
            }
            Self::AssertWithWeight(t, s, o, l, w) => {
                pg.add_edge(*t, node(*s), node(*o), [("w", Prop::I64(*w))], layer(l))
                    .unwrap();
            }
            Self::Retract(t, s, o, l) => {
                pg.delete_edge(*t, node(*s), node(*o), layer(l)).unwrap();
            }
            Self::RetractThenAssert(t, s, o, l) => {
                pg.delete_edge(*t, node(*s), node(*o), layer(l)).unwrap();
                pg.add_edge(*t, node(*s), node(*o), NO_PROPS, layer(l))
                    .unwrap();
            }
            Self::Node(t, n, k) => {
                let node_type = (n % 2 == 0).then_some("typed");
                pg.add_node(*t, node(*n), [("k", Prop::I64(*k))], node_type, None)
                    .unwrap();
            }
        }
    }
}

/// A persistent graph whose node and edge segments hold 3 and 4 entries, so ids are spread over
/// many segments, with gaps.
fn segmented() -> PersistentGraph {
    PersistentGraph::new_with_config(
        Args::default()
            .with_max_node_page_len(3)
            .with_max_edge_page_len(4),
    )
    .unwrap()
}

/// A persistent graph with deletions, re-assertions at the same time, orphan retractions,
/// self-loops, multi-layer edges, edge weights, node properties and node types, written to `pg`.
fn fixture(pg: PersistentGraph) -> PersistentGraph {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    for i in 0..300 {
        let t = 1 + next(8) as i64;
        let (s, o, l) = (next(5) as usize, next(5) as usize, next(3) as usize);
        let write = match next(6) {
            0 | 1 => Write::Assert(t, s, o, l),
            2 => Write::AssertWithWeight(t, s, o, l, next(4) as i64),
            3 => Write::Retract(t, s, o, l),
            4 => Write::RetractThenAssert(t, s, o, l),
            _ => Write::Node(t, s, (i % 5) as i64),
        };
        write.apply(&pg);
    }
    // an edge that is only ever retracted, between nodes nothing else uses
    pg.delete_edge(3, "n7", "n8", Some("p")).unwrap();
    // a node with an update but no edge, and a typed node whose only edge is in one layer
    pg.add_node(2, "n9", [("k", Prop::I64(9))], Some("typed"), None)
        .unwrap();
    pg.add_node(2, "n6", [("k", Prop::I64(3))], Some("typed"), None)
        .unwrap();
    pg.add_edge(2, "n6", "n0", NO_PROPS, Some("q")).unwrap();
    pg
}

/// The views of the fixture: every kind of view, alone and stacked, on the persistent graph and
/// on its event graph.
#[test]
fn only_valid_scans_match_scans_of_the_valid_view() {
    let pg = fixture(PersistentGraph::new());
    let tiny = fixture(segmented());
    assert!(
        tiny.core_graph().edge_segment_counts().counts().len() > 4,
        "{:?}",
        tiny.core_graph().edge_segment_counts().counts()
    );
    let stacks: Vec<Vec<ViewOp>> = {
        use ViewOp::*;
        let mut stacks = vec![
            vec![],
            vec![Window(2, 6)],
            vec![Before(4)],
            vec![After(5)],
            vec![Latest],
            vec![SnapshotLatest],
            vec![Layers(vec![0])],
            vec![Layers(vec![0, 1])],
            vec![ExcludeLayers(vec![1])],
            vec![Subgraph(vec![0, 1, 2])],
            vec![SnapshotAt(3), Subgraph(vec![0, 1, 6])],
            vec![ExcludeNodes(vec![1])],
            vec![NodeTypes],
            vec![NodeNameNe(2)],
            vec![NodePropertyGt(1)],
            vec![EdgePropertyGt(1)],
            vec![EdgeSrcNe(0)],
            vec![Valid],
            vec![Window(2, 7), Layers(vec![2]), ExcludeNodes(vec![3])],
            vec![NodeTypes, SnapshotAt(5)],
            vec![SnapshotAt(6), NodePropertyGt(2), Layers(vec![0, 2])],
            vec![EdgePropertyGt(0), Window(1, 5)],
            vec![Valid, Window(3, 8)],
            vec![Subgraph(vec![0, 2, 4]), Valid, Layers(vec![1])],
            vec![ExplodedWeightGt(0)],
            vec![ExplodedIsValid],
            vec![EdgeIsValid],
            vec![EdgeIsDeleted],
            vec![EdgeIsActive],
            vec![NodeDegreeGt(2)],
            vec![NodeIsActive],
            vec![EdgeDeletedOrSrcNe(1)],
            vec![NodeDegreeGtAndNameNe(1, 0)],
            vec![NodeDegreeGtAndExplodedWeightGt(1, 0)],
            vec![Window(2, 6), NodeDegreeGt(1), ExplodedWeightGt(0)],
            vec![SnapshotAt(5), ExplodedWeightGt(1)],
            vec![EdgeIsDeleted, Window(3, 7)],
            vec![Window(2, 6), EdgeIsActive, Layers(vec![0, 1])],
            vec![NodeIsActive, Window(3, 7)],
            vec![Window(1, 5), NodeIsActive],
            vec![EdgeIsValid, SnapshotAt(4), NodeDegreeGt(0)],
            vec![Before(6), EdgeDeletedOrSrcNe(2), Valid],
            vec![
                After(3),
                NodeDegreeGtAndExplodedWeightGt(0, 1),
                ExcludeNodes(vec![4]),
            ],
        ];
        for t in 0..=9 {
            stacks.push(vec![SnapshotAt(t)]);
        }
        stacks
    };
    let mut nonempty = 0;
    let mut persistent_pairs = Vec::new();
    for (graph, base) in [
        ("persistent", pg.clone().into_dynamic()),
        ("event", pg.event_graph().into_dynamic()),
        ("read only", pg.read_only().into_dynamic()),
        ("segmented", tiny.clone().into_dynamic()),
        ("segmented event", tiny.event_graph().into_dynamic()),
        ("segmented read only", tiny.read_only().into_dynamic()),
    ] {
        for stack in &stacks {
            let view = stack.iter().fold(base.clone(), |view, op| op.apply(view));
            let checked = assert_scans_match(&format!("{graph} {stack:?}"), &view);
            nonempty += usize::from(checked.pairs > 0);
            if graph == "persistent" {
                persistent_pairs.push(checked.pairs);
            }
            // views with only windows and layers read full scans from the edge
            // storage, and filtered views read every node
            if unfiltered(stack) {
                assert!(checked.with_layers > 0, "{graph} {stack:?}");
                assert_eq!(
                    checked.by_edge_storage, checked.with_layers,
                    "{graph} {stack:?}"
                );
            } else {
                assert_eq!(checked.by_edge_storage, 0, "{graph} {stack:?}");
            }
        }
    }
    assert!(nonempty > stacks.len(), "{nonempty}");
    // the filters on weights, degrees and activity keep some valid pairs and drop others
    let all = persistent_pairs[0];
    for (stack, pairs) in stacks.iter().zip(&persistent_pairs) {
        if let [ViewOp::ExplodedWeightGt(_)
        | ViewOp::NodeDegreeGt(_)
        | ViewOp::EdgeDeletedOrSrcNe(_)
        | ViewOp::NodeDegreeGtAndExplodedWeightGt(..)] = stack.as_slice()
        {
            assert!(0 < *pairs && *pairs < all, "{stack:?}: {pairs} of {all}");
        }
    }

    // the history is not trivial: some pairs are retracted, and differ between versions
    let view = pg.clone().into_dynamic();
    let now = scan_set(EdgeScan::all(view.clone(), None).only_valid()).len();
    let stored = scan_set(EdgeScan::all(view, None)).len();
    let at_3 = scan_set(EdgeScan::all(pg.snapshot_at(3).into_dynamic(), None).only_valid()).len();
    assert!(
        0 < now && now < stored && at_3 != now,
        "{now} {stored} {at_3}"
    );

    // `GRAPH <raphtory:asof:T>` scans `snapshot_at(T)` for its valid pairs, also one layer at
    // a time (from the edge storage)
    for g in [&pg, &tiny] {
        for t in 0..=9 {
            let reference = valid_pairs(&g.snapshot_at(t).into_dynamic());
            let rows = select(
                g,
                &format!("SELECT ?s ?p ?o {{ GRAPH <raphtory:asof:{t}> {{ ?s ?p ?o }} }}"),
            );
            assert_eq!(rows.len(), reference.len(), "as of {t}");
            for layer in LAYERS {
                let id = g.get_layer_id(layer).unwrap();
                let mut expected: Vec<Vec<String>> = subset(&reference, Some(id), |_| true)
                    .into_iter()
                    .map(|(s, _, o)| {
                        [s, o]
                            .map(|v| term_of(&g.node_name(VID(v))).to_string())
                            .to_vec()
                    })
                    .collect();
                expected.sort();
                let rows = select(
                    g,
                    &format!(
                        "SELECT ?s ?o {{ GRAPH <raphtory:asof:{t}> {{ ?s {} ?o }} }}",
                        term_of(layer)
                    ),
                );
                assert_eq!(rows, expected, "as of {t} in {layer}");
            }
        }
    }
}

/// The triples of the pattern `?s ?p ?o` (with the predicate of `layer`, if any) in `graph`
/// (as `internal_quads_for_pattern` takes it) of a dataset, in the order the dataset gives.
fn full_scan_of(
    dataset: &RaphtoryDataset,
    layer: Option<LayerId>,
    graph: Option<Option<&RdfTerm>>,
) -> Vec<Pair> {
    let predicate = layer.map(RdfTerm::Layer);
    dataset
        .internal_quads_for_pattern(None, predicate.as_ref(), None, graph)
        .map(|quad| {
            let quad = quad.unwrap();
            match (quad.subject, quad.predicate, quad.object) {
                (RdfTerm::Node(s), RdfTerm::Layer(l), RdfTerm::Node(o)) => (s.0, l.0, o.0),
                other => panic!("{other:?}"),
            }
        })
        .collect()
}

/// A repeated full scan is kept and replayed, and every request gives the reference triples.
#[test]
fn repeated_full_scans_are_replayed() {
    for pg in [fixture(PersistentGraph::new()), fixture(segmented())] {
        for (at, view) in [
            (None, pg.clone().into_dynamic()),
            (Some(5), pg.snapshot_at(5).into_dynamic()),
        ] {
            let reference = valid_pairs(&view);
            let dataset = RaphtoryDataset::new(pg.clone());
            let time_graph = at.map(|t| {
                let iri = NamedNode::new(format!("raphtory:asof:{t}")).unwrap();
                RdfTerm::TimeGraph(TimeGraph::parse(&iri).unwrap().unwrap().into())
            });
            let graph = Some(time_graph.as_ref());
            let mut kept = 0;
            for layer in std::iter::once(None).chain(LAYERS.map(Some)) {
                let layer = layer.map(|l| pg.get_layer_id(l).unwrap());
                let expected = subset(&reference, layer, |_| true);
                for read in 0..3 {
                    let mut triples = full_scan_of(&dataset, layer, graph);
                    if read > 0 {
                        // kept in the order of a scan of the nodes
                        let mut by_nodes = triples.clone();
                        by_nodes.sort_by_key(|&(s, l, o)| (s, o, l));
                        assert_eq!(triples, by_nodes, "{at:?} {layer:?}");
                    }
                    triples.sort();
                    assert_eq!(triples, expected, "{at:?} {layer:?} read {read}");
                }
                kept += 1;
                assert_eq!(dataset.num_kept_scans(), kept, "{at:?} {layer:?}");
            }
        }
    }
}

/// A `FILTER EXISTS` that rescans a predicate per solution gives the reference solutions.
#[test]
fn exists_over_repeated_scans() {
    let pg = fixture(PersistentGraph::new());
    for view in [
        pg.clone().into_dynamic(),
        pg.snapshot_at(4).into_dynamic(),
        pg.event_graph().into_dynamic(),
    ] {
        let reference = valid_pairs(&view);
        let id = |l: &str| view.get_layer_id(l).unwrap().0;
        let (p, q, r) = (id("p"), id("q"), id("r"));
        let expected = reference
            .iter()
            .filter(|&&(_, l, b)| {
                l == p
                    && reference.iter().any(|&(s, l, c)| {
                        s == b && l == q && reference.iter().any(|&(s, l, _)| s == c && l == r)
                    })
            })
            .count();
        let [p, q, r] = LAYERS.map(term_of);
        let rows = select(
            &view,
            &format!("SELECT ?a ?b {{ ?a {p} ?b FILTER EXISTS {{ ?b {q} ?c . ?c {r} ?d }} }}"),
        );
        assert!(expected > 0);
        assert_eq!(rows.len(), expected);
    }
}

/// Repeated scans are kept only while they fit in the cap; every request gives the reference triples.
#[test]
fn repeated_scans_beyond_the_memo_cap() {
    for pg in [fixture(PersistentGraph::new()), fixture(segmented())] {
        let reference = valid_pairs(&pg.clone().into_dynamic());
        let (p, q) = (pg.get_layer_id("p").unwrap(), pg.get_layer_id("q").unwrap());
        let (in_p, in_q) = (
            subset(&reference, Some(p), |_| true),
            subset(&reference, Some(q), |_| true),
        );
        assert!(in_p.len() > 3 && in_q.len() > 3, "{in_p:?} {in_q:?}");
        let read = |dataset: &RaphtoryDataset, layer: Option<LayerId>| {
            let mut triples = full_scan_of(dataset, layer, Some(None));
            triples.sort();
            triples
        };
        // `p` alone is kept if it fits
        for cap in [
            0,
            1,
            in_p.len() / 2,
            in_p.len() - 2,
            in_p.len() - 1,
            in_p.len(),
        ] {
            let dataset = RaphtoryDataset::new(pg.clone()).with_scan_memo_cap(cap);
            for i in 0..3 {
                assert_eq!(read(&dataset, Some(p)), in_p, "cap {cap}, read {i}");
            }
            let kept = usize::from(cap >= in_p.len());
            assert_eq!(dataset.num_kept_scans(), kept, "cap {cap}");
        }
        // `q` is kept after `p` if both fit
        let caps = [0, 1, in_q.len() - 2, in_q.len() - 1, in_q.len()].map(|c| in_p.len() + c);
        for cap in caps {
            let dataset = RaphtoryDataset::new(pg.clone()).with_scan_memo_cap(cap);
            for i in 0..2 {
                assert_eq!(read(&dataset, Some(p)), in_p, "cap {cap}, read {i} of p");
            }
            assert_eq!(dataset.num_kept_scans(), 1, "cap {cap}");
            for i in 0..3 {
                assert_eq!(read(&dataset, Some(q)), in_q, "cap {cap}, read {i} of q");
            }
            let kept = 1 + usize::from(cap >= in_p.len() + in_q.len());
            assert_eq!(dataset.num_kept_scans(), kept, "cap {cap}");
        }
        // the scan of every layer, beyond the cap
        let dataset = RaphtoryDataset::new(pg.clone()).with_scan_memo_cap(reference.len() / 2);
        for i in 0..3 {
            assert_eq!(read(&dataset, None), reference, "read {i} of all");
        }
        assert_eq!(dataset.num_kept_scans(), 0);
    }
}

/// A full scan of a layer-restricted view reads only edges of its layers and gives its valid pairs.
#[test]
fn full_scans_of_layer_views_read_only_their_edges() {
    for g in [PersistentGraph::new(), segmented()] {
        // the edges of each layer, by name
        let mut edges: Vec<(&str, BTreeSet<(usize, usize)>)> =
            ["big", "small", "mid"].map(|l| (l, BTreeSet::new())).into();
        let mut add = |t: i64, s: usize, o: usize, l: usize| {
            g.add_edge(t, node(s), node(o), NO_PROPS, Some(edges[l].0))
                .unwrap();
            edges[l].1.insert((s, o));
        };
        for i in 0..400 {
            add(1 + (i % 7) as i64, i % 23, (i * 7) % 29, 0);
        }
        for i in 0..6 {
            add(2 + i as i64, i, i + 1, 1);
        }
        // an edge also in `big`
        add(3, 0, 0, 1);
        add(4, 3, 9, 2);
        add(5, 9, 3, 2);
        // an edge of `small` that is only ever retracted
        g.delete_edge(4, node(40), node(41), Some("small")).unwrap();
        edges[1].1.insert((40, 41));
        assert!(edges[0].1.contains(&(0, 0)) && edges[0].1.len() > 100);
        let count = |layers: &[usize]| {
            let union: BTreeSet<_> = layers.iter().flat_map(|&l| &edges[l].1).collect();
            union.len()
        };
        let small_and_mid = count(&[1, 2]);
        let base = g.clone().into_dynamic();
        for (name, view, read) in [
            (
                "small",
                base.valid_layers("small").into_dynamic(),
                count(&[1]),
            ),
            (
                "small and mid",
                base.valid_layers(["small", "mid"]).into_dynamic(),
                small_and_mid,
            ),
            (
                "not big",
                base.exclude_valid_layers("big").into_dynamic(),
                small_and_mid,
            ),
            (
                "window of small",
                base.window(3, 6).valid_layers("small").into_dynamic(),
                count(&[1]),
            ),
            (
                "small and mid as of 4",
                base.snapshot_at(4)
                    .valid_layers(["small", "mid"])
                    .into_dynamic(),
                small_and_mid,
            ),
            (
                "event graph of mid",
                g.event_graph().valid_layers("mid").into_dynamic(),
                count(&[2]),
            ),
            (
                "read only small",
                g.read_only()
                    .into_dynamic()
                    .valid_layers("small")
                    .into_dynamic(),
                count(&[1]),
            ),
        ] {
            let reference = valid_pairs(&view);
            assert!(!reference.is_empty(), "{name}");
            let mut scan = EdgeScan::all(view.clone(), None).only_valid();
            assert!(scan.reads_edge_storage(), "{name}");
            let pairs = scan_set(scan.by_ref());
            assert_eq!(pairs, reference, "{name}");
            assert_eq!(scan.edges_read(), Some(read), "{name}");
            let rows = select(&view, "SELECT ?s ?p ?o { ?s ?p ?o }");
            assert_eq!(rows.len(), reference.len(), "{name}: SPARQL");
        }
    }
}

fn write_strategy() -> impl Strategy<Value = Write> {
    let t = 1i64..8;
    let n = 0usize..5;
    let l = 0usize..3;
    prop_oneof![
        3 => (t.clone(), n.clone(), n.clone(), l.clone()).prop_map(|(t, s, o, l)| Write::Assert(t, s, o, l)),
        1 => (t.clone(), n.clone(), n.clone(), l.clone(), 0i64..4)
            .prop_map(|(t, s, o, l, w)| Write::AssertWithWeight(t, s, o, l, w)),
        2 => (t.clone(), 0usize..7, 0usize..7, l.clone()).prop_map(|(t, s, o, l)| Write::Retract(t, s, o, l)),
        1 => (t.clone(), n.clone(), n.clone(), l).prop_map(|(t, s, o, l)| Write::RetractThenAssert(t, s, o, l)),
        1 => (t, 0usize..7, 0i64..4).prop_map(|(t, n, k)| Write::Node(t, n, k)),
    ]
}

fn view_op_strategy() -> impl Strategy<Value = ViewOp> {
    let t = 0i64..9;
    let nodes = proptest::collection::vec(0usize..7, 0..4);
    let layers = proptest::collection::vec(0usize..3, 1..3);
    prop_oneof![
        (t.clone(), 1i64..5).prop_map(|(a, d)| ViewOp::Window(a, a + d)),
        t.clone().prop_map(ViewOp::SnapshotAt),
        Just(ViewOp::SnapshotLatest),
        Just(ViewOp::Latest),
        t.clone().prop_map(ViewOp::Before),
        t.prop_map(ViewOp::After),
        layers.clone().prop_map(ViewOp::Layers),
        layers.prop_map(ViewOp::ExcludeLayers),
        nodes.clone().prop_map(ViewOp::Subgraph),
        nodes.prop_map(ViewOp::ExcludeNodes),
        Just(ViewOp::NodeTypes),
        (0usize..7).prop_map(ViewOp::NodeNameNe),
        (0i64..4).prop_map(ViewOp::NodePropertyGt),
        (0i64..4).prop_map(ViewOp::EdgePropertyGt),
        (0usize..7).prop_map(ViewOp::EdgeSrcNe),
        (0i64..4).prop_map(ViewOp::ExplodedWeightGt),
        Just(ViewOp::ExplodedIsValid),
        Just(ViewOp::EdgeIsValid),
        Just(ViewOp::EdgeIsDeleted),
        Just(ViewOp::EdgeIsActive),
        (0u64..4).prop_map(ViewOp::NodeDegreeGt),
        Just(ViewOp::NodeIsActive),
        (0usize..7).prop_map(ViewOp::EdgeDeletedOrSrcNe),
        (0u64..4, 0usize..7).prop_map(|(k, n)| ViewOp::NodeDegreeGtAndNameNe(k, n)),
        (0u64..4, 0i64..4).prop_map(|(k, w)| ViewOp::NodeDegreeGtAndExplodedWeightGt(k, w)),
        Just(ViewOp::Valid),
    ]
}

/// Whether a view stack has only windows and layer restrictions.
fn unfiltered(stack: &[ViewOp]) -> bool {
    stack.iter().all(|op| {
        matches!(
            op,
            ViewOp::Window(..)
                | ViewOp::SnapshotAt(_)
                | ViewOp::SnapshotLatest
                | ViewOp::Latest
                | ViewOp::Before(_)
                | ViewOp::After(_)
                | ViewOp::Layers(_)
                | ViewOp::ExcludeLayers(_)
        )
    })
}

/// Checks the view stack on the graph and on its event graph.
fn check_random<G: StaticGraphViewOps + IntoDynamic>(name: &str, base: G, stack: &[ViewOp]) {
    let view = stack
        .iter()
        .fold(base.into_dynamic(), |view, op| op.apply(view));
    assert_scans_match(&format!("{name} {stack:?}"), &view);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Random small temporal graphs under random stacks of views.
    #[test]
    fn random_views_scan_the_same_pairs(
        writes in proptest::collection::vec(write_strategy(), 0..40),
        stack in proptest::collection::vec(view_op_strategy(), 0..4),
    ) {
        let pg = PersistentGraph::new();
        let tiny = segmented();
        for write in &writes {
            write.apply(&pg);
            write.apply(&tiny);
        }
        check_random("persistent", pg.clone(), &stack);
        check_random("event", pg.event_graph(), &stack);
        check_random("segmented", tiny.clone(), &stack);
        check_random("segmented event", tiny.event_graph(), &stack);
    }
}
