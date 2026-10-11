//! `EdgeScan` on a view yields exactly the `(edge, layer)` pairs of the view.
//!
//! The reference is Raphtory's own edge iterator over the view, exploded into layers.
//! Every full, node and edge scan, with and without a layer, in both directions, is checked
//! against its subset of those pairs on persistent graphs and on views of them. Full scans
//! are also compared with `EdgeScan::all_by_nodes`, on graphs with tiny segments too.
use super::{exported, select};
use crate::{mapping::layer_term, scan::EdgeScan, term_of, RdfView};
use raphtory::{
    db::{
        api::view::{filter_ops::Filter as _, internal::CoreGraphOps, DynamicGraph, IntoDynamic},
        graph::views::filter::model::{
            degree_filter::DegreeFilterFactory, exploded_edge_filter::ExplodedEdgeFilter,
            ComposableFilter, PropertyFilterFactory,
        },
    },
    prelude::*,
};
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, VID};
use std::{collections::BTreeSet, iter};

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

/// The `(src, layer, dst)` pairs of a view, from Raphtory's own edge iterator (without
/// `EdgeScan`), sorted.
fn view_pairs(view: &DynamicGraph) -> Vec<Pair> {
    let pairs: BTreeSet<Pair> = view
        .edges()
        .iter()
        .flat_map(|e| e.explode_layers())
        .filter_map(|e| Some((e.edge.src().0, e.edge.layer()?.0, e.edge.dst().0)))
        .filter(|&(_, l, _)| l != STATIC_GRAPH_LAYER_ID.0)
        .collect();
    pairs.into_iter().collect()
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

/// Checks every scan of `view` against its subset of [`view_pairs`], and export and SPARQL
/// against [`view_pairs`]. Returns the number of pairs.
fn assert_scans_match(name: &str, view: &DynamicGraph) -> usize {
    let reference = view_pairs(view);
    // every node of the storage, also those the view hides
    let vids: Vec<VID> = view
        .core_graph()
        .node_segment_counts()
        .into_iter()
        .collect();
    for v in &vids {
        // a node that the view hides has no edges
        if view.node(*v).is_none() {
            let pairs = subset(&reference, None, |&(s, _, o)| s == v.0 || o == v.0);
            assert!(pairs.is_empty(), "{name}: {v:?} hidden with {pairs:?}");
        }
    }
    let patterns: Vec<(Option<VID>, Option<VID>)> = vids
        .iter()
        .flat_map(|&v| [(Some(v), None), (None, Some(v))])
        .chain(
            vids.iter()
                .flat_map(|&s| vids.iter().map(move |&o| (Some(s), Some(o)))),
        )
        .chain([(None, None)])
        .collect();
    let bound = |term: Option<VID>, v: usize| term.is_none_or(|term| term.0 == v);
    let check = |what: &dyn Fn() -> String, scan: EdgeScan, expected: Vec<Pair>| {
        let scan = scan_set(scan);
        if scan != expected {
            panic!("{name}: {}: scan {scan:?} != edges() {expected:?}", what());
        }
    };
    let layer_ids = iter::once(None).chain(LAYERS.iter().map(|l| view.get_layer_id(l)));
    for layer in layer_ids {
        for &(subject, object) in &patterns {
            check(
                &|| format!("{subject:?} {layer:?} {object:?}"),
                EdgeScan::matching(view.clone(), subject, layer, object),
                subset(&reference, layer, |&(s, _, o)| {
                    bound(subject, s) && bound(object, o)
                }),
            );
        }
        check(
            &|| format!("all by nodes {layer:?}"),
            EdgeScan::all_by_nodes(view.clone(), layer),
            subset(&reference, layer, |_| true),
        );
    }
    // export and SPARQL see exactly the pairs of the view
    let mut lines: Vec<String> = reference
        .iter()
        .map(|&(s, l, o)| {
            format!(
                "{} {} {} .",
                term_of(&view.node_name(VID(s))),
                layer_term(&view.get_layer_name(LayerId(l))),
                term_of(&view.node_name(VID(o)))
            )
        })
        .collect();
    lines.sort();
    let rdf = RdfView::from_view(view.clone()).unwrap();
    assert_eq!(exported(&rdf), lines, "{name}: to_rdf");
    let rows = select(&rdf, "SELECT ?s ?p ?o { ?s ?p ?o }");
    assert_eq!(rows.len(), reference.len(), "{name}: SPARQL");
    reference.len()
}

/// One step of a view stack.
#[derive(Debug, Clone)]
enum ViewOp {
    Window(i64, i64),
    SnapshotAt(i64),
    Layers(Vec<usize>),
    ExcludeLayers(Vec<usize>),
    Subgraph(Vec<usize>),
    EdgePropertyGt(i64),
    EdgeSrcNe(usize),
    /// An exploded-edge filter on the edge property `w` (per update).
    ExplodedWeightGt(i64),
    /// A node filter that depends on the edges of the node.
    NodeDegreeGt(u64),
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
            Self::Layers(ls) => view.valid_layers(layers(ls)).into_dynamic(),
            Self::ExcludeLayers(ls) => view.exclude_valid_layers(layers(ls)).into_dynamic(),
            Self::Subgraph(ns) => view.subgraph(ns.iter().map(|&n| node(n))).into_dynamic(),
            Self::EdgePropertyGt(w) => view
                .filter(EdgeFilter.property("w").gt(*w))
                .unwrap()
                .into_dynamic(),
            Self::EdgeSrcNe(n) => view
                .filter(EdgeFilter::src().name().ne(node(*n)))
                .unwrap()
                .into_dynamic(),
            Self::ExplodedWeightGt(w) => view
                .filter(ExplodedEdgeFilter.property("w").gt(*w))
                .unwrap()
                .into_dynamic(),
            Self::NodeDegreeGt(k) => view
                .filter(NodeFilter.degree().gt(*k))
                .unwrap()
                .into_dynamic(),
            Self::NodeDegreeGtAndExplodedWeightGt(k, w) => view
                .filter(
                    NodeFilter
                        .degree()
                        .gt(*k)
                        .and(ExplodedEdgeFilter.property("w").gt(*w)),
                )
                .unwrap()
                .into_dynamic(),
            Self::Valid => view.valid().into_dynamic(),
        }
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

/// Scans match the edges of each kind of view, on plain, read-only and tiny-segment graphs.
#[test]
fn scans_match_the_edges_of_the_view() {
    let pg = fixture(PersistentGraph::new());
    let tiny = fixture(segmented());
    assert!(
        tiny.core_graph().edge_segment_counts().counts().len() > 4,
        "{:?}",
        tiny.core_graph().edge_segment_counts().counts()
    );
    let stacks: Vec<Vec<ViewOp>> = {
        use ViewOp::*;
        vec![
            vec![],
            vec![Window(2, 6)],
            vec![SnapshotAt(3)],
            vec![Layers(vec![0, 1])],
            vec![ExcludeLayers(vec![1])],
            vec![Subgraph(vec![0, 1, 2])],
            vec![EdgePropertyGt(1)],
            vec![EdgeSrcNe(0)],
            vec![ExplodedWeightGt(0)],
            vec![ExplodedWeightGt(0), Valid],
            vec![NodeDegreeGt(2)],
            vec![NodeDegreeGtAndExplodedWeightGt(1, 0)],
            vec![Valid],
        ]
    };
    let mut nonempty = 0;
    let mut persistent_pairs = Vec::new();
    for (graph, base) in [
        ("persistent", pg.clone().into_dynamic()),
        ("read only", pg.read_only().into_dynamic()),
        ("segmented", tiny.clone().into_dynamic()),
    ] {
        for stack in &stacks {
            let view = stack.iter().fold(base.clone(), |view, op| op.apply(view));
            let pairs = assert_scans_match(&format!("{graph} {stack:?}"), &view);
            nonempty += usize::from(pairs > 0);
            if graph == "persistent" {
                persistent_pairs.push(pairs);
            }
        }
    }
    assert!(nonempty > stacks.len(), "{nonempty}");
    // the filters on weights and degrees keep some pairs and drop others (an exploded-edge
    // filter alone keeps every pair of the whole graph)
    let all = persistent_pairs[0];
    for (stack, pairs) in stacks.iter().zip(&persistent_pairs) {
        if let [ViewOp::ExplodedWeightGt(_), ViewOp::Valid]
        | [ViewOp::NodeDegreeGt(_)]
        | [ViewOp::NodeDegreeGtAndExplodedWeightGt(..)] = stack.as_slice()
        {
            assert!(0 < *pairs && *pairs < all, "{stack:?}: {pairs} of {all}");
        }
    }

    // the history is not trivial: some pairs are retracted, and differ between versions
    let now = scan_set(EdgeScan::all(pg.valid().into_dynamic(), None)).len();
    let stored = scan_set(EdgeScan::all(pg.clone().into_dynamic(), None)).len();
    let at_3 = scan_set(EdgeScan::all(pg.snapshot_at(3).into_dynamic(), None)).len();
    assert!(
        0 < now && now < stored && at_3 != now,
        "{now} {stored} {at_3}"
    );
}
