//! `EdgeScan` on a view yields exactly the `(edge, layer)` pairs of the view.
//!
//! The reference is Raphtory's own edge iterator over the view, exploded into layers.
//! Every full, node and edge scan, with and without a layer, in both directions, is checked
//! against its subset of those pairs on persistent graphs and on views of them. Full scans
//! that read the edges by id are also compared with `EdgeScan::all_by_nodes`, in chunks of
//! every size and on graphs with tiny segments.
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
            ComposableFilter, PropertyFilterFactory,
        },
    },
    prelude::*,
    rdf::{
        dataset::{RaphtoryDataset, RdfTerm},
        layer_term,
        model::NamedNode,
        scan::{Dir, EdgeScan},
        term_of, RdfFormat, TimeGraph,
    },
};
use raphtory_api::core::entities::{
    properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds, VID,
};
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

/// The sorted N-Triples lines `to_rdf` writes for a view.
fn exported<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, RdfFormat::NTriples, &[]).unwrap();
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
    /// The number of pairs.
    pairs: usize,
    /// The number of full scans (of a layer or of all) that read the edges by id.
    by_edge_id: usize,
    /// The number of full scans of a layer of the view, or of all layers of a view with some.
    with_layers: usize,
}

/// Checks every scan of `view` against its subset of [`view_pairs`], and export and SPARQL
/// against [`view_pairs`]. Full scans are checked when read by edge id (in chunks of
/// every size) and from the nodes.
fn assert_scans_match(name: &str, view: &DynamicGraph) -> Checked {
    let reference = view_pairs(view);
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
    let check = |what: &dyn Fn() -> String, scan: EdgeScan, expected: Vec<Pair>| {
        let scan = scan_set(scan);
        if scan != expected {
            panic!("{name}: {}: scan {scan:?} != edges() {expected:?}", what());
        }
    };
    let mut by_edge_id = 0;
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
        by_edge_id += usize::from(all.reads_by_edge_id());
        with_layers += usize::from(match layer {
            None => !matches!(view.layer_ids(), LayerIds::None),
            Some(l) => l != STATIC_GRAPH_LAYER_ID && view.layer_ids().contains(&l),
        });
        check(&|| format!("all {layer:?}"), all, expect(&|_| true));
        check(
            &|| format!("all by nodes {layer:?}"),
            EdgeScan::all_by_nodes(view.clone(), layer),
            expect(&|_| true),
        );
        for first_chunk in [1, 2] {
            check(
                &|| format!("all in chunks from {first_chunk} {layer:?}"),
                EdgeScan::all_in_chunks(view.clone(), layer, first_chunk),
                expect(&|_| true),
            );
        }
        for &v in &vids {
            for dir in [Dir::Out, Dir::In] {
                let around = EdgeScan::around(view.clone(), v, dir, layer);
                let expected = expect(&|&(s, _, o)| match dir {
                    Dir::Out => s == v.0,
                    Dir::In => o == v.0,
                });
                // a node that the view hides has no edges
                if view.node(v).is_none() {
                    assert!(
                        expected.is_empty(),
                        "{name}: {v:?} hidden with {expected:?}"
                    );
                }
                check(
                    &|| format!("around {v:?} {dir:?} {layer:?}"),
                    around,
                    expected,
                );
            }
            for &o in &vids {
                check(
                    &|| format!("between {v:?} {o:?} {layer:?}"),
                    EdgeScan::between(view.clone(), v, o, layer),
                    expect(&|&(s, _, d)| s == v.0 && d == o.0),
                );
            }
        }
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
    Checked {
        pairs: reference.len(),
        by_edge_id,
        with_layers,
    }
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

/// The views of the fixture: one of each kind, on the persistent graph, on a read-only view of
/// it and on a graph with tiny segments.
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
            let checked = assert_scans_match(&format!("{graph} {stack:?}"), &view);
            nonempty += usize::from(checked.pairs > 0);
            if graph == "persistent" {
                persistent_pairs.push(checked.pairs);
            }
            // views with only windows and layers read full scans by edge id, and filtered
            // views read every node
            if unfiltered(stack) {
                assert!(checked.with_layers > 0, "{graph} {stack:?}");
                assert_eq!(checked.by_edge_id, checked.with_layers, "{graph} {stack:?}");
            } else {
                assert_eq!(checked.by_edge_id, 0, "{graph} {stack:?}");
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

fn dataset_of(pg: &PersistentGraph) -> RaphtoryDataset {
    RaphtoryDataset::new(pg.clone())
}

/// Repeated full scans are kept only while they fit in the cap, and replayed in the order of a
/// scan of the nodes; every request gives the reference triples.
#[test]
fn repeated_scans_beyond_the_memo_cap() {
    let pg = fixture(PersistentGraph::new());
    let reference = view_pairs(&pg.clone().into_dynamic());
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
        let dataset = dataset_of(&pg).with_scan_memo_cap(cap);
        for i in 0..3 {
            assert_eq!(read(&dataset, Some(p)), in_p, "cap {cap}, read {i}");
        }
        let kept = usize::from(cap >= in_p.len());
        assert_eq!(dataset.num_kept_scans(), kept, "cap {cap}");
    }
    // `q` is kept after `p` if both fit
    let caps = [0, 1, in_q.len() - 2, in_q.len() - 1, in_q.len()].map(|c| in_p.len() + c);
    for cap in caps {
        let dataset = dataset_of(&pg).with_scan_memo_cap(cap);
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
    let dataset = dataset_of(&pg).with_scan_memo_cap(reference.len() / 2);
    for i in 0..3 {
        assert_eq!(read(&dataset, None), reference, "read {i} of all");
    }
    assert_eq!(dataset.num_kept_scans(), 0);

    // a time graph is a scan of its own
    let iri = NamedNode::new("raphtory:asof:5").unwrap();
    let at_5 = RdfTerm::TimeGraph(TimeGraph::parse(&iri).unwrap().unwrap().into());
    let reference_at_5 = view_pairs(&pg.snapshot_at(5).into_dynamic());
    let dataset = dataset_of(&pg);
    for (graph, expected) in [(None, &reference), (Some(&at_5), &reference_at_5)] {
        for i in 0..3 {
            let mut triples = full_scan_of(&dataset, None, Some(graph));
            if i > 0 {
                let mut by_nodes = triples.clone();
                by_nodes.sort_by_key(|&(s, l, o)| (s, o, l));
                assert_eq!(triples, by_nodes, "{graph:?}");
            }
            triples.sort();
            assert_eq!(&triples, expected, "{graph:?} read {i}");
        }
    }
    assert_eq!(dataset.num_kept_scans(), 2);
}

/// Whether a view stack has only windows and layer restrictions.
fn unfiltered(stack: &[ViewOp]) -> bool {
    stack.iter().all(|op| {
        matches!(
            op,
            ViewOp::Window(..)
                | ViewOp::SnapshotAt(_)
                | ViewOp::Layers(_)
                | ViewOp::ExcludeLayers(_)
        )
    })
}
