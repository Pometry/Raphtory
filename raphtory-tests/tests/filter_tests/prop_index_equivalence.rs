//! Storage-index equivalence: property filters must return identical node
//! sets on two graphs holding the same data, whatever the storage backend
//! does to accelerate them.
//!
//! The baseline graph is never flushed, so its data stays in the mutable
//! in-memory head and every filter is a plain scan. The disk graph is flushed
//! (and, where supported, gets secondary property indexes built). The suite
//! then interleaves further updates so index-accelerated segments coexist
//! with fresh unindexed data, and checks again after re-building indexes.
//! Filters cover every operator (including ones no index can serve) under
//! plain, windowed and composed views.
//!
//! The same equivalence must hold through persistent (deletion-aware) views:
//! both graphs are re-viewed via `persistent_graph()` over fixtures that
//! include edge deletions, and edge-validity filters (`is_valid`,
//! `is_deleted`, `is_active`) are composed with the property filters.

use proptest::prelude::*;
use raphtory::{
    db::{
        api::view::{filter_ops::Filter, StaticGraphViewOps},
        graph::views::filter::model::{
            edge_filter::EdgeFilter, node_filter::NodeFilter,
            property_filter::ops::PropertyFilterOps, ComposableFilter, EdgeViewFilterOps,
            PropertyFilterFactory, TemporalPropertyFilterFactory,
        },
    },
    prelude::*,
};
use raphtory_api::core::{entities::properties::prop::Prop, storage::arc_str::OptionAsStr};
use raphtory_storage::core_ops::CoreGraphOps;
use raphtory_tests::utils::{build_graph_strat, GraphFixture};
use std::collections::{BTreeMap, BTreeSet};

/// Batch 2 gets a disjoint id space so metadata and node types never collide
/// with batch 1 (metadata is set-once).
const BATCH_OFFSET: u64 = 1_000_000;

/// The two batches are generated independently, so the same property name can
/// come out with different dtypes; renaming batch 2's properties keeps the
/// graph schema consistent.
fn prefix_props(fixture: &GraphFixture, prefix: &str) -> GraphFixture {
    let mut fixture = fixture.clone();
    let rename = |props: &mut raphtory_tests::utils::PropUpdatesFixture| {
        for (_, updates) in props.t_props.iter_mut() {
            for (name, _) in updates.iter_mut() {
                *name = format!("{prefix}{name}");
            }
        }
        for (name, _) in props.c_props.iter_mut() {
            *name = format!("{prefix}{name}");
        }
    };
    for updates in fixture.nodes.0.values_mut() {
        rename(&mut updates.props);
    }
    for updates in fixture.edges.0.values_mut() {
        rename(&mut updates.props);
    }
    fixture
}

fn apply_fixture(g: &Graph, fixture: &GraphFixture, id_offset: u64) {
    for ((src, dst, layer), updates) in fixture.edges() {
        let (src, dst) = (src + id_offset, dst + id_offset);
        for (t, props) in updates.props.t_props.iter() {
            g.add_edge(*t, src, dst, props.clone(), layer).unwrap();
        }
        if let Some(e) = g.edge(src, dst) {
            if !updates.props.c_props.is_empty() {
                e.add_metadata(updates.props.c_props.clone(), layer)
                    .unwrap();
            }
        }
        for t in updates.deletions.iter() {
            g.delete_edge(*t, src, dst, layer).unwrap();
        }
    }
    for (node, updates) in fixture.nodes() {
        let node = node + id_offset;
        let node_layer = updates.node_layer.as_str();
        for (t, props) in updates.props.t_props.iter() {
            g.add_node(*t, node, props.clone(), None, node_layer)
                .unwrap();
        }
        if let Some(n) = g.node(node) {
            n.add_metadata(updates.props.c_props.clone()).unwrap();
            if let Some(node_type) = updates.node_type.as_str() {
                n.set_node_type(node_type).unwrap();
            }
        }
    }
}

/// Distinct property values per name, deterministically ordered.
fn prop_values(
    fixtures: &[&GraphFixture],
) -> (BTreeMap<String, Vec<Prop>>, BTreeMap<String, Vec<Prop>>) {
    let mut temporal: BTreeMap<String, Vec<Prop>> = BTreeMap::new();
    let mut metadata: BTreeMap<String, Vec<Prop>> = BTreeMap::new();
    for fixture in fixtures {
        for (_, updates) in fixture.nodes() {
            for (_, props) in updates.props.t_props.iter() {
                for (name, value) in props {
                    temporal
                        .entry(name.clone())
                        .or_default()
                        .push(value.clone());
                }
            }
            for (name, value) in updates.props.c_props.iter() {
                metadata
                    .entry(name.clone())
                    .or_default()
                    .push(value.clone());
            }
        }
    }
    for values in temporal.values_mut().chain(metadata.values_mut()) {
        values.sort_by_key(|p| format!("{p:?}"));
        values.dedup_by_key(|p| format!("{p:?}"));
    }
    (temporal, metadata)
}

fn all_times(fixtures: &[&GraphFixture]) -> Vec<i64> {
    let mut times: Vec<i64> = fixtures
        .iter()
        .flat_map(|f| {
            f.nodes()
                .flat_map(|(_, u)| u.props.t_props.iter().map(|(t, _)| *t))
                .chain(
                    f.edges()
                        .flat_map(|(_, u)| u.props.t_props.iter().map(|(t, _)| *t)),
                )
                .chain(f.edges().flat_map(|(_, u)| u.deletions.iter().copied()))
        })
        .collect();
    times.sort_unstable();
    times.dedup();
    times
}

fn substring_of(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.is_empty() {
        return String::new();
    }
    let start = chars.len() / 4;
    let end = (start + 4).min(chars.len());
    chars[start..end].iter().collect()
}

/// Runs one filter on the same view of both graphs; node-name sets (or the
/// errors) must be identical.
macro_rules! check_one {
    ($mem:expr, $disk:expr, $filter:expr, $ctx:expr) => {{
        let f = $filter;
        let a = match $mem.filter(f.clone()) {
            Ok(view) => Ok(view
                .nodes()
                .iter()
                .map(|n| n.name())
                .collect::<BTreeSet<String>>()),
            Err(e) => Err(e.to_string()),
        };
        let b = match $disk.filter(f) {
            Ok(view) => Ok(view
                .nodes()
                .iter()
                .map(|n| n.name())
                .collect::<BTreeSet<String>>()),
            Err(e) => Err(e.to_string()),
        };
        assert_eq!(a, b, "index/scan mismatch: {}", $ctx);
    }};
}

/// Runs one filter across all views (full, windows, before/after, layers).
macro_rules! check_views {
    ($mem:expr, $disk:expr, $mid:expr, $layers:expr, $filter:expr, $ctx:expr) => {{
        let f = $filter;
        let mid = $mid;
        check_one!($mem, $disk, f.clone(), format!("{} [full]", $ctx));
        check_one!(
            $mem.before(mid),
            $disk.before(mid),
            f.clone(),
            format!("{} [before {mid}]", $ctx)
        );
        check_one!(
            $mem.after(mid),
            $disk.after(mid),
            f.clone(),
            format!("{} [after {mid}]", $ctx)
        );
        check_one!(
            $mem.window(mid.saturating_sub(10), mid.saturating_add(10)),
            $disk.window(mid.saturating_sub(10), mid.saturating_add(10)),
            f.clone(),
            format!("{} [window around {mid}]", $ctx)
        );
        check_one!(
            $mem.at(mid),
            $disk.at(mid),
            f.clone(),
            format!("{} [at {mid}]", $ctx)
        );
        let layers: &Vec<String> = $layers;
        for layer in layers.iter().take(2) {
            check_one!(
                $mem.layers(layer.as_str()).unwrap(),
                $disk.layers(layer.as_str()).unwrap(),
                f.clone(),
                format!("{} [layer {layer}]", $ctx)
            );
        }
        if layers.len() > 1 {
            let names: Vec<&str> = layers.iter().map(|s| s.as_str()).collect();
            check_one!(
                $mem.layers(names.clone()).unwrap(),
                $disk.layers(names).unwrap(),
                f,
                format!("{} [all layers]", $ctx)
            );
        }
    }};
}

/// Runs one filter on the same view of both graphs; edge "src->dst" sets (or
/// the errors) must be identical.
macro_rules! check_one_edges {
    ($mem:expr, $disk:expr, $filter:expr, $ctx:expr) => {{
        let f = $filter;
        let a = match $mem.filter(f.clone()) {
            Ok(view) => Ok(view
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<BTreeSet<String>>()),
            Err(e) => Err(e.to_string()),
        };
        let b = match $disk.filter(f) {
            Ok(view) => Ok(view
                .edges()
                .iter()
                .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
                .collect::<BTreeSet<String>>()),
            Err(e) => Err(e.to_string()),
        };
        assert_eq!(a, b, "index/scan mismatch (edges): {}", $ctx);
    }};
}

/// Runs one edge filter across all views, comparing edge sets.
macro_rules! check_edge_views {
    ($mem:expr, $disk:expr, $mid:expr, $layers:expr, $filter:expr, $ctx:expr) => {{
        let f = $filter;
        let mid = $mid;
        check_one_edges!($mem, $disk, f.clone(), format!("{} [full]", $ctx));
        check_one_edges!(
            $mem.before(mid),
            $disk.before(mid),
            f.clone(),
            format!("{} [before {mid}]", $ctx)
        );
        check_one_edges!(
            $mem.after(mid),
            $disk.after(mid),
            f.clone(),
            format!("{} [after {mid}]", $ctx)
        );
        check_one_edges!(
            $mem.window(mid.saturating_sub(10), mid.saturating_add(10)),
            $disk.window(mid.saturating_sub(10), mid.saturating_add(10)),
            f.clone(),
            format!("{} [window around {mid}]", $ctx)
        );
        check_one_edges!(
            $mem.at(mid),
            $disk.at(mid),
            f.clone(),
            format!("{} [at {mid}]", $ctx)
        );
        let layers: &Vec<String> = $layers;
        for layer in layers.iter().take(2) {
            check_one_edges!(
                $mem.layers(layer.as_str()).unwrap(),
                $disk.layers(layer.as_str()).unwrap(),
                f.clone(),
                format!("{} [layer {layer}]", $ctx)
            );
        }
    }};
}

fn layer_names(fixtures: &[&GraphFixture]) -> Vec<String> {
    let mut names: Vec<String> = fixtures
        .iter()
        .flat_map(|f| {
            f.edges()
                .filter_map(|((_, _, layer), _)| layer.map(str::to_string))
                .chain(
                    f.nodes()
                        .filter_map(|(_, u)| u.node_layer.as_str().map(str::to_string)),
                )
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

fn compare_all_filters<MG: StaticGraphViewOps, DG: StaticGraphViewOps>(
    mem: &MG,
    disk: &DG,
    fixtures: &[&GraphFixture],
    phase: &str,
) {
    let (temporal, metadata) = prop_values(fixtures);
    let times = all_times(fixtures);
    let mid = times.get(times.len() / 2).copied().unwrap_or(0);
    let layers = layer_names(fixtures);

    // sanity: the graphs agree before any filtering
    let mem_nodes: BTreeSet<String> = mem.nodes().iter().map(|n| n.name()).collect();
    let disk_nodes: BTreeSet<String> = disk.nodes().iter().map(|n| n.name()).collect();
    assert_eq!(mem_nodes, disk_nodes, "node sets diverged in {phase}");

    let mut first_two = Vec::new();
    for (name, values) in &temporal {
        let v = values.first().unwrap().clone();
        let w = values.last().unwrap().clone();
        let p = NodeFilter.property(name.as_str());

        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.eq(v.clone()),
            format!("{phase}: {name} eq")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.ne(v.clone()),
            format!("{phase}: {name} ne")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.is_in([v.clone(), w.clone()]),
            format!("{phase}: {name} is_in")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.is_not_in([v.clone()]),
            format!("{phase}: {name} is_not_in")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.is_some(),
            format!("{phase}: {name} is_some")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.is_none(),
            format!("{phase}: {name} is_none")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.lt(w.clone()),
            format!("{phase}: {name} lt")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.le(v.clone()),
            format!("{phase}: {name} le")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.gt(v.clone()),
            format!("{phase}: {name} gt")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.ge(w.clone()),
            format!("{phase}: {name} ge")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.temporal().any().eq(v.clone()),
            format!("{phase}: {name} temporal any eq")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            p.temporal().last().eq(w.clone()),
            format!("{phase}: {name} temporal last eq")
        );

        if let Prop::Str(s) = &v {
            let pat = substring_of(s);
            check_views!(
                mem,
                disk,
                mid,
                &layers,
                p.contains(pat.clone()),
                format!("{phase}: {name} contains {pat:?}")
            );
            check_views!(
                mem,
                disk,
                mid,
                &layers,
                p.not_contains(pat.clone()),
                format!("{phase}: {name} not_contains {pat:?}")
            );
            let prefix: String = s.chars().take(2).collect();
            check_views!(
                mem,
                disk,
                mid,
                &layers,
                p.starts_with(prefix.clone()),
                format!("{phase}: {name} starts_with {prefix:?}")
            );
            let suffix: String = {
                let chars: Vec<char> = s.chars().collect();
                chars[chars.len().saturating_sub(2)..].iter().collect()
            };
            check_views!(
                mem,
                disk,
                mid,
                &layers,
                p.ends_with(suffix.clone()),
                format!("{phase}: {name} ends_with {suffix:?}")
            );
        }

        if first_two.len() < 2 {
            first_two.push((name.clone(), v));
        }
    }

    for (name, values) in &metadata {
        let v = values.first().unwrap().clone();
        let m = NodeFilter.metadata(name.as_str());
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            m.eq(v.clone()),
            format!("{phase}: meta {name} eq")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            m.ne(v.clone()),
            format!("{phase}: meta {name} ne")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            m.is_some(),
            format!("{phase}: meta {name} is_some")
        );
        if let Prop::Str(s) = &v {
            let pat = substring_of(s);
            check_views!(
                mem,
                disk,
                mid,
                &layers,
                m.contains(pat.clone()),
                format!("{phase}: meta {name} contains {pat:?}")
            );
        }
    }

    // composition: and / or / not over the first two temporal props
    if let [(n1, v1), (n2, v2)] = first_two.as_slice() {
        let f1 = NodeFilter.property(n1.as_str()).eq(v1.clone());
        let f2 = NodeFilter.property(n2.as_str()).eq(v2.clone());
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            f1.clone().and(f2.clone()),
            format!("{phase}: {n1} AND {n2}")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            f1.clone().or(f2.clone()),
            format!("{phase}: {n1} OR {n2}")
        );
        check_views!(
            mem,
            disk,
            mid,
            &layers,
            f1.clone().not(),
            format!("{phase}: NOT {n1}")
        );
    }
}

/// Edge-validity filters (only meaningful under persistent semantics) alone
/// and composed with node property filters; edge sets and node sets must
/// agree between the scan graph and the indexed graph.
fn compare_valid_edges<MG: StaticGraphViewOps, DG: StaticGraphViewOps>(
    mem: &MG,
    disk: &DG,
    fixtures: &[&GraphFixture],
    phase: &str,
) {
    let times = all_times(fixtures);
    let mid = times.get(times.len() / 2).copied().unwrap_or(0);
    let layers = layer_names(fixtures);

    check_edge_views!(
        mem,
        disk,
        mid,
        &layers,
        EdgeFilter.is_valid(),
        format!("{phase}: is_valid")
    );
    check_edge_views!(
        mem,
        disk,
        mid,
        &layers,
        EdgeFilter.is_deleted(),
        format!("{phase}: is_deleted")
    );
    check_edge_views!(
        mem,
        disk,
        mid,
        &layers,
        EdgeFilter.is_active(),
        format!("{phase}: is_active")
    );

    let (temporal, _) = prop_values(fixtures);
    if let Some((name, values)) = temporal.iter().next() {
        let v = values.first().unwrap().clone();
        let np = NodeFilter.property(name.as_str()).eq(v);

        // chained: restrict to currently-valid edges, then push the node
        // property filter (with its index domain) on top
        let m = mem.filter(EdgeFilter.is_valid());
        let d = disk.filter(EdgeFilter.is_valid());
        match (m, d) {
            (Ok(m), Ok(d)) => {
                check_one!(
                    m,
                    d,
                    np.clone(),
                    format!("{phase}: is_valid -> {name} eq [nodes]")
                );
                check_one_edges!(
                    m,
                    d,
                    np.clone(),
                    format!("{phase}: is_valid -> {name} eq [edges]")
                );
            }
            (m, d) => assert_eq!(
                m.err().map(|e| e.to_string()),
                d.err().map(|e| e.to_string()),
                "is_valid filter construction diverged in {phase}"
            ),
        }

        // single combined expression mixing edge validity and node property
        check_one!(
            mem,
            disk,
            EdgeFilter.is_valid().and(np.clone()),
            format!("{phase}: is_valid AND {name} eq [nodes]")
        );
        check_one_edges!(
            mem,
            disk,
            EdgeFilter.is_valid().and(np),
            format!("{phase}: is_valid AND {name} eq [edges]")
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// Same random data in an unflushed in-memory graph (pure scan) and a
    /// flushed+indexed disk graph must filter identically — before indexing,
    /// with fresh unindexed updates on top of a built index, and after
    /// rebuilding the index.
    #[test]
    fn filters_agree_with_and_without_index(
        batch1 in build_graph_strat(8, 10, 5, 5, false),
        batch2 in build_graph_strat(8, 10, 5, 5, false),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let batch2 = prefix_props(&batch2, "b2_");

        let mem = Graph::new();
        apply_fixture(&mem, &batch1, 0);

        let disk = Graph::new_at_path(dir.path()).unwrap();
        apply_fixture(&disk, &batch1, 0);
        disk.flush().unwrap();
        disk.core_graph().build_node_prop_index().unwrap();

        compare_all_filters(&mem, &disk, &[&batch1], "phase 1 (indexed)");

        // fresh updates land in the unindexed head on the disk graph
        apply_fixture(&mem, &batch2, BATCH_OFFSET);
        apply_fixture(&disk, &batch2, BATCH_OFFSET);
        compare_all_filters(&mem, &disk, &[&batch1, &batch2], "phase 2 (stale index + new updates)");

        // rebuild: everything indexed again
        disk.core_graph().build_node_prop_index().unwrap();
        compare_all_filters(&mem, &disk, &[&batch1, &batch2], "phase 3 (rebuilt index)");
    }

    /// The deployment base case: persistent (deletion-aware) views over
    /// graphs with edge deletions. Every property filter plus the
    /// edge-validity filters must agree between the unflushed scan graph and
    /// the flushed+indexed disk graph through `persistent_graph()` views.
    #[test]
    fn filters_agree_on_persistent_graph(
        batch1 in build_graph_strat(8, 10, 5, 5, true),
        batch2 in build_graph_strat(8, 10, 5, 5, true),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let batch2 = prefix_props(&batch2, "b2_");

        let mem = Graph::new();
        apply_fixture(&mem, &batch1, 0);
        let mem_pg = mem.persistent_graph();

        let disk = Graph::new_at_path(dir.path()).unwrap();
        apply_fixture(&disk, &batch1, 0);
        disk.flush().unwrap();
        disk.core_graph().build_node_prop_index().unwrap();
        let disk_pg = disk.persistent_graph();

        compare_all_filters(&mem_pg, &disk_pg, &[&batch1], "persistent phase 1 (indexed)");
        compare_valid_edges(&mem_pg, &disk_pg, &[&batch1], "persistent phase 1 (indexed)");

        // fresh updates and deletions land in the unindexed head on the disk graph
        apply_fixture(&mem, &batch2, BATCH_OFFSET);
        apply_fixture(&disk, &batch2, BATCH_OFFSET);
        compare_all_filters(
            &mem_pg,
            &disk_pg,
            &[&batch1, &batch2],
            "persistent phase 2 (stale index + new updates)",
        );
        compare_valid_edges(
            &mem_pg,
            &disk_pg,
            &[&batch1, &batch2],
            "persistent phase 2 (stale index + new updates)",
        );

        // rebuild: everything indexed again
        disk.core_graph().build_node_prop_index().unwrap();
        compare_all_filters(&mem_pg, &disk_pg, &[&batch1, &batch2], "persistent phase 3 (rebuilt index)");
        compare_valid_edges(&mem_pg, &disk_pg, &[&batch1, &batch2], "persistent phase 3 (rebuilt index)");
    }
}
