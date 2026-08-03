// Medium complexity algorithms (roughly ~1ms - 200ms), run against a dedicated 1500-node
// medium graph rather than the large (5000 node) or tiny (100 node) graphs used by the
// fast/slow tiers.
//
// `pagerank` is this binary's representative for graph/subgraph/layered/filtered view
// coverage; every other algorithm here only benchmarks the plain graph.

use raphtory::{
    algorithms::{
        centrality::{hits::hits, pagerank::page_rank},
        community_detection::{
            label_propagation::label_propagation, louvain::louvain, modularity::ModularityUnDir,
        },
        components::{in_component, in_component_filtered, strongly_connected_components},
        cores::k_core::{k_core, k_core_set},
        embeddings::fast_rp::fast_rp,
        metrics::{
            balance::balance,
            clustering_coefficient::global_clustering_coefficient::global_clustering_coefficient,
            reciprocity::{all_local_reciprocity, global_reciprocity},
        },
        motifs::{
            global_temporal_three_node_motifs::{
                global_temporal_three_node_motif, temporal_three_node_motif_multi,
                triangle_motifs as global_triangle_motifs_internal,
            },
            local_temporal_three_node_motifs::{
                temporal_three_node_motif as local_temporal_three_node_motif,
                triangle_motifs as local_triangle_motifs_internal,
            },
            triangle_count::triangle_count,
            triplet_count::triplet_count,
        },
        pathing::{
            dijkstra::dijkstra_single_source_shortest_paths,
            single_source_shortest_path::single_source_shortest_path,
        },
        projections::temporal_bipartite_projection::temporal_bipartite_projection,
    },
    db::graph::views::filter::Unfiltered,
    prelude::*,
};
use criterion::{criterion_group, criterion_main, Criterion};
use raphtory_api::core::Direction;
use raphtory_benchmark::algobench_common::{
    first_node_id, graph_benchmark, graph_benchmark_with_setup, medium_random_attachment_filtered,
    medium_random_attachment_graph, medium_random_attachment_layered,
    medium_random_attachment_subgraph, medium_typed_random_attachment_graph,
    medium_weighted_random_attachment_graph,
};

pub fn graphgen_medium_clustering_coeff(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_clustering_coeff",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| global_clustering_coefficient(graph),
    );
}

pub fn graphgen_medium_pagerank(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_pagerank",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    );
    graph_benchmark(
        c,
        "graphgen_medium_pagerank_subgraph",
        10,
        10,
        medium_random_attachment_subgraph,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    );
    graph_benchmark(
        c,
        "graphgen_medium_pagerank_layered",
        10,
        10,
        medium_random_attachment_layered,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    );
    graph_benchmark(
        c,
        "graphgen_medium_pagerank_graph_filtered",
        20,
        10,
        medium_random_attachment_filtered,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    )
}

pub fn graphgen_medium_hits(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_hits",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| hits(graph, 100, None),
    );
}

pub fn graphgen_medium_triangle_count(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_triangle_count",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| triangle_count(graph, None),
    );
}

pub fn graphgen_medium_triplet_count(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_triplet_count",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| triplet_count(graph, None),
    );
}

pub fn graphgen_medium_reciprocity(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_reciprocity",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| global_reciprocity(graph),
    );
}

pub fn graphgen_medium_scc(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_scc",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| strongly_connected_components(graph),
    );
}

pub fn graphgen_medium_label_propagation(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_label_propagation",
        20,
        10,
        medium_random_attachment_graph,
        |graph, _| label_propagation(graph, 20, Some([1; 32]), None),
    );
}

pub fn graphgen_medium_louvain(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_louvain",
        20,
        10,
        medium_random_attachment_graph,
        |graph, _| louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42)),
    );
}

pub fn graphgen_medium_all_local_reciprocity(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_all_local_reciprocity",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| all_local_reciprocity(graph),
    );
}

pub fn graphgen_medium_balance(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_balance",
        5,
        10,
        medium_weighted_random_attachment_graph,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    );
}

pub fn graphgen_medium_temporal_motif_multi(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_temporal_motif_multi",
        20,
        10,
        medium_random_attachment_graph,
        |graph, _| temporal_three_node_motif_multi(graph, vec![100], None),
    );
}

pub fn graphgen_medium_local_temporal_motif(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_local_temporal_motif",
        20,
        10,
        medium_random_attachment_graph,
        |graph, _| local_temporal_three_node_motif(graph, 100, None),
    );
}

pub fn graphgen_medium_dijkstra(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_medium_dijkstra",
        5,
        10,
        medium_random_attachment_graph,
        first_node_id,
        |graph, source| {
            dijkstra_single_source_shortest_paths(
                graph,
                source.clone(),
                vec![source.clone()],
                None,
                Direction::BOTH,
            )
            .unwrap()
        },
    );
}

pub fn graphgen_medium_single_source_shortest_path(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_medium_single_source_shortest_path",
        5,
        10,
        medium_random_attachment_graph,
        first_node_id,
        |graph, source| single_source_shortest_path(graph, source.clone(), None),
    );
}

pub fn graphgen_medium_in_component(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_medium_in_component",
        5,
        10,
        medium_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component(node)
        },
    );
}

pub fn graphgen_medium_in_component_filtered(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_medium_in_component_filtered",
        5,
        10,
        medium_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component_filtered(node, Unfiltered).unwrap()
        },
    );
}

pub fn graphgen_internal_global_triangle_motifs(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_internal_global_triangle_motifs",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| global_triangle_motifs_internal(graph, vec![100], None),
    );
}

pub fn graphgen_internal_local_triangle_motifs(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_internal_local_triangle_motifs",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| local_triangle_motifs_internal(graph, vec![100], None),
    );
}

pub fn graphgen_medium_k_core_set(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_k_core_set",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| k_core_set(graph, 2, usize::MAX, None),
    );
}

pub fn graphgen_medium_k_core(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_k_core",
        5,
        10,
        medium_random_attachment_graph,
        |graph, _| k_core(graph, 2, usize::MAX, None),
    );
}

pub fn graphgen_medium_fast_rp(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_fast_rp",
        10,
        10,
        medium_random_attachment_graph,
        |graph, _| fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None),
    );
}

pub fn graphgen_medium_temporal_bipartite_projection(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_medium_temporal_bipartite_projection",
        20,
        10,
        medium_typed_random_attachment_graph,
        |graph, _| temporal_bipartite_projection(graph, 1, "Right".to_string()),
    );
}

pub fn temporal_motifs(c: &mut Criterion) {
    graph_benchmark(
        c,
        "temporal_motifs",
        20,
        10,
        medium_random_attachment_graph,
        |graph, _| global_temporal_three_node_motif(graph, 100, None),
    );
}

criterion_group!(
    benches,
    graphgen_medium_clustering_coeff,
    graphgen_medium_pagerank,
    graphgen_medium_hits,
    graphgen_medium_triangle_count,
    graphgen_medium_triplet_count,
    graphgen_medium_reciprocity,
    graphgen_medium_scc,
    graphgen_medium_label_propagation,
    graphgen_medium_louvain,
    graphgen_medium_all_local_reciprocity,
    graphgen_medium_balance,
    graphgen_medium_temporal_motif_multi,
    graphgen_medium_local_temporal_motif,
    graphgen_medium_dijkstra,
    graphgen_medium_single_source_shortest_path,
    graphgen_medium_in_component,
    graphgen_medium_in_component_filtered,
    graphgen_internal_global_triangle_motifs,
    graphgen_internal_local_triangle_motifs,
    graphgen_medium_k_core_set,
    graphgen_medium_k_core,
    graphgen_medium_fast_rp,
    graphgen_medium_temporal_bipartite_projection,
    temporal_motifs,
);
criterion_main!(benches);
