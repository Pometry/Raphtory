// Trivial / fast algorithms (sub-millisecond to ~1ms on the 5000-node large graph).
//
// `directed_graph_density` is this binary's representative for graph/subgraph/layered/filtered
// view coverage; every other algorithm here only benchmarks the plain graph.

use raphtory::{
    algorithms::{
        alternating_mask::alternating_mask,
        centrality::degree_centrality::degree_centrality,
        components::{out_component, out_component_filtered},
        metrics::{
            clustering_coefficient::{
                local_clustering_coefficient::local_clustering_coefficient,
                local_clustering_coefficient_batch::local_clustering_coefficient_batch,
            },
            degree::{
                average_degree, max_degree, max_in_degree, max_out_degree, min_degree,
                min_in_degree, min_out_degree,
            },
            directed_graph_density::directed_graph_density,
        },
        motifs::{
            local_triangle_count::local_triangle_count,
            three_node_motifs::{
                init_star_count, init_tri_count, init_two_node_count, new_triangle_edge,
                star_event, two_node_event,
            },
        },
        pathing::temporal_reachability::temporally_reachable_nodes,
        components::weakly_connected_components,
        dynamics::temporal::epidemics::{temporal_SEIR, Number},
    },
    db::graph::views::filter::Unfiltered,
    prelude::*,
};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rngs::SmallRng, SeedableRng};
use raphtory_benchmark::algobench_common::{
    first_node_id, graph_benchmark, graph_benchmark_with_setup, large_random_attachment_filtered,
    large_random_attachment_graph, large_random_attachment_layered,
    large_random_attachment_subgraph, simple_benchmark,
};

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "local_triangle_count",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_triangle_count(graph, node_id.clone()).unwrap(),
    );
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "local_clustering_coefficient",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient(graph, node_id.clone()),
    );
}

pub fn graphgen_directed_density(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_directed_density",
        10,
        large_random_attachment_graph,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_directed_density_subgraph",
        10,
        large_random_attachment_subgraph,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_directed_density_layered",
        10,
        large_random_attachment_layered,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_directed_density_graph_filtered",
        10,
        large_random_attachment_filtered,
        |graph, _| directed_graph_density(graph),
    )
}

pub fn graphgen_degree_centrality(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_degree_centrality",
        10,
        large_random_attachment_graph,
        |graph, _| degree_centrality(graph),
    );
}

pub fn graphgen_concomp(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_concomp",
        10,
        large_random_attachment_graph,
        |graph, _| weakly_connected_components(graph),
    );
}

pub fn graphgen_alternating_mask(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_alternating_mask",
        10,
        large_random_attachment_graph,
        |graph, _| alternating_mask(graph),
    );
}

pub fn graphgen_max_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_max_degree",
        10,
        large_random_attachment_graph,
        |graph, _| max_degree(graph),
    );
}

pub fn graphgen_min_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_min_degree",
        10,
        large_random_attachment_graph,
        |graph, _| min_degree(graph),
    );
}

pub fn graphgen_max_out_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_max_out_degree",
        10,
        large_random_attachment_graph,
        |graph, _| max_out_degree(graph),
    );
}

pub fn graphgen_max_in_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_max_in_degree",
        10,
        large_random_attachment_graph,
        |graph, _| max_in_degree(graph),
    );
}

pub fn graphgen_min_out_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_min_out_degree",
        10,
        large_random_attachment_graph,
        |graph, _| min_out_degree(graph),
    );
}

pub fn graphgen_min_in_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_min_in_degree",
        10,
        large_random_attachment_graph,
        |graph, _| min_in_degree(graph),
    );
}

pub fn graphgen_average_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_average_degree",
        10,
        large_random_attachment_graph,
        |graph, _| average_degree(graph),
    );
}

pub fn graphgen_local_clustering_coefficient_batch(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_local_clustering_coefficient_batch",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient_batch(graph, vec![node_id.clone()]),
    );
}

pub fn graphgen_temporally_reachable_nodes(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_temporally_reachable_nodes",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None),
    );
}

pub fn graphgen_out_component(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_out_component",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component(node)
        },
    );
}

pub fn graphgen_out_component_filtered(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_out_component_filtered",
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component_filtered(node, Unfiltered).unwrap()
        },
    );
}

pub fn graphgen_temporal_seir(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_temporal_seir",
        10,
        large_random_attachment_graph,
        |graph, _| {
            let mut rng = SmallRng::seed_from_u64(1);
            temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng).unwrap()
        },
    );
}

pub fn graphgen_internal_two_node_event(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_two_node_event", 10, || {
        two_node_event(1, 100)
    })
}

pub fn graphgen_internal_init_two_node_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_two_node_count", 10, || {
        init_two_node_count()
    })
}

pub fn graphgen_internal_star_event(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_star_event", 10, || {
        star_event(0, 1, 100)
    })
}

pub fn graphgen_internal_init_star_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_star_count", 10, || {
        init_star_count(128)
    })
}

pub fn graphgen_internal_new_triangle_edge(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_new_triangle_edge", 10, || {
        new_triangle_edge(true, 1, 0, 1, 100)
    })
}

pub fn graphgen_internal_init_tri_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_tri_count", 10, || {
        init_tri_count(128)
    })
}

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_directed_density,
    graphgen_degree_centrality,
    graphgen_concomp,
    graphgen_alternating_mask,
    graphgen_max_degree,
    graphgen_min_degree,
    graphgen_max_out_degree,
    graphgen_max_in_degree,
    graphgen_min_out_degree,
    graphgen_min_in_degree,
    graphgen_average_degree,
    graphgen_local_clustering_coefficient_batch,
    graphgen_temporally_reachable_nodes,
    graphgen_out_component,
    graphgen_out_component_filtered,
    graphgen_temporal_seir,
    graphgen_internal_two_node_event,
    graphgen_internal_init_two_node_count,
    graphgen_internal_star_event,
    graphgen_internal_init_star_count,
    graphgen_internal_new_triangle_edge,
    graphgen_internal_init_tri_count,
);
criterion_main!(benches);
