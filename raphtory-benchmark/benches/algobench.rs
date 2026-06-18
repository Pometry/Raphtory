use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use rand::{rngs::SmallRng, SeedableRng};
use raphtory::{
    algorithms::{
        alternating_mask::alternating_mask,
        bipartite::max_weight_matching::max_weight_matching,
        centrality::{
            betweenness::betweenness_centrality, degree_centrality::degree_centrality,
            hits::hits, new_pagerank::page_rank as new_page_rank, pagerank::page_rank,
        },
        community_detection::{
            label_propagation::label_propagation,
            louvain::louvain,
            modularity::ModularityUnDir,
        },
        components::{
            in_component, in_component_filtered, in_components, in_components_filtered,
            out_component, out_component_filtered, out_components, out_components_filtered,
            strongly_connected_components, weakly_connected_components,
        },
        cores::k_core::{k_core, k_core_set},
        dynamics::temporal::epidemics::{temporal_SEIR, Number},
        embeddings::fast_rp::fast_rp,
        layout::{
            cohesive_fruchterman_reingold::cohesive_fruchterman_reingold,
            fruchterman_reingold::fruchterman_reingold_unbounded,
        },
        metrics::{
            balance::balance,
            clustering_coefficient::{
                global_clustering_coefficient::global_clustering_coefficient,
                local_clustering_coefficient::local_clustering_coefficient,
                local_clustering_coefficient_batch::local_clustering_coefficient_batch,
            },
            degree::{
                average_degree, max_degree, max_in_degree, max_out_degree, min_degree,
                min_in_degree, min_out_degree,
            },
            directed_graph_density::directed_graph_density,
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
            local_triangle_count::local_triangle_count,
            three_node_motifs::{
                init_star_count, init_tri_count, init_two_node_count, new_triangle_edge,
                star_event, two_node_event,
            },
            temporal_rich_club_coefficient::temporal_rich_club_coefficient,
            triangle_count::triangle_count,
            triplet_count::triplet_count,
        },
        pathing::{
            dijkstra::dijkstra_single_source_shortest_paths,
            single_source_shortest_path::single_source_shortest_path,
            temporal_reachability::temporally_reachable_nodes,
        },
        projections::temporal_bipartite_projection::temporal_bipartite_projection,
    },
    db::graph::views::filter::Unfiltered,
    graphgen::random_attachment::random_attachment,
    prelude::*,
};
use raphtory_api::core::Direction;
use raphtory_benchmark::common::bench;
use std::hint::black_box;

fn graph_benchmark_with_setup<BuildGraph, Setup, Run, SetupData, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    setup: Setup,
    mut run: Run,
) where
    BuildGraph: FnOnce() -> Graph,
    Setup: FnOnce(&Graph) -> SetupData,
    Run: FnMut(&Graph, &SetupData) -> Output,
{
    let mut group = c.benchmark_group(name);
    let graph = build_graph();
    let setup_data = setup(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(measurement_secs));
    group.sample_size(sample_size);
    group.bench_with_input(BenchmarkId::new(name, &graph), &graph, |b, graph| {
        b.iter(|| {
            let result = run(graph, &setup_data);
            black_box(result);
        });
    });
    group.finish()
}

fn graph_benchmark<BuildGraph, Run, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    run: Run,
) where
    BuildGraph: FnOnce() -> Graph,
    Run: FnMut(&Graph, &()) -> Output,
{
    graph_benchmark_with_setup(c, name, measurement_secs, sample_size, build_graph, |_| (), run)
}

fn simple_benchmark<Run, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    mut run: Run,
) where
    Run: FnMut() -> Output,
{
    let mut group = c.benchmark_group(name);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(measurement_secs));
    group.sample_size(sample_size);
    group.bench_function(name, |b| {
        b.iter(|| {
            let result = run();
            black_box(result);
        });
    });
    group.finish()
}

fn large_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 500000, 4, Some(seed));
    graph
}

fn first_node_id(graph: &Graph) -> GID {
    graph
        .nodes()
        .id()
        .iter_values()
        .next()
        .expect("graph has nodes")
}

fn large_weighted_random_attachment_graph() -> Graph {
    let graph = large_random_attachment_graph();
    let ids = graph.nodes().id().iter_values().collect::<Vec<_>>();
    if let (Some(src), Some(dst)) = (ids.first(), ids.get(1)) {
        graph
            .add_edge(0, src.clone(), dst.clone(), [("weight", 1.0f64)], None)
            .expect("unable to add weighted edge");
    }
    graph
}

fn large_typed_random_attachment_graph() -> Graph {
    let graph = large_random_attachment_graph();
    for id in graph.nodes().id().iter_values() {
        graph
            .add_node(0, id, NO_PROPS, Some("Right"), None)
            .expect("unable to set node type");
    }
    graph
}

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_triangle_count");
    group.sample_size(10);
    bench(&mut group, "local_triangle_count", None, |b| {
        let graph = large_random_attachment_graph();
        let node_id = graph.nodes().id().iter_values().next().expect("graph has nodes");

        b.iter(|| black_box(local_triangle_count(&graph, node_id.clone()).unwrap()))
    });

    group.finish();
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_clustering_coefficient");

    bench(&mut group, "local_clustering_coefficient", None, |b| {
        let graph = large_random_attachment_graph();
        let node_id = graph.nodes().id().iter_values().next().expect("graph has nodes");

        b.iter(|| black_box(local_clustering_coefficient(&graph, node_id.clone())))
    });

    group.finish();
}

pub fn graphgen_large_clustering_coeff(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_clustering_coeff",
        60,
        10,
        large_random_attachment_graph,
        |graph, _| global_clustering_coefficient(graph),
    )
}

pub fn graphgen_large_pagerank(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_pagerank",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    )
}

pub fn graphgen_large_new_pagerank(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_new_pagerank",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| new_page_rank(graph, None, Some(100), None, None, true, None),
    )
}


pub fn graphgen_large_concomp(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_concomp",
        60,
        10,
        large_random_attachment_graph,
        |graph, _| weakly_connected_components(graph),
    )
}

pub fn graphgen_large_hits(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_hits",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| hits(graph, 100, None),
    )
}

pub fn graphgen_large_degree_centrality(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_degree_centrality");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_degree_centrality", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = degree_centrality(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_betweenness(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_betweenness");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_betweenness", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = betweenness_centrality(graph, None, false);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_triangle_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_triangle_count");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_triangle_count", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = triangle_count(graph, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_triplet_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_triplet_count");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_triplet_count", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = triplet_count(graph, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_directed_density(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_directed_density");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_directed_density", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = directed_graph_density(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_reciprocity(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_reciprocity");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_reciprocity", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = global_reciprocity(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_scc(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_scc");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_scc", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = strongly_connected_components(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_in_components(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_in_components");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_in_components", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = in_components(graph, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_out_components(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_out_components");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_out_components", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = out_components(graph, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_in_components_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_in_components_filtered");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_in_components_filtered", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = in_components_filtered(graph, None, Unfiltered).unwrap();
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_out_components_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_out_components_filtered");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_out_components_filtered", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = out_components_filtered(graph, None, Unfiltered).unwrap();
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_label_propagation(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_label_propagation");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_label_propagation", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = label_propagation(graph, 20, Some([1; 32]), None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_louvain(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_louvain");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_louvain", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_alternating_mask(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_alternating_mask");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_alternating_mask", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = alternating_mask(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_all_local_reciprocity(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_all_local_reciprocity");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_all_local_reciprocity", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = all_local_reciprocity(graph);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_balance(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_balance",
        20,
        10,
        large_weighted_random_attachment_graph,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    )
}

pub fn graphgen_large_max_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_max_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_max_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(max_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_min_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_min_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_min_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(min_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_max_out_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_max_out_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_max_out_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(max_out_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_max_in_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_max_in_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_max_in_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(max_in_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_min_out_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_min_out_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_min_out_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(min_out_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_min_in_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_min_in_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_min_in_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(min_in_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_average_degree(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_average_degree");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_average_degree", &graph),
        &graph,
        |b, graph| {
            b.iter(|| black_box(average_degree(graph)));
        },
    );
    group.finish()
}

pub fn graphgen_large_local_clustering_coefficient_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_local_clustering_coefficient_batch");
    let graph = large_random_attachment_graph();
    let node_id = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_local_clustering_coefficient_batch", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = local_clustering_coefficient_batch(graph, vec![node_id.clone()]);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_temporal_rich_club(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_temporal_rich_club");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_temporal_rich_club", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let rolling = graph.rolling(1, Some(1)).unwrap();
                let result = temporal_rich_club_coefficient(graph, rolling, 3, 3);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_temporal_motif_multi(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_temporal_motif_multi");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_temporal_motif_multi", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = temporal_three_node_motif_multi(graph, vec![100], None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_local_temporal_motif(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_local_temporal_motif");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_local_temporal_motif", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = local_temporal_three_node_motif(graph, 100, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_dijkstra(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_dijkstra",
        20,
        10,
        large_random_attachment_graph,
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
    )
}

pub fn graphgen_large_single_source_shortest_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_single_source_shortest_path");
    let graph = large_random_attachment_graph();
    let source = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_single_source_shortest_path", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = single_source_shortest_path(graph, source.clone(), None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_temporally_reachable_nodes(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_temporally_reachable_nodes");
    let graph = large_random_attachment_graph();
    let source = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_temporally_reachable_nodes", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result =
                    temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_in_component(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_in_component");
    let graph = large_random_attachment_graph();
    let source = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_in_component", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let node = graph.node(source.clone()).expect("source node exists");
                let result = in_component(node);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_out_component(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_out_component");
    let graph = large_random_attachment_graph();
    let source = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_out_component", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let node = graph.node(source.clone()).expect("source node exists");
                let result = out_component(node);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_in_component_filtered(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_filtered",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component_filtered(node, Unfiltered).unwrap()
        },
    )
}

pub fn graphgen_large_out_component_filtered(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_out_component_filtered");
    let graph = large_random_attachment_graph();
    let source = first_node_id(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_out_component_filtered", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let node = graph.node(source.clone()).expect("source node exists");
                let result = out_component_filtered(node, Unfiltered).unwrap();
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_internal_two_node_event(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_two_node_event", 20, 10, || {
        two_node_event(1, 100)
    })
}

pub fn graphgen_internal_init_two_node_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_two_node_count", 20, 10, || {
        init_two_node_count()
    })
}

pub fn graphgen_internal_star_event(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_star_event", 20, 10, || {
        star_event(0, 1, 100)
    })
}

pub fn graphgen_internal_init_star_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_star_count", 20, 10, || {
        init_star_count(128)
    })
}

pub fn graphgen_internal_new_triangle_edge(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_new_triangle_edge", 20, 10, || {
        new_triangle_edge(true, 1, 0, 1, 100)
    })
}

pub fn graphgen_internal_init_tri_count(c: &mut Criterion) {
    simple_benchmark(c, "graphgen_internal_init_tri_count", 20, 10, || {
        init_tri_count(128)
    })
}

pub fn graphgen_internal_global_triangle_motifs(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_internal_global_triangle_motifs");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_internal_global_triangle_motifs", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = global_triangle_motifs_internal(graph, vec![100], None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_internal_local_triangle_motifs(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_internal_local_triangle_motifs");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_internal_local_triangle_motifs", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = local_triangle_motifs_internal(graph, vec![100], None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_k_core_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_k_core_set");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_k_core_set", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = k_core_set(graph, 2, usize::MAX, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_k_core(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_k_core");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_k_core", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = k_core(graph, 2, usize::MAX, None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_fruchterman_reingold(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_fruchterman_reingold");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_fruchterman_reingold", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_cohesive_fruchterman_reingold(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_cohesive_fruchterman_reingold");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_cohesive_fruchterman_reingold", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_fast_rp(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_fast_rp");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_fast_rp", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_max_weight_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_max_weight_matching");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_max_weight_matching", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = max_weight_matching(graph, None, false, false);
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_temporal_seir(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_temporal_seir");
    let graph = large_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_temporal_seir", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let mut rng = SmallRng::seed_from_u64(1);
                let result = temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng)
                    .unwrap();
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn graphgen_large_temporal_bipartite_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("graphgen_large_temporal_bipartite_projection");
    let graph = large_typed_random_attachment_graph();

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);
    group.bench_with_input(
        BenchmarkId::new("graphgen_large_temporal_bipartite_projection", &graph),
        &graph,
        |b, graph| {
            b.iter(|| {
                let result = temporal_bipartite_projection(graph, 1, "Right".to_string());
                black_box(result);
            });
        },
    );
    group.finish()
}

pub fn temporal_motifs(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_motifs");

    bench(&mut group, "temporal_motifs", None, |b| {
        let graph = large_random_attachment_graph();

        b.iter(|| black_box(global_temporal_three_node_motif(&graph, 100, None)))
    });

    group.finish();
}

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_large_clustering_coeff,
    graphgen_large_pagerank,
    graphgen_large_new_pagerank,
    graphgen_large_concomp,
    graphgen_large_hits,
    graphgen_large_degree_centrality,
    graphgen_large_betweenness,
    graphgen_large_triangle_count,
    graphgen_large_triplet_count,
    graphgen_large_directed_density,
    graphgen_large_reciprocity,
    graphgen_large_scc,
    graphgen_large_in_components,
    graphgen_large_out_components,
    graphgen_large_in_components_filtered,
    graphgen_large_out_components_filtered,
    graphgen_large_label_propagation,
    graphgen_large_louvain,
    graphgen_large_alternating_mask,
    graphgen_large_all_local_reciprocity,
    graphgen_large_balance,
    graphgen_large_max_degree,
    graphgen_large_min_degree,
    graphgen_large_max_out_degree,
    graphgen_large_max_in_degree,
    graphgen_large_min_out_degree,
    graphgen_large_min_in_degree,
    graphgen_large_average_degree,
    graphgen_large_local_clustering_coefficient_batch,
    graphgen_large_temporal_rich_club,
    graphgen_large_temporal_motif_multi,
    graphgen_large_local_temporal_motif,
    graphgen_large_dijkstra,
    graphgen_large_single_source_shortest_path,
    graphgen_large_temporally_reachable_nodes,
    graphgen_large_in_component,
    graphgen_large_out_component,
    graphgen_large_in_component_filtered,
    graphgen_large_out_component_filtered,
    graphgen_large_k_core_set,
    graphgen_large_k_core,
    graphgen_large_fruchterman_reingold,
    graphgen_large_cohesive_fruchterman_reingold,
    graphgen_large_fast_rp,
    graphgen_large_max_weight_matching,
    graphgen_large_temporal_seir,
    graphgen_large_temporal_bipartite_projection,
    graphgen_internal_two_node_event,
    graphgen_internal_init_two_node_count,
    graphgen_internal_star_event,
    graphgen_internal_init_star_count,
    graphgen_internal_new_triangle_edge,
    graphgen_internal_init_tri_count,
    graphgen_internal_global_triangle_motifs,
    graphgen_internal_local_triangle_motifs,
    temporal_motifs,
);
criterion_main!(benches);
