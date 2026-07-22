use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use rand::{rngs::SmallRng, SeedableRng};
use raphtory::{
    algorithms::{
        alternating_mask::alternating_mask,
        bipartite::max_weight_matching::max_weight_matching,
        centrality::{
            betweenness::betweenness_centrality, degree_centrality::degree_centrality, hits::hits,
            pagerank::page_rank,
        },
        community_detection::{
            label_propagation::label_propagation, louvain::louvain, modularity::ModularityUnDir,
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
            temporal_rich_club_coefficient::temporal_rich_club_coefficient,
            three_node_motifs::{
                init_star_count, init_tri_count, init_two_node_count, new_triangle_edge,
                star_event, two_node_event,
            },
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
    db::{
        api::view::{Filter, StaticGraphViewOps},
        graph::views::{
            filter::{
                model::{
                    degree_filter::DegreeFilterFactory, property_filter::ops::PropertyFilterOps,
                },
                Unfiltered,
            },
            node_subgraph::NodeSubgraph,
        },
    },
    graphgen::random_attachment::random_attachment,
    prelude::*,
};
use raphtory_api::core::Direction;
use std::hint::black_box;

fn graph_benchmark_with_setup<G, BuildGraph, Setup, Run, SetupData, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    setup: Setup,
    mut run: Run,
) where
    G: StaticGraphViewOps,
    BuildGraph: FnOnce() -> G,
    Setup: Fn(&G) -> SetupData,
    Run: FnMut(&G, &SetupData) -> Output,
{
    let mut group = c.benchmark_group(name);
    let graph = build_graph();
    let setup_data = setup(&graph);

    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(measurement_secs));
    group.sample_size(sample_size);
    group.bench_function(name, |b| {
        b.iter(|| {
            let result = run(&graph, &setup_data);
            black_box(result);
        });
    });
    group.finish()
}

fn graph_benchmark<G, BuildGraph, Run, Output>(
    c: &mut Criterion,
    name: &str,
    measurement_secs: u64,
    sample_size: usize,
    build_graph: BuildGraph,
    run: Run,
) where
    G: StaticGraphViewOps,
    BuildGraph: FnOnce() -> G,
    Run: FnMut(&G, &()) -> Output,
{
    graph_benchmark_with_setup(
        c,
        name,
        measurement_secs,
        sample_size,
        build_graph,
        |_| (),
        run,
    )
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

// Graph Constructors

fn large_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 5000, 4, Some(seed));
    graph
}

fn large_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = large_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

fn first_node_id<G: StaticGraphViewOps>(graph: &G) -> GID {
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

fn large_weighted_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = large_weighted_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

fn large_random_attachment_filtered() -> impl StaticGraphViewOps {
    large_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

fn large_weighted_random_attachment_filtered() -> impl StaticGraphViewOps {
    large_weighted_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
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

fn large_typed_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = large_typed_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

fn large_typed_random_attachment_filtered() -> impl StaticGraphViewOps {
    large_typed_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

fn large_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = large_random_attachment_graph();
    graph.default_layer()
}

fn large_weighted_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = large_weighted_random_attachment_graph();
    graph.default_layer()
}

fn large_typed_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = large_typed_random_attachment_graph();
    graph.default_layer()
}

fn small_random_attachment_graph() -> Graph {
    let graph = Graph::new();
    let seed: [u8; 32] = [1; 32];
    random_attachment(&graph, 500, 4, Some(seed));
    graph
}

fn small_random_attachment_subgraph() -> NodeSubgraph<Graph> {
    let graph = small_random_attachment_graph();
    let subgraph = graph.subgraph(graph.nodes());
    subgraph
}

fn small_random_attachment_filtered() -> impl StaticGraphViewOps {
    small_random_attachment_graph()
        .filter(NodeFilter.degree().ge(0u64))
        .unwrap()
}

fn small_random_attachment_layered() -> impl StaticGraphViewOps {
    let graph = small_random_attachment_graph();
    graph.default_layer()
}

// Benchmarks

pub fn local_triangle_count_analysis(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "local_triangle_count",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_triangle_count(graph, node_id.clone()).unwrap(),
    );

    graph_benchmark_with_setup(
        c,
        "local_triangle_count_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, node_id| local_triangle_count(graph, node_id.clone()).unwrap(),
    );
    graph_benchmark_with_setup(
        c,
        "local_triangle_count_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, node_id| local_triangle_count(graph, node_id.clone()).unwrap(),
    );
    graph_benchmark_with_setup(
        c,
        "local_triangle_count_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, node_id| local_triangle_count(graph, node_id.clone()).unwrap(),
    );
}

pub fn local_clustering_coefficient_analysis(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "local_clustering_coefficient",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient(graph, node_id.clone()),
    );
    graph_benchmark_with_setup(
        c,
        "local_clustering_coefficient_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient(graph, node_id.clone()),
    );
    graph_benchmark_with_setup(
        c,
        "local_clustering_coefficient_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, node_id| local_clustering_coefficient(graph, node_id.clone()),
    );
    graph_benchmark_with_setup(
        c,
        "local_clustering_coefficient_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, node_id| local_clustering_coefficient(graph, node_id.clone()),
    )
}

pub fn graphgen_large_clustering_coeff(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_clustering_coeff",
        60,
        10,
        large_random_attachment_graph,
        |graph, _| global_clustering_coefficient(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_clustering_coeff_subgraph",
        60,
        10,
        large_random_attachment_subgraph,
        |graph, _| global_clustering_coefficient(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_clustering_coeff_layered",
        60,
        10,
        large_random_attachment_layered,
        |graph, _| global_clustering_coefficient(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_clustering_coeff_graph_filtered",
        60,
        10,
        large_random_attachment_filtered,
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
    );
    graph_benchmark(
        c,
        "graphgen_large_pagerank_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_pagerank_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_pagerank_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| page_rank(graph, None, Some(100), None, None, true, None),
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
    );
    graph_benchmark(
        c,
        "graphgen_large_concomp_subgraph",
        60,
        10,
        large_random_attachment_subgraph,
        |graph, _| weakly_connected_components(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_concomp_layered",
        60,
        10,
        large_random_attachment_layered,
        |graph, _| weakly_connected_components(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_concomp_graph_filtered",
        60,
        10,
        large_random_attachment_filtered,
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
    );
    graph_benchmark(
        c,
        "graphgen_large_hits_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| hits(graph, 100, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_hits_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| hits(graph, 100, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_hits_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| hits(graph, 100, None),
    )
}

pub fn graphgen_large_degree_centrality(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_degree_centrality",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| degree_centrality(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_degree_centrality_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| degree_centrality(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_degree_centrality_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| degree_centrality(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_degree_centrality_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| degree_centrality(graph),
    )
}

pub fn graphgen_large_betweenness(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_betweenness",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_betweenness_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_betweenness_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_betweenness_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| betweenness_centrality(graph, None, false),
    )
}

pub fn graphgen_large_triangle_count(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_triangle_count",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| triangle_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triangle_count_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| triangle_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triangle_count_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| triangle_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triangle_count_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| triangle_count(graph, None),
    )
}

pub fn graphgen_large_triplet_count(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_triplet_count",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| triplet_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triplet_count_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| triplet_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triplet_count_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| triplet_count(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_triplet_count_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| triplet_count(graph, None),
    )
}

pub fn graphgen_large_directed_density(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_directed_density",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_directed_density_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_directed_density_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| directed_graph_density(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_directed_density_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| directed_graph_density(graph),
    )
}

pub fn graphgen_large_reciprocity(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_reciprocity",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| global_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_reciprocity_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| global_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_reciprocity_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| global_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_reciprocity_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| global_reciprocity(graph),
    )
}

pub fn graphgen_large_scc(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_scc",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| strongly_connected_components(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_scc_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| strongly_connected_components(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_scc_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| strongly_connected_components(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_scc_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| strongly_connected_components(graph),
    )
}

pub fn graphgen_large_in_components(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_in_components",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| in_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| in_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| in_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| in_components(graph, None),
    )
}

pub fn graphgen_large_out_components(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_out_components",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| out_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| out_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| out_components(graph, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| out_components(graph, None),
    )
}

pub fn graphgen_large_in_components_filtered(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_in_components_filtered",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| in_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_filtered_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| in_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_filtered_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| in_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_in_components_filtered_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| in_components_filtered(graph, None, Unfiltered).unwrap(),
    )
}

pub fn graphgen_large_out_components_filtered(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_out_components_filtered",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| out_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_filtered_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| out_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_filtered_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| out_components_filtered(graph, None, Unfiltered).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_out_components_filtered_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| out_components_filtered(graph, None, Unfiltered).unwrap(),
    )
}

pub fn graphgen_large_label_propagation(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_label_propagation",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| label_propagation(graph, 20, Some([1; 32]), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_label_propagation_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| label_propagation(graph, 20, Some([1; 32]), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_label_propagation_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| label_propagation(graph, 20, Some([1; 32]), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_label_propagation_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| label_propagation(graph, 20, Some([1; 32]), None),
    )
}

pub fn graphgen_large_louvain(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_louvain",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42)),
    );
    graph_benchmark(
        c,
        "graphgen_large_louvain_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42)),
    );
    graph_benchmark(
        c,
        "graphgen_large_louvain_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42)),
    );
    graph_benchmark(
        c,
        "graphgen_large_louvain_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42)),
    )
}

pub fn graphgen_large_alternating_mask(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_alternating_mask",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| alternating_mask(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_alternating_mask_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| alternating_mask(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_alternating_mask_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| alternating_mask(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_alternating_mask_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| alternating_mask(graph),
    )
}

pub fn graphgen_large_all_local_reciprocity(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_all_local_reciprocity",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| all_local_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_all_local_reciprocity_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| all_local_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_all_local_reciprocity_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| all_local_reciprocity(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_all_local_reciprocity_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| all_local_reciprocity(graph),
    )
}

pub fn graphgen_large_balance(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_balance",
        20,
        10,
        large_weighted_random_attachment_graph,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_balance_subgraph",
        20,
        10,
        large_weighted_random_attachment_subgraph,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_balance_layered",
        20,
        10,
        large_weighted_random_attachment_layered,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    );
    graph_benchmark(
        c,
        "graphgen_large_balance_graph_filtered",
        20,
        10,
        large_weighted_random_attachment_filtered,
        |graph, _| balance(graph, "weight".to_string(), Direction::BOTH).unwrap(),
    )
}

pub fn graphgen_large_max_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_max_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| max_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| max_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| max_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| max_degree(graph),
    )
}

pub fn graphgen_large_min_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_min_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| min_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| min_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| min_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| min_degree(graph),
    )
}

pub fn graphgen_large_max_out_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_max_out_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| max_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_out_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| max_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_out_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| max_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_out_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| max_out_degree(graph),
    )
}

pub fn graphgen_large_max_in_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_max_in_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| max_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_in_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| max_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_in_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| max_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_in_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| max_in_degree(graph),
    )
}

pub fn graphgen_large_min_out_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_min_out_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| min_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_out_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| min_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_out_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| min_out_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_out_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| min_out_degree(graph),
    )
}

pub fn graphgen_large_min_in_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_min_in_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| min_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_in_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| min_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_in_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| min_in_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_min_in_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| min_in_degree(graph),
    )
}

pub fn graphgen_large_average_degree(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_average_degree",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| average_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_average_degree_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| average_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_average_degree_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| average_degree(graph),
    );
    graph_benchmark(
        c,
        "graphgen_large_average_degree_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| average_degree(graph),
    )
}

pub fn graphgen_large_local_clustering_coefficient_batch(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_local_clustering_coefficient_batch",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient_batch(graph, vec![node_id.clone()]),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_local_clustering_coefficient_batch_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, node_id| local_clustering_coefficient_batch(graph, vec![node_id.clone()]),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_local_clustering_coefficient_batch_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, node_id| local_clustering_coefficient_batch(graph, vec![node_id.clone()]),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_local_clustering_coefficient_batch_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, node_id| local_clustering_coefficient_batch(graph, vec![node_id.clone()]),
    )
}

pub fn graphgen_large_temporal_rich_club(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_temporal_rich_club",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| {
            let rolling = graph.rolling(1, Some(1)).unwrap();
            temporal_rich_club_coefficient(graph, rolling, 3, 3)
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_rich_club_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| {
            let rolling = graph.rolling(1, Some(1)).unwrap();
            temporal_rich_club_coefficient(graph, rolling, 3, 3)
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_rich_club_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| {
            let rolling = graph.rolling(1, Some(1)).unwrap();
            temporal_rich_club_coefficient(graph, rolling, 3, 3)
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_rich_club_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| {
            let rolling = graph.rolling(1, Some(1)).unwrap();
            temporal_rich_club_coefficient(graph, rolling, 3, 3)
        },
    )
}

pub fn graphgen_large_temporal_motif_multi(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_temporal_motif_multi",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| temporal_three_node_motif_multi(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_motif_multi_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| temporal_three_node_motif_multi(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_motif_multi_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| temporal_three_node_motif_multi(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_motif_multi_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| temporal_three_node_motif_multi(graph, vec![100], None),
    )
}

pub fn graphgen_large_local_temporal_motif(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_local_temporal_motif",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| local_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_local_temporal_motif_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| local_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_local_temporal_motif_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| local_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_local_temporal_motif_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| local_temporal_three_node_motif(graph, 100, None),
    )
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
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_dijkstra_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
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
    graph_benchmark_with_setup(
        c,
        "graphgen_large_dijkstra_layered",
        20,
        10,
        large_random_attachment_layered,
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
    graph_benchmark_with_setup(
        c,
        "graphgen_large_dijkstra_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
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
    graph_benchmark_with_setup(
        c,
        "graphgen_large_single_source_shortest_path",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| single_source_shortest_path(graph, source.clone(), None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_single_source_shortest_path_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| single_source_shortest_path(graph, source.clone(), None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_single_source_shortest_path_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| single_source_shortest_path(graph, source.clone(), None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_single_source_shortest_path_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| single_source_shortest_path(graph, source.clone(), None),
    )
}

pub fn graphgen_large_temporally_reachable_nodes(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_temporally_reachable_nodes",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_temporally_reachable_nodes_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_temporally_reachable_nodes_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None),
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_temporally_reachable_nodes_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| temporally_reachable_nodes(graph, None, 20, 0, vec![source.clone()], None),
    )
}

pub fn graphgen_large_in_component(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component(node)
        },
    )
}

pub fn graphgen_large_out_component(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component(node)
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component(node)
        },
    )
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
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_filtered_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component_filtered(node, Unfiltered).unwrap()
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_filtered_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component_filtered(node, Unfiltered).unwrap()
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_in_component_filtered_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            in_component_filtered(node, Unfiltered).unwrap()
        },
    )
}

pub fn graphgen_large_out_component_filtered(c: &mut Criterion) {
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_filtered",
        20,
        10,
        large_random_attachment_graph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component_filtered(node, Unfiltered).unwrap()
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_filtered_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component_filtered(node, Unfiltered).unwrap()
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_filtered_layered",
        20,
        10,
        large_random_attachment_layered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component_filtered(node, Unfiltered).unwrap()
        },
    );
    graph_benchmark_with_setup(
        c,
        "graphgen_large_out_component_filtered_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        first_node_id,
        |graph, source| {
            let node = graph.node(source.clone()).expect("source node exists");
            out_component_filtered(node, Unfiltered).unwrap()
        },
    )
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
    graph_benchmark(
        c,
        "graphgen_internal_global_triangle_motifs",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| global_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_global_triangle_motifs_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| global_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_global_triangle_motifs_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| global_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_global_triangle_motifs_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| global_triangle_motifs_internal(graph, vec![100], None),
    )
}

pub fn graphgen_internal_local_triangle_motifs(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_internal_local_triangle_motifs",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| local_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_local_triangle_motifs_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| local_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_local_triangle_motifs_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| local_triangle_motifs_internal(graph, vec![100], None),
    );
    graph_benchmark(
        c,
        "graphgen_internal_local_triangle_motifs_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| local_triangle_motifs_internal(graph, vec![100], None),
    )
}

pub fn graphgen_large_k_core_set(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_k_core_set",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| k_core_set(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_set_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| k_core_set(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_set_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| k_core_set(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_set_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| k_core_set(graph, 2, usize::MAX, None),
    )
}

pub fn graphgen_large_k_core(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_k_core",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| k_core(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| k_core(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| k_core(graph, 2, usize::MAX, None),
    );
    graph_benchmark(
        c,
        "graphgen_large_k_core_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| k_core(graph, 2, usize::MAX, None),
    )
}

pub fn graphgen_large_fruchterman_reingold(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_fruchterman_reingold",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_fruchterman_reingold_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_fruchterman_reingold_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_fruchterman_reingold_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1),
    )
}

pub fn graphgen_large_cohesive_fruchterman_reingold(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_cohesive_fruchterman_reingold",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_cohesive_fruchterman_reingold_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_cohesive_fruchterman_reingold_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
    graph_benchmark(
        c,
        "graphgen_large_cohesive_fruchterman_reingold_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1),
    )
}

pub fn graphgen_large_fast_rp(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_fast_rp",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_fast_rp_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_fast_rp_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None),
    );
    graph_benchmark(
        c,
        "graphgen_large_fast_rp_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| fast_rp(graph, 32, 0.5, vec![1.0, 1.0, 1.0], Some(1), None),
    )
}

pub fn graphgen_large_max_weight_matching(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_max_weight_matching",
        20,
        10,
        small_random_attachment_graph,
        |graph, _| max_weight_matching(graph, None, false, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_weight_matching_subgraph",
        20,
        10,
        small_random_attachment_subgraph,
        |graph, _| max_weight_matching(graph, None, false, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_weight_matching_layered",
        20,
        10,
        small_random_attachment_layered,
        |graph, _| max_weight_matching(graph, None, false, false),
    );
    graph_benchmark(
        c,
        "graphgen_large_max_weight_matching_graph_filtered",
        20,
        10,
        small_random_attachment_filtered,
        |graph, _| max_weight_matching(graph, None, false, false),
    )
}

pub fn graphgen_large_temporal_seir(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_temporal_seir",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| {
            let mut rng = SmallRng::seed_from_u64(1);
            temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng).unwrap()
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_seir_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| {
            let mut rng = SmallRng::seed_from_u64(1);
            temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng).unwrap()
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_seir_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| {
            let mut rng = SmallRng::seed_from_u64(1);
            temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng).unwrap()
        },
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_seir_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| {
            let mut rng = SmallRng::seed_from_u64(1);
            temporal_SEIR(graph, Some(0.1), None, 0.5f64, 0, Number(1), &mut rng).unwrap()
        },
    )
}

pub fn graphgen_large_temporal_bipartite_projection(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_large_temporal_bipartite_projection",
        20,
        10,
        large_typed_random_attachment_graph,
        |graph, _| temporal_bipartite_projection(graph, 1, "Right".to_string()),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_bipartite_projection_subgraph",
        20,
        10,
        large_typed_random_attachment_subgraph,
        |graph, _| temporal_bipartite_projection(graph, 1, "Right".to_string()),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_bipartite_projection_layered",
        20,
        10,
        large_typed_random_attachment_layered,
        |graph, _| temporal_bipartite_projection(graph, 1, "Right".to_string()),
    );
    graph_benchmark(
        c,
        "graphgen_large_temporal_bipartite_projection_graph_filtered",
        20,
        10,
        large_typed_random_attachment_filtered,
        |graph, _| temporal_bipartite_projection(graph, 1, "Right".to_string()),
    )
}

pub fn temporal_motifs(c: &mut Criterion) {
    graph_benchmark(
        c,
        "temporal_motifs",
        20,
        10,
        large_random_attachment_graph,
        |graph, _| global_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "temporal_motifs_subgraph",
        20,
        10,
        large_random_attachment_subgraph,
        |graph, _| global_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "temporal_motifs_layered",
        20,
        10,
        large_random_attachment_layered,
        |graph, _| global_temporal_three_node_motif(graph, 100, None),
    );
    graph_benchmark(
        c,
        "temporal_motifs_graph_filtered",
        20,
        10,
        large_random_attachment_filtered,
        |graph, _| global_temporal_three_node_motif(graph, 100, None),
    )
}

criterion_group!(
    benches,
    local_triangle_count_analysis,
    local_clustering_coefficient_analysis,
    graphgen_large_clustering_coeff,
    graphgen_large_pagerank,
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
