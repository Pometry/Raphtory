use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::{
    algorithms::{
        bipartite::max_weight_matching::max_weight_matching,
        centrality::betweenness::betweenness_centrality,
        components::{
            in_components, in_components_filtered, out_components, out_components_filtered,
        },
        layout::{
            cohesive_fruchterman_reingold::cohesive_fruchterman_reingold,
            fruchterman_reingold::fruchterman_reingold_unbounded,
        },
        motifs::temporal_rich_club_coefficient::temporal_rich_club_coefficient,
    },
    db::graph::views::filter::Unfiltered,
    prelude::*,
};
use raphtory_benchmark::algobench_common::{
    graph_benchmark, tiny_random_attachment_filtered, tiny_random_attachment_graph,
    tiny_random_attachment_layered, tiny_random_attachment_subgraph,
};

pub fn graphgen_betweenness(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_betweenness",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_betweenness_subgraph",
        20,
        10,
        tiny_random_attachment_subgraph,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_betweenness_layered",
        20,
        10,
        tiny_random_attachment_layered,
        |graph, _| betweenness_centrality(graph, None, false),
    );
    graph_benchmark(
        c,
        "graphgen_betweenness_graph_filtered",
        20,
        10,
        tiny_random_attachment_filtered,
        |graph, _| betweenness_centrality(graph, None, false),
    )
}

pub fn graphgen_in_components(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_in_components",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| in_components(graph, None),
    );
}

pub fn graphgen_out_components(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_out_components",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| out_components(graph, None),
    );
}

pub fn graphgen_in_components_filtered(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_in_components_filtered",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| in_components_filtered(graph, None, Unfiltered).unwrap(),
    );
}

pub fn graphgen_out_components_filtered(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_out_components_filtered",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| out_components_filtered(graph, None, Unfiltered).unwrap(),
    );
}

pub fn graphgen_temporal_rich_club(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_temporal_rich_club",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| {
            let rolling = graph.rolling(1, Some(1)).unwrap();
            temporal_rich_club_coefficient(graph, rolling, 3, 3)
        },
    );
}

pub fn graphgen_fruchterman_reingold(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_fruchterman_reingold",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| fruchterman_reingold_unbounded(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
}

pub fn graphgen_cohesive_fruchterman_reingold(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_cohesive_fruchterman_reingold",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| cohesive_fruchterman_reingold(graph, 5, 1.0, 1.0, 0.9, 0.1),
    );
}

pub fn graphgen_max_weight_matching(c: &mut Criterion) {
    graph_benchmark(
        c,
        "graphgen_max_weight_matching",
        20,
        10,
        tiny_random_attachment_graph,
        |graph, _| max_weight_matching(graph, None, false, false),
    );
}

criterion_group!(
    benches,
    graphgen_betweenness,
    graphgen_in_components,
    graphgen_out_components,
    graphgen_in_components_filtered,
    graphgen_out_components_filtered,
    graphgen_temporal_rich_club,
    graphgen_fruchterman_reingold,
    graphgen_cohesive_fruchterman_reingold,
    graphgen_max_weight_matching,
);
criterion_main!(benches);
