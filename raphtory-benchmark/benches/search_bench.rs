use criterion::{criterion_group, criterion_main, Criterion};
use once_cell::sync::Lazy;
use rand::{seq::SliceRandom, thread_rng};
use raphtory::{
    core::IntoProp,
    db::{
        api::view::{internal::InternalIndexSearch, SearchableGraphOps},
        graph::views::{
            deletion_graph::PersistentGraph,
            property_filter::{CompositeEdgeFilter, CompositeNodeFilter, Filter},
        },
    },
    prelude::{
        AdditionOps, EdgePropertyFilterOps, Graph, GraphViewOps, NodePropertyFilterOps,
        NodeViewOps, PropertyFilter, StableDecode,
    },
};
use std::{sync::Arc, time::Instant};

static GRAPH: Lazy<Arc<PersistentGraph>> = Lazy::new(|| {
    // let graph = PersistentGraph::decode("/tmp/graphs/SPARK-22915").unwrap();
    let graph = PersistentGraph::decode("/tmp/graphs/master").unwrap();

    let start = Instant::now();
    let _ = graph.searcher().unwrap();
    let duration = start.elapsed();
    println!("Time taken to initialize graph and indexes: {:?}", duration);

    Arc::new(graph)
});

fn setup_graph() -> Arc<PersistentGraph> {
    Arc::clone(&GRAPH)
}

fn get_node_names() -> Vec<&'static str> {
    vec![
        "SPARK-22644",
        "SPARK-22882",
        "smurakozi",
        "SPARK-22915",
        "SPARK-22883",
        "gsomogyi",
        "SPARK-22886",
        "attilapiros",
        "SPARK-22887",
        "SPARK-22881",
        "SPARK-22884",
        "weichenxu123",
        "SPARK-22885",
        "josephkb",
    ]
}

fn get_property_filters() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("issuetype", "Test"),
        PropertyFilter::eq("summary", "StructuredStreaming"),
        PropertyFilter::eq("priority", "Major"),
        // PropertyFilter::eq("description", "Transformers"),
        // PropertyFilter::eq("duration", 1u64),
        PropertyFilter::eq("Fix Version", "2.3.0"),
        PropertyFilter::eq("resolution", "Fixed"),
        PropertyFilter::eq("status", "Resolved"),
    ]
}

fn bench_search_nodes_by_name(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_node_names();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_name", |b| {
        b.iter(|| {
            let random_name = *node_names.choose(&mut rng).unwrap();
            let filter = CompositeNodeFilter::Node(Filter::eq("node_name", random_name));
            graph.search_nodes(&filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_property(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters();

    c.bench_function("search_nodes_by_property", |b| {
        b.iter(|| {
            let mut rng = thread_rng();
            let random_filter = property_filters.choose(&mut rng).unwrap();
            let filter = CompositeNodeFilter::Property(random_filter.clone());
            graph.search_nodes(&filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_count(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64));

    c.bench_function("search_nodes_count", |b| {
        b.iter(|| graph.search_nodes_count(&filter).unwrap())
    });
}

fn bench_search_edges_by_src_dst(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeEdgeFilter::And(vec![
        CompositeEdgeFilter::Edge(Filter::eq("from", "1")),
        CompositeEdgeFilter::Edge(Filter::eq("to", "2")),
    ]);

    c.bench_function("search_edges_by_src_dst", |b| {
        b.iter(|| graph.search_edges(&filter, 5, 0).unwrap())
    });
}

fn bench_search_edges_by_property(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeEdgeFilter::Property(PropertyFilter::eq("ep2", 2u64));

    c.bench_function("search_edges_by_property", |b| {
        b.iter(|| graph.search_edges(&filter, 5, 0).unwrap())
    });
}

fn bench_search_edges_count(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeEdgeFilter::Property(PropertyFilter::eq("ep2", 2u64));

    c.bench_function("search_edges_count", |b| {
        b.iter(|| graph.search_edges_count(&filter).unwrap())
    });
}

fn bench_search_nodes_by_name_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_node_names();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_name_raph", |b| {
        b.iter(|| {
            let random_name = node_names.choose(&mut rng).unwrap();
            graph.node(random_name).unwrap();
        })
    });
}

fn bench_search_nodes_by_property_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let property_filters = get_property_filters();
    let random_filter = property_filters.choose(&mut rng).unwrap();

    c.bench_function("search_nodes_by_property_raph", |b| {
        b.iter(|| graph.filter_nodes(random_filter.clone()).unwrap())
    });
}

fn bench_search_nodes_count_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = PropertyFilter::eq("p3", 1u64);

    c.bench_function("search_nodes_count_raph", |b| {
        b.iter(|| graph.filter_nodes(filter.clone()).iter().count())
    });
}

fn bench_search_edges_by_src_dst_raph(c: &mut Criterion) {
    let graph = setup_graph();
    c.bench_function("search_edges_by_src_dst_raph", |b| {
        b.iter(|| graph.edge("3", "1").unwrap())
    });
}

fn bench_search_edges_by_property_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = PropertyFilter::eq("ep2", 2u64);

    c.bench_function("search_edges_by_property_raph", |b| {
        b.iter(|| graph.filter_edges(filter.clone()).unwrap())
    });
}

fn bench_search_edges_count_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = PropertyFilter::eq("ep2", 2u64);

    c.bench_function("search_edges_count_raph", |b| {
        b.iter(|| graph.filter_edges(filter.clone()).iter().count())
    });
}

criterion_group!(
    search_benches,
    bench_search_nodes_by_name,
    bench_search_nodes_by_property,
    // bench_search_nodes_count,
    // bench_search_edges_by_src_dst,
    // bench_search_edges_by_property,
    // bench_search_edges_count
);

criterion_group!(
    search_benches_raph,
    bench_search_nodes_by_name_raph,
    bench_search_nodes_by_property_raph,
    //     bench_search_nodes_count_raph,
    //     bench_search_edges_by_src_dst_raph,
    //     bench_search_edges_by_property_raph,
    //     bench_search_edges_count_raph
);

criterion_main!(search_benches, search_benches_raph);
