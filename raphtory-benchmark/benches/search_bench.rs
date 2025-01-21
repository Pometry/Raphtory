use criterion::{criterion_group, criterion_main, Criterion};
use raphtory::{
    core::IntoProp,
    db::{
        api::view::SearchableGraphOps,
        graph::views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter, Filter},
    },
    prelude::{
        AdditionOps, EdgePropertyFilterOps, Graph, GraphViewOps, NodePropertyFilterOps,
        NodeViewOps, PropertyFilter,
    },
};

fn setup_graph() -> Graph {
    let graph = Graph::new();
    graph
        .add_node(1, 1, [("p1", "shivam_kapoor")], Some("fire_nation"))
        .unwrap();
    graph
        .add_node(
            2,
            2,
            [("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
            Some("air_nomads"),
        )
        .unwrap();
    graph
        .add_node(3, 3, [("p2", 6u64), ("p3", 1u64)], Some("fire_nation"))
        .unwrap();
    graph.add_node(3, 4, [("p4", "pometry")], None).unwrap();
    graph.add_node(4, 4, [("p5", 12u64)], None).unwrap();

    graph
        .add_edge(1, 1, 2, [("ep1", "shivam_kapoor")], Some("pometry"))
        .unwrap();
    graph
        .add_edge(
            2,
            2,
            3,
            [("ep1", "prop12".into_prop()), ("ep2", 2u64.into_prop())],
            Some("network"),
        )
        .unwrap();
    graph
        .add_edge(3, 3, 1, [("ep2", 6u64), ("ep3", 1u64)], Some("disks"))
        .unwrap();

    graph
}

fn bench_search_nodes_by_name(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeNodeFilter::Node(Filter::eq("node_name", "3"));

    c.bench_function("search_nodes_by_name", |b| {
        b.iter(|| graph.search_nodes(&filter, 5, 0).unwrap())
    });
}

fn bench_search_nodes_by_property(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64));

    c.bench_function("search_nodes_by_property", |b| {
        b.iter(|| graph.search_nodes(&filter, 5, 0).unwrap())
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
    c.bench_function("search_nodes_by_name_raph", |b| {
        b.iter(|| graph.node(3).unwrap())
    });
}

fn bench_search_nodes_by_property_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let filter = PropertyFilter::eq("p2", 2u64);

    c.bench_function("search_nodes_by_property_raph", |b| {
        b.iter(|| graph.filter_nodes(filter.clone()).unwrap())
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
    bench_search_nodes_count,
    bench_search_edges_by_src_dst,
    bench_search_edges_by_property,
    bench_search_edges_count
);

criterion_group!(
    search_benches_raph,
    bench_search_nodes_by_name_raph,
    bench_search_nodes_by_property_raph,
    bench_search_nodes_count_raph,
    bench_search_edges_by_src_dst_raph,
    bench_search_edges_by_property_raph,
    bench_search_edges_count_raph
);

criterion_main!(search_benches, search_benches_raph);
