use criterion::{criterion_group, criterion_main, Criterion};
use once_cell::sync::Lazy;
use rand::{seq::SliceRandom, thread_rng};
use raphtory::{
    core::{IntoProp, Prop},
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
use rayon::prelude::*;
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

fn get_property_filters_eq() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("issuetype", "Test"),
        PropertyFilter::eq("summary", "StructuredStreaming"),
        PropertyFilter::eq("priority", "Major"),
        PropertyFilter::eq("description", "Transformers"),
        PropertyFilter::eq("duration", 1i64),
        PropertyFilter::eq("Fix Version", "2.3.0"),
        PropertyFilter::eq("resolution", "Fixed"),
        PropertyFilter::eq("status", "Resolved"),
    ]
}

fn get_property_filters_ne() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::ne("issuetype", "Test"),
        PropertyFilter::ne("summary", "StructuredStreaming"),
        PropertyFilter::ne("priority", "Major"),
        PropertyFilter::ne("description", "Transformers"),
        PropertyFilter::ne("duration", 1i64),
        PropertyFilter::ne("Fix Version", "2.3.0"),
        PropertyFilter::ne("resolution", "Fixed"),
        PropertyFilter::ne("status", "Resolved"),
    ]
}

fn get_property_filters_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::any("issuetype", vec![Prop::Str("Test".into())]),
        PropertyFilter::any(
            "summary",
            vec![
                Prop::Str("StructuredStreaming".into()),
                Prop::Str("Query".into()),
            ],
        ),
        PropertyFilter::any(
            "priority",
            vec![
                Prop::Str("Major".into()),
                Prop::Str("Critical".into()),
                Prop::Str("Trivial".into()),
            ],
        ),
        PropertyFilter::any(
            "description",
            vec![
                Prop::Str("Transformers".into()),
                Prop::Str("SESSION".into()),
            ],
        ),
        PropertyFilter::any("duration", vec![Prop::I64(1i64)]),
        PropertyFilter::any(
            "Fix Version",
            vec![
                Prop::Str("2.3.0".into()),
                Prop::Str("4.0.0".into()),
                Prop::Str("3.5.0".into()),
            ],
        ),
        PropertyFilter::any(
            "resolution",
            vec![Prop::Str("Fixed".into()), Prop::Str("Duplicate".into())],
        ),
        PropertyFilter::any(
            "status",
            vec![Prop::Str("Resolved".into()), Prop::Str("Reopened".into())],
        ),
    ]
}

fn get_property_filters_not_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::not_any("issuetype", vec![Prop::Str("Test".into())]),
        PropertyFilter::not_any(
            "summary",
            vec![
                Prop::Str("StructuredStreaming".into()),
                Prop::Str("Query".into()),
            ],
        ),
        PropertyFilter::not_any(
            "priority",
            vec![
                Prop::Str("Major".into()),
                Prop::Str("Critical".into()),
                Prop::Str("Trivial".into()),
            ],
        ),
        PropertyFilter::not_any(
            "description",
            vec![
                Prop::Str("Transformers".into()),
                Prop::Str("SESSION".into()),
            ],
        ),
        PropertyFilter::not_any("duration", vec![Prop::I64(1i64)]),
        PropertyFilter::not_any(
            "Fix Version",
            vec![
                Prop::Str("2.3.0".into()),
                Prop::Str("4.0.0".into()),
                Prop::Str("3.5.0".into()),
            ],
        ),
        PropertyFilter::not_any(
            "resolution",
            vec![Prop::Str("Fixed".into()), Prop::Str("Duplicate".into())],
        ),
        PropertyFilter::not_any(
            "status",
            vec![Prop::Str("Resolved".into()), Prop::Str("Reopened".into())],
        ),
    ]
}

fn bench_search_nodes_by_name(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_node_names();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_name", |b| {
        b.iter(|| {
            let random_name = node_names.choose(&mut rng).unwrap();
            let random_filter = CompositeNodeFilter::Node(Filter::eq("node_name", *random_name));
            graph.search_nodes(&random_filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_property_eq(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_eq", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            let random_filter = CompositeNodeFilter::Property(random_filter.clone());
            graph.search_nodes(&random_filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_property_ne(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_ne();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_ne", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            let random_filter = CompositeNodeFilter::Property(random_filter.clone());
            graph.search_nodes(&random_filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_property_in(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_in();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_in", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            let random_filter = CompositeNodeFilter::Property(random_filter.clone());
            graph.search_nodes(&random_filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_property_not_in(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_not_in();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_not_in", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            let random_filter = CompositeNodeFilter::Property(random_filter.clone());
            graph.search_nodes(&random_filter, 5, 0).unwrap();
        })
    });
}

fn bench_search_nodes_by_composite_property_filter_and(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_composite_property_filter_and", |b| {
        b.iter(|| {
            let mut chosen_filters = property_filters
                .choose_multiple(&mut rng, 2)
                .cloned()
                .collect::<Vec<_>>();

            if chosen_filters.len() == 2 {
                let random_filter1 = chosen_filters.pop().unwrap();
                let random_filter2 = chosen_filters.pop().unwrap();
                let filter = CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Property(random_filter1),
                    CompositeNodeFilter::Property(random_filter2),
                ]);
                graph.search_nodes(&filter, 5, 0).unwrap();
            }
        })
    });
}

fn bench_search_nodes_by_composite_property_filter_or(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_composite_property_filter_or", |b| {
        b.iter(|| {
            let mut chosen_filters = property_filters
                .choose_multiple(&mut rng, 2)
                .cloned()
                .collect::<Vec<_>>();

            if chosen_filters.len() == 2 {
                let random_filter1 = chosen_filters.pop().unwrap();
                let random_filter2 = chosen_filters.pop().unwrap();
                let filter = CompositeNodeFilter::Or(vec![
                    CompositeNodeFilter::Property(random_filter1),
                    CompositeNodeFilter::Property(random_filter2),
                ]);
                graph.search_nodes(&filter, 5, 0).unwrap();
            }
        })
    });
}

fn bench_search_nodes_count(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_count", |b| {
        let random_filter = property_filters.choose(&mut rng).unwrap();
        let random_filter = CompositeNodeFilter::Property(random_filter.clone());
        b.iter(|| graph.search_nodes_count(&random_filter).unwrap())
    });
}

fn bench_search_edges_by_src_dst(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_node_names();
    let mut rng = thread_rng();

    c.bench_function("search_edges_by_src_dst", |b| {
        b.iter(|| {
            let mut chosen_names = node_names
                .choose_multiple(&mut rng, 2)
                .cloned()
                .collect::<Vec<_>>();

            if chosen_names.len() == 2 {
                let random_src_name = chosen_names.pop().unwrap();
                let random_dst_name = chosen_names.pop().unwrap();
                let random_filter = CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Edge(Filter::eq("from", random_src_name)),
                    CompositeEdgeFilter::Edge(Filter::eq("to", random_dst_name)),
                ]);
                graph.search_edges(&random_filter, 5, 0).unwrap();
            }
        })
    });
}

fn bench_search_edges_by_property(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_edges_by_property", |b| {
        let random_filter = property_filters.choose(&mut rng).unwrap();
        let random_filter = CompositeEdgeFilter::Property(random_filter.clone());
        b.iter(|| graph.search_edges(&random_filter, 5, 0).unwrap())
    });
}

fn bench_search_edges_count(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("search_edges_count", |b| {
        let random_filter = property_filters.choose(&mut rng).unwrap();
        let random_filter = CompositeEdgeFilter::Property(random_filter.clone());
        b.iter(|| graph.search_edges_count(&random_filter).unwrap())
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

fn bench_search_nodes_by_property_eq_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let property_filters = get_property_filters_eq();

    c.bench_function("search_nodes_by_property_eq_raph", |b| {
        let random_filter = property_filters.choose(&mut rng).unwrap();
        b.iter(|| {
            graph
                .filter_nodes(random_filter.clone())
                .unwrap()
                .nodes()
                .into_iter()
                .take(5)
                .collect::<Vec<_>>()
        })
    });
}

fn bench_search_nodes_by_property_ne_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_ne();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_ne_raph", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            graph
                .filter_nodes(random_filter.clone())
                .unwrap()
                .nodes()
                .into_iter()
                .take(5)
                .collect::<Vec<_>>()
        })
    });
}

fn bench_search_nodes_by_property_in_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_in();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_in_raph", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            graph
                .filter_nodes(random_filter.clone())
                .unwrap()
                .nodes()
                .into_iter()
                .take(5)
                .collect::<Vec<_>>()
        })
    });
}

fn bench_search_nodes_by_property_not_in_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_property_filters_not_in();
    let mut rng = thread_rng();

    c.bench_function("search_nodes_by_property_not_in_raph", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            graph
                .filter_nodes(random_filter.clone())
                .unwrap()
                .nodes()
                .into_iter()
                .take(5)
                .collect::<Vec<_>>()
        })
    });
}

fn bench_search_nodes_count_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let property_filters = get_property_filters_eq();

    c.bench_function("search_nodes_count_raph", |b| {
        let random_filter = property_filters.choose(&mut rng).unwrap();
        b.iter(|| graph.filter_nodes(random_filter.clone()).iter().count())
    });
}

fn bench_search_edges_by_src_dst_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_node_names();
    let mut rng = thread_rng();

    c.bench_function("search_edges_by_src_dst_raph", |b| {
        b.iter(|| {
            let random_src_name = *node_names.choose(&mut rng).unwrap();
            let random_dst_name = *node_names.choose(&mut rng).unwrap();
            println!("src = {}, dst = {}", random_src_name, random_dst_name);
            graph.edge(random_src_name, random_dst_name).unwrap()
        })
    });
}

fn bench_search_edges_by_property_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let property_filters = get_property_filters_eq();

    c.bench_function("search_edges_by_property_raph", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            graph
                .filter_edges(random_filter.clone())
                .unwrap()
                .edges()
                .into_iter()
                .take(5)
                .collect::<Vec<_>>()
        })
    });
}

fn bench_search_edges_count_raph(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let property_filters = get_property_filters_eq();

    c.bench_function("search_edges_count_raph", |b| {
        b.iter(|| {
            let random_filter = property_filters.choose(&mut rng).unwrap();
            graph.filter_edges(random_filter.clone()).iter().count()
        })
    });
}

criterion_group!(
    search_benches,
    bench_search_nodes_by_name,
    bench_search_nodes_by_property_eq,
    bench_search_nodes_by_property_ne,
    bench_search_nodes_by_property_in,
    bench_search_nodes_by_property_not_in,
    bench_search_nodes_by_composite_property_filter_and,
    bench_search_nodes_by_composite_property_filter_or,
    bench_search_nodes_count,
    bench_search_edges_by_src_dst,
    // bench_search_edges_by_property,
    // bench_search_edges_count
);

criterion_group!(
    search_benches_raph,
    bench_search_nodes_by_name_raph,
    bench_search_nodes_by_property_eq_raph,
    bench_search_nodes_by_property_ne_raph,
    bench_search_nodes_by_property_in_raph,
    bench_search_nodes_by_property_not_in_raph,
    bench_search_nodes_count_raph,
    // bench_search_edges_by_src_dst_raph,
    // bench_search_edges_by_property_raph,
    // bench_search_edges_count_raph
);

criterion_main!(search_benches, search_benches_raph);
