use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use once_cell::sync::Lazy;
use rand::{seq::SliceRandom, thread_rng};
use raphtory::{
    core::{ Prop},
    db::{
        api::view::{internal::InternalIndexSearch, SearchableGraphOps},
        graph::views::{
            property_filter::{CompositeEdgeFilter, CompositeNodeFilter, Filter},
        },
    },
    prelude::{
        EdgePropertyFilterOps, Graph, GraphViewOps, NodePropertyFilterOps,
        NodeViewOps, PropertyFilter, StableDecode,
    },
};
use rayon::prelude::*;
use std::{sync::Arc, time::Instant};

static GRAPH: Lazy<Arc<Graph>> = Lazy::new(|| {
    let data_dir = "/tmp/graphs/raph_social/rf0.1";
    // let data_dir = "/tmp/graphs/raph_social/rf1.0";
    let graph = Graph::decode(data_dir).unwrap();

    println!("Nodes count = {}", graph.count_nodes());
    println!("Edges count = {}", graph.count_edges());

    let start = Instant::now();
    let _ = graph.searcher().unwrap();
    let duration = start.elapsed();
    println!("Time taken to initialize graph and indexes: {:?}", duration);

    Arc::new(graph)
});

fn setup_graph() -> Arc<Graph> {
    Arc::clone(&GRAPH)
}

fn get_node_names() -> Vec<&'static str> {
    vec![
        "forum_362",
        "post_13707",
        "comment_54312",
        "comment_67046",
        "person_2431",
        "person_238",
        "person_666",
        "comment_5905",
        "comment_13180",
        "post_9227",
        "post_3390",
        "post_31007",
        "person_243",
        "comment_51777",
        "forum_76",
        "comment_29138", // nodes which 1 million comments
        "comment_39131",
        "comment_64817",
        "forum_15",
        "forum_418",
        "forum_328",
        "forum_178",
        "forum_75",
        "post_3081",
    ]
}

fn get_node_types() -> Vec<&'static str> {
    vec![
        "post",
        "comment",
        "forum",
        "person",
    ]
}

fn get_node_property_filters_eq() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("first_name", "Calista"),
        PropertyFilter::eq("last_name", "Williamson"),
        PropertyFilter::eq("gender", "female"),
        PropertyFilter::eq("title", "deleniti"),
        // PropertyFilter::eq("creator_id", "Resolved"),
        PropertyFilter::eq("location_ip", "178.87.115.183"),
        PropertyFilter::eq("browser_used", "Edge"),
        PropertyFilter::eq("content", "voluptatibus"),
        PropertyFilter::eq("length", 100u64),
    ]
}

fn get_node_property_filters_ne() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("first_name", "Calista"),
        PropertyFilter::eq("last_name", "Williamson"),
        PropertyFilter::eq("gender", "female"),
        PropertyFilter::eq("title", "deleniti"),
        // PropertyFilter::eq("creator_id", "Resolved"),
        PropertyFilter::eq("location_ip", "178.87.115.183"),
        PropertyFilter::eq("browser_used", "Edge"),
        PropertyFilter::eq("content", "voluptatibus"),
        PropertyFilter::eq("length", 100u64),
    ]
}

fn get_node_property_filters_le() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::le("length", 1000u64),
        PropertyFilter::le("length", 10u64),
        PropertyFilter::le("length", 80u64),
        PropertyFilter::le("length", 1u64),
        PropertyFilter::le("length", 50u64),
        PropertyFilter::le("length", 200u64),
        PropertyFilter::le("length", 300u64),
        PropertyFilter::le("length", 40u64),
        PropertyFilter::le("length", 2000u64),
        PropertyFilter::le("length", 5000u64),
    ]
}

fn get_node_property_filters_lt() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::lt("length", 1000u64),
        PropertyFilter::lt("length", 10u64),
        PropertyFilter::lt("length", 80u64),
        PropertyFilter::lt("length", 1u64),
        PropertyFilter::lt("length", 50u64),
        PropertyFilter::lt("length", 200u64),
        PropertyFilter::lt("length", 300u64),
        PropertyFilter::lt("length", 40u64),
        PropertyFilter::lt("length", 2000u64),
        PropertyFilter::lt("length", 5000u64),
    ]
}

fn get_node_property_filters_ge() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::ge("length", 1000u64),
        PropertyFilter::ge("length", 10u64),
        PropertyFilter::ge("length", 80u64),
        PropertyFilter::ge("length", 1u64),
        PropertyFilter::ge("length", 50u64),
        PropertyFilter::ge("length", 200u64),
        PropertyFilter::ge("length", 300u64),
        PropertyFilter::ge("length", 40u64),
        PropertyFilter::ge("length", 2000u64),
        PropertyFilter::ge("length", 5000u64),
    ]
}

fn get_node_property_filters_gt() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::gt("length", 1000u64),
        PropertyFilter::gt("length", 10u64),
        PropertyFilter::gt("length", 80u64),
        PropertyFilter::gt("length", 1u64),
        PropertyFilter::gt("length", 50u64),
        PropertyFilter::gt("length", 200u64),
        PropertyFilter::gt("length", 300u64),
        PropertyFilter::gt("length", 40u64),
        PropertyFilter::gt("length", 2000u64),
        PropertyFilter::gt("length", 5000u64),
    ]
}

fn get_node_property_filters_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::any(
            "first_name",
            vec![Prop::Str("Rowland".into()), Prop::Str("Heath".into())],
        ),
        PropertyFilter::any(
            "last_name",
            vec![Prop::Str("Buckridge".into()), Prop::Str("Pollich".into())],
        ),
        PropertyFilter::any("gender", vec![Prop::Str("male".into())]),
        PropertyFilter::any(
            "title",
            vec![Prop::Str("optio".into()), Prop::Str("dolorem".into())],
        ),
        PropertyFilter::any("location_ip", vec![Prop::I64(1i64)]),
        PropertyFilter::any(
            "browser_used",
            vec![Prop::Str("Firefox".into()), Prop::Str("Chrome".into())],
        ),
        PropertyFilter::any(
            "content",
            vec![Prop::Str("sit".into()), Prop::Str("qui".into())],
        ),
        PropertyFilter::any("length", vec![Prop::U64(100), Prop::U64(420)]),
    ]
}

fn get_node_property_filters_not_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::any(
            "first_name",
            vec![Prop::Str("Rowland".into()), Prop::Str("Heath".into())],
        ),
        PropertyFilter::any(
            "last_name",
            vec![Prop::Str("Buckridge".into()), Prop::Str("Pollich".into())],
        ),
        PropertyFilter::any("gender", vec![Prop::Str("male".into())]),
        PropertyFilter::any(
            "title",
            vec![Prop::Str("optio".into()), Prop::Str("dolorem".into())],
        ),
        PropertyFilter::any("location_ip", vec![Prop::I64(1i64)]),
        PropertyFilter::any(
            "browser_used",
            vec![Prop::Str("Firefox".into()), Prop::Str("Chrome".into())],
        ),
        PropertyFilter::any(
            "content",
            vec![Prop::Str("sit".into()), Prop::Str("qui".into())],
        ),
        PropertyFilter::any("length", vec![Prop::U64(100), Prop::U64(420)]),
    ]
}

fn get_edge_property_filters_eq() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("is_moderator", false),
        PropertyFilter::eq("activity_score", 2.0695148555643916f64),
        PropertyFilter::eq("is_featured", true),
        PropertyFilter::eq("likes_count", 254u64),
        PropertyFilter::eq("comments_count", 70u64),
        PropertyFilter::eq("is_edited", false),
        PropertyFilter::eq("upvotes", 70u64),
        PropertyFilter::eq("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_ne() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::eq("is_moderator", false),
        PropertyFilter::eq("activity_score", 2.0695148555643916f64),
        PropertyFilter::eq("is_featured", true),
        PropertyFilter::eq("likes_count", 254u64),
        PropertyFilter::eq("comments_count", 70u64),
        PropertyFilter::eq("is_edited", false),
        PropertyFilter::eq("upvotes", 70u64),
        PropertyFilter::eq("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_le() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::le("activity_score", 2.0695148555643916f64),
        PropertyFilter::le("likes_count", 254u64),
        PropertyFilter::le("comments_count", 70u64),
        PropertyFilter::le("upvotes", 70u64),
        PropertyFilter::le("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_lt() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::lt("activity_score", 2.0695148555643916f64),
        PropertyFilter::lt("likes_count", 254u64),
        PropertyFilter::lt("comments_count", 70u64),
        PropertyFilter::lt("upvotes", 70u64),
        PropertyFilter::lt("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_ge() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::ge("activity_score", 2.0695148555643916f64),
        PropertyFilter::ge("likes_count", 254u64),
        PropertyFilter::ge("comments_count", 70u64),
        PropertyFilter::ge("upvotes", 70u64),
        PropertyFilter::ge("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_gt() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::gt("activity_score", 2.0695148555643916f64),
        PropertyFilter::gt("likes_count", 254u64),
        PropertyFilter::gt("comments_count", 70u64),
        PropertyFilter::gt("upvotes", 70u64),
        PropertyFilter::gt("reply_count", 100u64),
    ]
}

fn get_edge_property_filters_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::any("is_moderator", vec![Prop::Bool(true)]),
        PropertyFilter::any("activity_score", vec![Prop::F64(2.0695148555643916f64)]),
        PropertyFilter::any("is_featured", vec![Prop::Bool(true)]),
        PropertyFilter::any("likes_count", vec![Prop::U64(254u64)]),
        PropertyFilter::any("comments_count", vec![Prop::U64(70u64)]),
        PropertyFilter::any("is_edited", vec![Prop::Bool(false)]),
        PropertyFilter::any("upvotes", vec![Prop::U64(70u64)]),
        PropertyFilter::any("reply_count", vec![Prop::U64(100u64)]),
    ]
}

fn get_edge_property_filters_not_in() -> Vec<PropertyFilter> {
    vec![
        PropertyFilter::not_any("is_moderator", vec![Prop::Bool(true)]),
        PropertyFilter::not_any("activity_score", vec![Prop::F64(2.0695148555643916f64)]),
        PropertyFilter::not_any("is_featured", vec![Prop::Bool(true)]),
        PropertyFilter::not_any("likes_count", vec![Prop::U64(254u64)]),
        PropertyFilter::not_any("comments_count", vec![Prop::U64(70u64)]),
        PropertyFilter::not_any("is_edited", vec![Prop::Bool(false)]),
        PropertyFilter::not_any("upvotes", vec![Prop::U64(70u64)]),
        PropertyFilter::not_any("reply_count", vec![Prop::U64(100u64)]),
    ]
}

fn bench_search_nodes_by_property_filter<F>(c: &mut Criterion, bench_name: &str, filter_fn: F)
where
    F: Fn() -> Vec<PropertyFilter> + Copy,
{
    let graph = setup_graph();
    let property_filters = filter_fn();
    let mut rng = thread_rng();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| property_filters.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || {
                let random_filter = iter.next().unwrap().clone();
                CompositeNodeFilter::Property(random_filter)
            },
            |random_filter| {
                graph.search_nodes(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || iter.next().unwrap().clone(),
            |random_filter| {
                graph
                    .filter_nodes(random_filter)
                    .unwrap()
                    .nodes()
                    .into_iter()
                    .take(5)
                    .collect::<Vec<_>>()
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

macro_rules! bench_search_nodes_by_property_filter {
    ($fn_name:ident, $filter_fn:expr) => {
        fn $fn_name(c: &mut Criterion) {
            bench_search_nodes_by_property_filter(c, stringify!($fn_name), $filter_fn);
        }
    };
}

fn bench_search_nodes_by_property_filter_count<F>(c: &mut Criterion, bench_name: &str, filter_fn: F)
where
    F: Fn() -> Vec<PropertyFilter> + Copy,
{
    let graph = setup_graph();
    let property_filters = filter_fn();
    let mut rng = thread_rng();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| property_filters.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || {
                let random_filter = iter.next().unwrap().clone();
                CompositeNodeFilter::Property(random_filter)
            },
            |random_filter| graph.search_nodes_count(&random_filter),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || iter.next().unwrap().clone(),
            |random_filter| graph.filter_nodes(random_filter.clone()).iter().count(),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

macro_rules! bench_search_nodes_by_property_filter_count {
    ($fn_name:ident, $filter_fn:expr) => {
        fn $fn_name(c: &mut Criterion) {
            bench_search_nodes_by_property_filter_count(c, stringify!($fn_name), $filter_fn);
        }
    };
}

fn bench_search_edges_by_property_filter<F>(c: &mut Criterion, bench_name: &str, filter_fn: F)
where
    F: Fn() -> Vec<PropertyFilter> + Copy,
{
    let graph = setup_graph();
    let property_filters = filter_fn();
    let mut rng = thread_rng();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| property_filters.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || {
                let random_filter = iter.next().unwrap().clone();
                CompositeEdgeFilter::Property(random_filter)
            },
            |random_filter| {
                graph.search_edges(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || iter.next().unwrap().clone(),
            |random_filter| {
                graph
                    .filter_edges(random_filter)
                    .unwrap()
                    .edges()
                    .into_iter()
                    .take(5)
                    .collect::<Vec<_>>()
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

macro_rules! bench_search_edges_by_property_filter {
    ($fn_name:ident, $filter_fn:expr) => {
        fn $fn_name(c: &mut Criterion) {
            bench_search_edges_by_property_filter(c, stringify!($fn_name), $filter_fn);
        }
    };
}

fn bench_search_edges_by_property_filter_count<F>(c: &mut Criterion, bench_name: &str, filter_fn: F)
where
    F: Fn() -> Vec<PropertyFilter> + Copy,
{
    let graph = setup_graph();
    let property_filters = filter_fn();
    let mut rng = thread_rng();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| property_filters.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || {
                let random_filter = iter.next().unwrap().clone();
                CompositeEdgeFilter::Property(random_filter)
            },
            |random_filter| graph.search_edges_count(&random_filter),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        let mut iter = sample_inputs.iter().cycle();
        b.iter_batched(
            || iter.next().unwrap().clone(),
            |random_filter| graph.filter_edges(random_filter.clone()).iter().count(),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

macro_rules! bench_search_edges_by_property_filter_count {
    ($fn_name:ident, $filter_fn:expr) => {
        fn $fn_name(c: &mut Criterion) {
            bench_search_edges_by_property_filter_count(c, stringify!($fn_name), $filter_fn);
        }
    };
}

fn bench_search_nodes_by_name(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let node_names = get_node_names();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| node_names.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group("bench_search_nodes_by_name");

    group.bench_function("search_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                let random_name = iter.next().unwrap().clone();
                CompositeNodeFilter::Node(Filter::eq("node_name", random_name))
            },
            |random_filter| {
                graph.search_nodes(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                iter.next().unwrap().clone()
            },
            |random_name| graph.node(random_name)
            ,
            BatchSize::SmallInput,
        )
    });
}

fn bench_search_nodes_by_node_type(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let node_types = get_node_names();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| node_types.choose(&mut rng).unwrap().clone())
        .collect();

    let mut group = c.benchmark_group("bench_search_nodes_by_node_type");

    group.bench_function("search_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                let random_node_type = iter.next().unwrap().clone();
                CompositeNodeFilter::Node(Filter::eq("node_type", random_node_type))
            },
            |random_filter| {
                graph.search_nodes(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                iter.next().unwrap().clone()
            },
            |random_node_type| {
                graph.nodes().type_filter(&[random_node_type])
            },
            BatchSize::SmallInput,
        )
    });
}

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_eq,
    get_node_property_filters_eq
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_ne,
    get_node_property_filters_ne
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_le,
    get_node_property_filters_le
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_lt,
    get_node_property_filters_lt
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_ge,
    get_node_property_filters_ge
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_gt,
    get_node_property_filters_gt
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_in,
    get_node_property_filters_in
);

bench_search_nodes_by_property_filter!(
    bench_search_nodes_by_property_not_in,
    get_node_property_filters_not_in
);

bench_search_nodes_by_property_filter_count!(
    bench_search_nodes_count,
    get_node_property_filters_eq
);

fn bench_search_nodes_by_composite_property_filter_and(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_node_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("bench_search_nodes_by_composite_property_filter_and", |b| {
        b.iter_batched(
            || {
                let random_filter1 = property_filters.choose(&mut rng).unwrap();
                let random_filter2 = property_filters.choose(&mut rng).unwrap();
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Property(random_filter1.clone()),
                    CompositeNodeFilter::Property(random_filter2.clone()),
                ])
            },
            |random_filter| {
                graph.search_nodes(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_search_nodes_by_composite_property_filter_or(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_node_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("bench_search_nodes_by_composite_property_filter_or", |b| {
        b.iter_batched(
            || {
                let random_filter1 = property_filters.choose(&mut rng).unwrap();
                let random_filter2 = property_filters.choose(&mut rng).unwrap();
                CompositeNodeFilter::Or(vec![
                    CompositeNodeFilter::Property(random_filter1.clone()),
                    CompositeNodeFilter::Property(random_filter2.clone()),
                ])
            },
            |random_filter| {
                graph.search_nodes(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_search_edges_by_src_dst(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let node_names = get_node_names();
    let sample_inputs: Vec<_> = (0..100)
        .map(|_| {
            (
                node_names.choose(&mut rng).unwrap().clone(),
                node_names.choose(&mut rng).unwrap().clone(),
            )
        })
        .collect();

    let mut group = c.benchmark_group("bench_search_edges_by_src_dst");

    group.bench_function("search_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                let random_name = iter.next().unwrap().clone();
                let random_src_name = random_name.0;
                let random_dst_name = random_name.1;
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Edge(Filter::eq("from", random_src_name)),
                    CompositeEdgeFilter::Edge(Filter::eq("to", random_dst_name)),
                ])
            },
            |random_filter| {
                graph.search_edges(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("raph_api", |b| {
        b.iter_batched(
            || {
                let mut iter = sample_inputs.iter().cycle();
                iter.next().unwrap().clone()
            },
            |random_name| {
                let random_src_name = random_name.0;
                let random_dst_name = random_name.1;
                graph.edge(random_src_name, random_dst_name)
            },
            BatchSize::SmallInput,
        )
    });
}

bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_eq,
    get_edge_property_filters_eq
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_ne,
    get_edge_property_filters_ne
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_le,
    get_edge_property_filters_le
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_lt,
    get_edge_property_filters_lt
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_ge,
    get_edge_property_filters_ge
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_gt,
    get_edge_property_filters_gt
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_in,
    get_edge_property_filters_in
);
bench_search_edges_by_property_filter!(
    bench_search_edges_by_property_not_in,
    get_edge_property_filters_not_in
);

bench_search_edges_by_property_filter_count!(
    bench_search_edges_count,
    get_edge_property_filters_eq
);

fn bench_search_edges_by_composite_property_filter_and(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_edge_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("bench_search_edges_by_composite_property_filter_and", |b| {
        b.iter_batched(
            || {
                let random_filter1 = property_filters.choose(&mut rng).unwrap();
                let random_filter2 = property_filters.choose(&mut rng).unwrap();
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Property(random_filter1.clone()),
                    CompositeEdgeFilter::Property(random_filter2.clone()),
                ])
            },
            |random_filter| {
                graph.search_edges(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_search_edges_by_composite_property_filter_or(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_edge_property_filters_eq();
    let mut rng = thread_rng();

    c.bench_function("bench_search_edges_by_composite_property_filter_or", |b| {
        b.iter_batched(
            || {
                let random_filter1 = property_filters.choose(&mut rng).unwrap();
                let random_filter2 = property_filters.choose(&mut rng).unwrap();
                CompositeEdgeFilter::Or(vec![
                    CompositeEdgeFilter::Property(random_filter1.clone()),
                    CompositeEdgeFilter::Property(random_filter2.clone()),
                ])
            },
            |random_filter| {
                graph.search_edges(&random_filter, 5, 0).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    search_benches,
    bench_search_nodes_by_name,
    bench_search_nodes_by_node_type,
    bench_search_nodes_count,
    bench_search_nodes_by_property_eq,
    bench_search_nodes_by_property_ne,
    bench_search_nodes_by_property_le,
    bench_search_nodes_by_property_lt,
    bench_search_nodes_by_property_ge,
    bench_search_nodes_by_property_gt,
    bench_search_nodes_by_property_in,
    bench_search_nodes_by_property_not_in,
    bench_search_nodes_by_composite_property_filter_and,
    bench_search_nodes_by_composite_property_filter_or,
    bench_search_edges_by_src_dst,
    bench_search_edges_count,
    bench_search_edges_by_property_eq,
    bench_search_edges_by_property_ne,
    bench_search_edges_by_property_le,
    bench_search_edges_by_property_lt,
    bench_search_edges_by_property_ge,
    bench_search_edges_by_property_gt,
    bench_search_edges_by_property_in,
    bench_search_edges_by_property_not_in,
    bench_search_edges_by_composite_property_filter_and,
    bench_search_edges_by_composite_property_filter_or,
);

criterion_main!(search_benches);

// TODO: Add is_some, is_none, tests
