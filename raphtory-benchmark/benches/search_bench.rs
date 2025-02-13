use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use once_cell::sync::Lazy;
use rand::{
    seq::{IteratorRandom, SliceRandom},
    thread_rng, Rng,
};
use raphtory::{
    core::Prop,
    db::{
        api::{
            properties::internal::{
                ConstPropertiesOps, TemporalPropertiesOps, TemporalPropertyViewOps,
            },
            view::{
                internal::{CoreGraphOps, InternalIndexSearch},
                SearchableGraphOps,
            },
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::property_filter::{
                CompositeEdgeFilter, CompositeNodeFilter, Filter, FilterOperator, FilterOperator::*,
            },
        },
    },
    prelude::{
        EdgePropertyFilterOps, EdgeViewOps, Graph, GraphViewOps, NodePropertyFilterOps,
        NodeViewOps, PropUnwrap, PropertyFilter, StableDecode,
    },
};
use raphtory_api::core::PropType;
use rayon::prelude::*;
use std::{iter, sync::Arc, time::Instant};

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

fn get_random_node_names(graph: &Graph) -> Vec<String> {
    let mut rng = thread_rng();
    iter::repeat_with(move || graph.nodes().into_iter().choose(&mut rng))
        .filter_map(|opt| opt.map(|n| n.name().to_string()))
        .take(100)
        .collect()
}

fn get_random_edges_by_src_dst_names(graph: &Graph) -> Vec<(String, String)> {
    let mut rng = thread_rng();
    iter::repeat_with(move || graph.edges().into_iter().choose(&mut rng))
        .filter_map(|opt| opt.map(|e| (e.src().name().to_string(), e.dst().name().to_string())))
        .take(100)
        .collect()
}

fn get_node_types(graph: &Graph) -> Vec<String> {
    graph
        .node_meta()
        .get_all_node_types()
        .into_iter()
        .map(|nt| nt.to_string())
        .collect::<Vec<_>>()
}

fn is_numeric_type(dtype: &PropType) -> bool {
    matches!(
        dtype,
        PropType::U8
            | PropType::U16
            | PropType::U32
            | PropType::U64
            | PropType::I32
            | PropType::I64
            | PropType::F32
            | PropType::F64
    )
}

fn get_node_const_property_names(
    node: &NodeView<Graph>,
    only_numeric: bool,
) -> Vec<(String, usize)> {
    let const_props = node
        .const_prop_keys()
        .filter_map(|k| {
            let prop_name = k.to_string();
            let prop_id = node.get_const_prop_id(&prop_name)?;
            let prop_value = node.get_const_prop(prop_id)?;
            let dtype = prop_value.dtype();
            if only_numeric {
                if is_numeric_type(&dtype) {
                    Some((prop_name, prop_id))
                } else {
                    None
                }
            } else {
                Some((prop_name, prop_id))
            }
        })
        .collect::<Vec<_>>();

    const_props
}

fn get_node_temporal_property_names(
    node: &NodeView<Graph>,
    only_numeric: bool,
) -> Vec<(String, usize)> {
    let temporal_props = node
        .temporal_prop_keys()
        .filter_map(|k| {
            let prop_name = k.to_string();
            let prop_id = node.get_temporal_prop_id(&prop_name)?;
            let prop_value = node.temporal_value(prop_id)?;
            let dtype = prop_value.dtype();
            if only_numeric {
                if is_numeric_type(&dtype) {
                    Some((prop_name, prop_id))
                } else {
                    None
                }
            } else {
                Some((prop_name, prop_id))
            }
        })
        .collect::<Vec<_>>();

    temporal_props
}

fn get_edge_const_property_names(
    edge: &EdgeView<Graph>,
    only_numeric: bool,
) -> Vec<(String, usize)> {
    let const_props = edge
        .const_prop_keys()
        .filter_map(|k| {
            let prop_name = k.to_string();
            let prop_id = edge.get_const_prop_id(&prop_name)?;
            let prop_value = edge.get_const_prop(prop_id)?;
            let dtype = prop_value.dtype();
            if only_numeric {
                if is_numeric_type(&dtype) {
                    Some((prop_name, prop_id))
                } else {
                    None
                }
            } else {
                Some((prop_name, prop_id))
            }
        })
        .collect::<Vec<_>>();

    const_props
}

fn get_edge_temporal_property_names(
    edge: &EdgeView<Graph>,
    only_numeric: bool,
) -> Vec<(String, usize)> {
    let temporal_props = edge
        .temporal_prop_keys()
        .filter_map(|k| {
            let prop_name = k.to_string();
            let prop_id = edge.get_temporal_prop_id(&prop_name)?;
            let prop_value = edge.temporal_value(prop_id)?;
            let dtype = prop_value.dtype();
            if only_numeric {
                if is_numeric_type(&dtype) {
                    Some((prop_name, prop_id))
                } else {
                    None
                }
            } else {
                Some((prop_name, prop_id))
            }
        })
        .collect::<Vec<_>>();

    temporal_props
}

fn convert_to_property_filter(
    prop_name: &str,
    prop_value: Prop,
    filter_op: FilterOperator,
    sampled_values: Option<Vec<Prop>>,
) -> Option<PropertyFilter> {
    let mut rng = thread_rng();

    match prop_value.dtype() {
        // String properties support tokenized matches for eq and ne
        PropType::Str => {
            if let Some(full_str) = prop_value.into_str() {
                let tokens: Vec<&str> = full_str.split_whitespace().collect();
                if tokens.len() > 1 && rng.gen_bool(0.3) {
                    // 30% chance to use a random substring
                    let start = rng.gen_range(0..tokens.len());
                    let end = rng.gen_range(start..tokens.len());
                    let sub_str = tokens[start..=end].join(" ");

                    match filter_op {
                        Eq => Some(PropertyFilter::eq(prop_name, sub_str)),
                        Ne => Some(PropertyFilter::ne(prop_name, sub_str)),
                        In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
                        NotIn => {
                            sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals))
                        }
                        _ => None, // No numeric comparison for strings
                    }
                } else {
                    match filter_op {
                        Eq => Some(PropertyFilter::eq(prop_name, full_str)),
                        Ne => Some(PropertyFilter::ne(prop_name, full_str)),
                        In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
                        NotIn => {
                            sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals))
                        }
                        _ => None, // No numeric comparison for strings
                    }
                }
            } else {
                None
            }
        }

        // Numeric properties support all comparison operators
        PropType::U64 => prop_value.into_u64().and_then(|v| match filter_op {
            Eq => Some(PropertyFilter::eq(prop_name, v)),
            Ne => Some(PropertyFilter::ne(prop_name, v)),
            Lt => Some(PropertyFilter::lt(prop_name, v)),
            Le => Some(PropertyFilter::le(prop_name, v)),
            Gt => Some(PropertyFilter::gt(prop_name, v)),
            Ge => Some(PropertyFilter::ge(prop_name, v)),
            In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
            NotIn => sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals)),
            _ => return None,
        }),
        PropType::I64 => prop_value.into_i64().and_then(|v| match filter_op {
            Eq => Some(PropertyFilter::eq(prop_name, v)),
            Ne => Some(PropertyFilter::ne(prop_name, v)),
            Lt => Some(PropertyFilter::lt(prop_name, v)),
            Le => Some(PropertyFilter::le(prop_name, v)),
            Gt => Some(PropertyFilter::gt(prop_name, v)),
            Ge => Some(PropertyFilter::ge(prop_name, v)),
            In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
            NotIn => sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals)),
            _ => return None,
        }),
        PropType::F64 => prop_value.into_f64().and_then(|v| match filter_op {
            Eq => Some(PropertyFilter::eq(prop_name, v)),
            Ne => Some(PropertyFilter::ne(prop_name, v)),
            Lt => Some(PropertyFilter::lt(prop_name, v)),
            Le => Some(PropertyFilter::le(prop_name, v)),
            Gt => Some(PropertyFilter::gt(prop_name, v)),
            Ge => Some(PropertyFilter::ge(prop_name, v)),
            In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
            NotIn => sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals)),
            _ => return None,
        }),
        PropType::Bool => prop_value.into_bool().and_then(|v| match filter_op {
            Eq => Some(PropertyFilter::eq(prop_name, v)),
            Ne => Some(PropertyFilter::ne(prop_name, v)),
            In => sampled_values.map(|vals| PropertyFilter::any(prop_name, vals)),
            NotIn => sampled_values.map(|vals| PropertyFilter::not_any(prop_name, vals)),
            _ => return None,
        }),

        _ => None, // Skip unsupported types
    }
}

// Get list of properties from multiple random nodes for IN, NOT_IN filters
fn get_node_property_samples(graph: &Graph, prop_id: &usize, is_const: bool) -> Vec<Prop> {
    let mut rng = thread_rng();
    let node_names = get_random_node_names(graph);
    let mut samples = Vec::new();

    for node_name in node_names.iter() {
        if let Some(node) = graph.node(node_name) {
            let prop_value = if is_const {
                node.get_const_prop(*prop_id)
            } else {
                node.temporal_value(*prop_id)
            };

            if let Some(prop_value) = prop_value {
                samples.push(prop_value);
            }

            if samples.len() >= rng.gen_range(3..=5) {
                break;
            }
        }
    }

    samples
}

// Randomly pick one of the const or temporal properties per node
fn pick_node_property_filter(
    graph: &Graph,
    node: &NodeView<Graph, Graph>,
    props: &[(String, usize)],
    is_const: bool,
    filter_op: FilterOperator,
) -> Option<PropertyFilter> {
    let mut rng = thread_rng();
    if let Some((prop_name, prop_id)) = props.choose(&mut rng) {
        let prop_value = if is_const {
            node.get_const_prop(*prop_id)
        } else {
            node.temporal_value(*prop_id)
        }?;

        let sampled_values = match filter_op {
            In | NotIn => Some(get_node_property_samples(graph, prop_id, is_const)),
            _ => None,
        };

        convert_to_property_filter(prop_name, prop_value, filter_op, sampled_values)
    } else {
        None
    }
}

fn get_random_node_property_filters(
    graph: &Graph,
    filter_op: FilterOperator,
) -> Vec<PropertyFilter> {
    let mut rng = thread_rng();
    let node_names = get_random_node_names(graph);

    let mut filters = Vec::new();

    for node_name in node_names {
        if let Some(node) = graph.node(&node_name) {
            let mut chosen_filter = None;

            let (const_props, temporal_props) = if filter_op.is_strictly_numeric_operation() {
                (
                    get_node_const_property_names(&node, true),
                    get_node_temporal_property_names(&node, true),
                )
            } else {
                (
                    get_node_const_property_names(&node, false),
                    get_node_temporal_property_names(&node, false),
                )
            };

            // First try randomly selecting from both categories i.e., const or temporal properties.
            // Fallback to other property list if one is empty i.e., if const properties are empty
            // fallback to temporal properties and vice versa. This ensures, we always have as many
            // property filters as there are nodes.
            let choice = rng.gen_bool(0.5);
            if choice {
                chosen_filter =
                    pick_node_property_filter(graph, &node, &const_props, true, filter_op);
                if chosen_filter.is_none() {
                    chosen_filter =
                        pick_node_property_filter(graph, &node, &temporal_props, false, filter_op);
                }
            } else {
                chosen_filter =
                    pick_node_property_filter(graph, &node, &temporal_props, false, filter_op);
                if chosen_filter.is_none() {
                    chosen_filter =
                        pick_node_property_filter(graph, &node, &const_props, true, filter_op);
                }
            }

            // Only push if a filter is found
            if let Some(filter) = chosen_filter {
                filters.push(filter);
            }
        }
    }

    filters
}

// Get list of properties from multiple random edges for IN, NOT_IN filters
fn get_edge_property_samples(graph: &Graph, prop_id: &usize, is_const: bool) -> Vec<Prop> {
    let mut rng = thread_rng();
    let edges = get_random_edges_by_src_dst_names(graph);
    let mut samples = Vec::new();

    for (src, dst) in edges.iter() {
        if let Some(edge) = graph.edge(src, dst) {
            let prop_value = if is_const {
                edge.get_const_prop(*prop_id)
            } else {
                edge.temporal_value(*prop_id)
            };

            if let Some(prop_value) = prop_value {
                samples.push(prop_value);
            }

            if samples.len() >= rng.gen_range(3..=5) {
                break;
            }
        }
    }

    samples
}

// Randomly pick one of the const or temporal properties per edge
fn pick_edge_property_filter(
    graph: &Graph,
    edge: &EdgeView<Graph, Graph>,
    props: &[(String, usize)],
    is_const: bool,
    filter_op: FilterOperator,
) -> Option<PropertyFilter> {
    let mut rng = thread_rng();

    if let Some((prop_name, prop_id)) = props.choose(&mut rng) {
        let prop_value = if is_const {
            edge.get_const_prop(*prop_id)
        } else {
            edge.temporal_value(*prop_id)
        }?;

        let sampled_values = match filter_op {
            In | NotIn => Some(get_edge_property_samples(graph, prop_id, is_const)),
            _ => None,
        };

        convert_to_property_filter(prop_name, prop_value, filter_op, sampled_values)
    } else {
        None
    }
}

fn get_random_edge_property_filters(
    graph: &Graph,
    filter_op: FilterOperator,
) -> Vec<PropertyFilter> {
    let mut rng = thread_rng();
    let edges = get_random_edges_by_src_dst_names(graph);

    let mut filters = Vec::new();

    for (src, dst) in edges {
        if let Some(edge) = graph.edge(&src, &dst) {
            let mut chosen_filter = None;

            let (const_props, temporal_props) = if filter_op.is_strictly_numeric_operation() {
                (
                    get_edge_const_property_names(&edge, true),
                    get_edge_temporal_property_names(&edge, true),
                )
            } else {
                (
                    get_edge_const_property_names(&edge, false),
                    get_edge_temporal_property_names(&edge, false),
                )
            };

            // First try randomly selecting from both categories i.e., const or temporal properties.
            // Fallback to other property list if one is empty i.e., if const properties are empty
            // fallback to temporal properties and vice versa. This ensures, we always have as many
            // property filters as there are edges.
            let choice = rng.gen_bool(0.5);
            if choice {
                chosen_filter =
                    pick_edge_property_filter(graph, &edge, &const_props, true, filter_op);
                if chosen_filter.is_none() {
                    chosen_filter =
                        pick_edge_property_filter(graph, &edge, &temporal_props, false, filter_op);
                }
            } else {
                chosen_filter =
                    pick_edge_property_filter(graph, &edge, &temporal_props, false, filter_op);
                if chosen_filter.is_none() {
                    chosen_filter =
                        pick_edge_property_filter(graph, &edge, &const_props, true, filter_op);
                }
            }

            // Only push if a filter is found
            if let Some(filter) = chosen_filter {
                filters.push(filter);
            }
        } else {
            println!("edge {} -> {} not found", src, dst);
        }
    }

    filters
}

fn bench_search_nodes_by_property_filter<F>(
    c: &mut Criterion,
    bench_name: &str,
    graph: &Graph,
    filter_op: FilterOperator,
) {
    let property_filters = get_random_node_property_filters(graph, filter_op);

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = property_filters.iter().cycle();
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
        let mut iter = property_filters.iter().cycle();
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
    ($fn_name:ident) => {
        fn $fn_name(c: &mut Criterion) {
            let graph = setup_graph();
            let filter_op: FilterOperator =
                match stringify!($fn_name).trim_start_matches("bench_search_nodes_by_property_") {
                    "eq" => FilterOperator::Eq,
                    "ne" => FilterOperator::Ne,
                    "le" => FilterOperator::Le,
                    "lt" => FilterOperator::Lt,
                    "ge" => FilterOperator::Ge,
                    "gt" => FilterOperator::Gt,
                    "in" => FilterOperator::In,
                    "not_in" => FilterOperator::NotIn,
                    _ => panic!("Unknown filter type in function name"),
                };
            bench_search_nodes_by_property_filter::<FilterOperator>(
                c,
                stringify!($fn_name),
                &graph,
                filter_op,
            );
        }
    };
}

fn bench_search_edges_by_property_filter<F>(
    c: &mut Criterion,
    bench_name: &str,
    graph: &Graph,
    filter_op: FilterOperator,
) {
    let property_filters = get_random_edge_property_filters(graph, filter_op);

    let mut group = c.benchmark_group(bench_name);

    group.bench_function("search_api", |b| {
        let mut iter = property_filters.iter().cycle();
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
        let mut iter = property_filters.iter().cycle();
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
    ($fn_name:ident) => {
        fn $fn_name(c: &mut Criterion) {
            let graph = setup_graph();
            let filter_op: FilterOperator =
                match stringify!($fn_name).trim_start_matches("bench_search_edges_by_property_") {
                    "eq" => FilterOperator::Eq,
                    "ne" => FilterOperator::Ne,
                    "le" => FilterOperator::Le,
                    "lt" => FilterOperator::Lt,
                    "ge" => FilterOperator::Ge,
                    "gt" => FilterOperator::Gt,
                    "in" => FilterOperator::In,
                    "not_in" => FilterOperator::NotIn,
                    _ => panic!("Unknown filter type in function name"),
                };
            bench_search_edges_by_property_filter::<FilterOperator>(
                c,
                stringify!($fn_name),
                &graph,
                filter_op,
            );
        }
    };
}

fn bench_search_nodes_by_name(c: &mut Criterion) {
    let graph = setup_graph();
    let node_names = get_random_node_names(&graph);

    let mut group = c.benchmark_group("bench_search_nodes_by_name");

    group.bench_function("search_api", |b| {
        b.iter_batched(
            || {
                let mut iter = node_names.iter().cycle();
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
                let mut iter = node_names.iter().cycle();
                iter.next().unwrap().clone()
            },
            |random_name| graph.node(random_name),
            BatchSize::SmallInput,
        )
    });
}

fn bench_search_nodes_by_node_type(c: &mut Criterion) {
    let graph = setup_graph();
    let mut rng = thread_rng();
    let node_types = get_node_types(&graph);
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
            |random_node_type| graph.nodes().type_filter(&[random_node_type]),
            BatchSize::SmallInput,
        )
    });
}

bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_eq);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_ne);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_le);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_lt);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_ge);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_gt);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_in);
bench_search_nodes_by_property_filter!(bench_search_nodes_by_property_not_in);

fn bench_search_nodes_by_composite_property_filter_and(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_random_node_property_filters(&graph, Eq);
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
    let property_filters = get_random_node_property_filters(&graph, Eq);
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
    let sample_inputs = get_random_edges_by_src_dst_names(&graph);

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

bench_search_edges_by_property_filter!(bench_search_edges_by_property_eq);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_ne);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_le);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_lt);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_ge);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_gt);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_in);
bench_search_edges_by_property_filter!(bench_search_edges_by_property_not_in);

fn bench_search_edges_by_composite_property_filter_and(c: &mut Criterion) {
    let graph = setup_graph();
    let property_filters = get_random_edge_property_filters(&graph, Eq);
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
    let property_filters = get_random_edge_property_filters(&graph, Eq);
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
