use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::Path,
};

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use either::Either::Left;
use itertools::Itertools;
use proptest::{collection, prelude::*};
use raphtory_api::core::entities::properties::{
    prop::{DECIMAL_MAX, Prop, PropType},
    tprop::TPropOps,
};
use raphtory_core::{entities::VID, storage::timeindex::TimeIndexOps};
use rayon::prelude::*;

use crate::{
    EdgeEntryOps, EdgeRefOps, EdgeSegmentOps, NodeEntryOps, NodeRefOps, NodeSegmentOps,
    error::DBV4Error, pages::GraphStore,
};

pub fn check_edges_support<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    EXT: Clone + Default + Send + Sync,
>(
    edges: Vec<(impl Into<VID>, impl Into<VID>)>,
    par_load: bool,
    check_load: bool,
    make_graph: impl FnOnce(&Path) -> GraphStore<NS, ES, EXT>,
) {
    let mut edges = edges
        .into_iter()
        .map(|(src, dst)| (src.into(), dst.into()))
        .collect::<Vec<_>>();

    let graph_dir = tempfile::tempdir().unwrap();
    let graph = make_graph(graph_dir.path());
    let mut nodes = HashSet::new();

    for (src, dst) in &edges {
        nodes.insert(*src);
        nodes.insert(*dst);
    }

    if par_load {
        edges
            .par_iter()
            .try_for_each(|(src, dst)| {
                let _ = graph.add_edge(0, *src, *dst)?;
                Ok::<_, DBV4Error>(())
            })
            .expect("Failed to add edge");
    } else {
        edges
            .iter()
            .try_for_each(|(src, dst)| {
                let _ = graph.add_edge(0, *src, *dst)?;
                Ok::<_, DBV4Error>(())
            })
            .expect("Failed to add edge");
    }

    let actual_num_nodes = graph.nodes().num_nodes();
    assert_eq!(actual_num_nodes, nodes.len());

    edges.sort_unstable();

    fn check<
        NS: NodeSegmentOps<Extension = EXT>,
        ES: EdgeSegmentOps<Extension = EXT>,
        EXT: Clone + Default,
    >(
        stage: &str,
        es: &[(VID, VID)],
        graph: &GraphStore<NS, ES, EXT>,
    ) {
        let nodes = graph.nodes();
        let edges = graph.edges();

        if !es.is_empty() {
            assert!(nodes.pages().count() > 0, "{stage}");
        }

        let mut expected_graph: HashMap<VID, (Vec<VID>, Vec<VID>)> = es
            .iter()
            .chunk_by(|(src, _)| *src)
            .into_iter()
            .map(|(src, edges)| {
                let mut out: Vec<_> = edges.map(|(_, dst)| *dst).collect();
                out.sort_unstable();
                out.dedup();
                (src, (out, vec![]))
            })
            .collect::<HashMap<_, _>>();

        let mut edges_sorted_by_dest = es.to_vec();
        edges_sorted_by_dest.sort_unstable_by_key(|(_, dst)| *dst);

        // now inbounds
        edges_sorted_by_dest
            .iter()
            .chunk_by(|(_, dst)| *dst)
            .into_iter()
            .for_each(|(dst, edges)| {
                let mut edges: Vec<_> = edges.map(|(src, _)| *src).collect();
                edges.sort_unstable();
                edges.dedup();
                let (_, inb) = expected_graph.entry(dst).or_default();
                *inb = edges;
            });

        for (n, (exp_out, exp_inb)) in expected_graph {
            let entry = nodes.node(n);

            let adj = entry.as_ref();
            let out_nbrs: Vec<_> = adj.out_nbrs_sorted().collect();
            assert_eq!(out_nbrs, exp_out, "{stage} node: {:?}", n);

            let in_nbrs: Vec<_> = adj.inb_nbrs_sorted().collect();
            assert_eq!(in_nbrs, exp_inb, "{stage} node: {:?}", n);

            for (exp_dst, eid) in adj.out_edges() {
                let (src, dst) = edges.get_edge(eid).unwrap();
                assert_eq!(src, n, "{stage}");
                assert_eq!(dst, exp_dst, "{stage}");
            }

            for (exp_src, eid) in adj.inb_edges() {
                let (src, dst) = edges.get_edge(eid).unwrap();
                assert_eq!(src, exp_src, "{stage}");
                assert_eq!(dst, n, "{stage}");
            }
        }
    }

    check("pre-drop", &edges, &graph);
    if check_load {
        drop(graph);

        let maybe_ns = GraphStore::<NS, ES, EXT>::load(graph_dir.path());
        if edges.is_empty() {
            assert!(maybe_ns.is_err());
        } else {
            match maybe_ns {
                Ok(graph) => {
                    check("post-drop", &edges, &graph);
                }
                Err(e) => {
                    panic!("Failed to load graph: {:?}", e);
                }
            }
        }
    }
}

pub fn check_graph_with_nodes_support<
    EXT: Clone + Default + Send + Sync,
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
>(
    fixture: &NodeFixture,
    check_load: bool,
    make_graph: impl FnOnce(&Path) -> GraphStore<NS, ES, EXT>,
) {
    let NodeFixture {
        temp_props,
        const_props,
    } = fixture;

    let graph_dir = tempfile::tempdir().unwrap();
    let graph = make_graph(graph_dir.path());

    for (node, t, t_props) in temp_props {
        let err = graph.add_node_props(*t, *node, t_props.clone());

        assert!(err.is_ok(), "Failed to add node: {:?}", err);
    }

    for (node, const_props) in const_props {
        let err = graph.update_node_const_props(*node, const_props.clone());

        assert!(err.is_ok(), "Failed to add node: {:?}", err);
    }

    let check_fn = |temp_props: &[(VID, i64, Vec<(String, Prop)>)],
                    const_props: &[(VID, Vec<(String, Prop)>)],
                    graph: &GraphStore<NS, ES, EXT>| {
        let mut ts_for_nodes = HashMap::new();
        for (node, t, _) in temp_props {
            ts_for_nodes.entry(*node).or_insert_with(|| vec![]).push(*t);
        }
        ts_for_nodes.iter_mut().for_each(|(_, ts)| {
            ts.sort_unstable();
        });

        for (node, ts_expected) in ts_for_nodes {
            let ne = graph.nodes().node(node);
            let node_entry = ne.as_ref();
            let actual: Vec<_> = node_entry.additions().iter_t().collect();
            assert_eq!(
                actual, ts_expected,
                "Expected node additions for node ({node:?})",
            );
        }

        let mut const_props_values = HashMap::new();
        for (node, const_props) in const_props {
            let node = *node;
            for (name, prop) in const_props {
                const_props_values
                    .entry((node, name))
                    .or_insert_with(|| HashSet::new())
                    .insert(prop.clone());
            }
        }

        for ((node, name), const_props) in const_props_values {
            let ne = graph.nodes().node(node);
            let node_entry = ne.as_ref();

            let prop_id = graph
                .node_meta()
                .const_prop_meta()
                .get_id(&name)
                .unwrap_or_else(|| panic!("Failed to get prop id for {}", name));
            let actual_props = node_entry.c_prop(prop_id);

            if !const_props.is_empty() {
                let actual_prop = actual_props
                    .unwrap_or_else(|| panic!("Failed to get prop {name} for {node:?}"));
                assert!(
                    const_props.contains(&actual_prop),
                    "failed to get const prop {name} for {node:?}, expected {:?}, got {:?}",
                    const_props,
                    actual_prop
                );
            }
        }

        let mut nod_t_prop_groups = HashMap::new();
        for (node, t, t_props) in temp_props {
            let node = *node;
            let t = *t;

            for (prop_name, prop) in t_props {
                let prop_values = nod_t_prop_groups
                    .entry((node, prop_name))
                    .or_insert_with(|| vec![]);
                prop_values.push((t, prop.clone()));
            }
        }

        nod_t_prop_groups.iter_mut().for_each(|(_, props)| {
            props.sort_unstable_by_key(|(t, _)| *t);
        });

        for ((node, prop_name), props) in nod_t_prop_groups {
            let prop_id = graph
                .node_meta()
                .temporal_prop_meta()
                .get_id(&prop_name)
                .unwrap_or_else(|| panic!("Failed to get prop id for {}", prop_name));

            let ne = graph.nodes().node(node);
            let node_entry = ne.as_ref();
            let actual_props = node_entry.t_prop(prop_id).iter_t().collect::<Vec<_>>();

            assert_eq!(
                actual_props, props,
                "Expected properties for node ({:?}) to be {:?}, but got {:?}",
                node, props, actual_props
            );
        }
    };

    check_fn(temp_props, const_props, &graph);

    if check_load {
        drop(graph);
        let graph = GraphStore::<NS, ES, EXT>::load(graph_dir.path()).unwrap();
        check_fn(temp_props, const_props, &graph);
    }
}

pub fn check_graph_with_props_support<
    EXT: Clone + Default + Send + Sync,
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
>(
    fixture: &Fixture,
    check_load: bool,
    make_graph: impl FnOnce(&Path) -> GraphStore<NS, ES, EXT>,
) {
    let Fixture { edges, const_props } = fixture;
    let graph_dir = tempfile::tempdir().unwrap();
    let graph = make_graph(graph_dir.path());

    // Add edges
    for (src, dst, t, t_props, _, _) in edges {
        let err = graph.add_edge_props(*t, *src, *dst, t_props.clone(), 0);

        assert!(err.is_ok(), "Failed to add edge: {:?}", err);
    }

    // Add const props
    for ((src, dst), const_props) in const_props {
        let eid = graph
            .nodes()
            .get_edge(*src, *dst)
            .unwrap_or_else(|| panic!("Failed to get edge ({:?}, {:?}) from graph", src, dst));
        let res = graph.update_edge_const_props(eid, const_props.clone());

        assert!(
            res.is_ok(),
            "Failed to update edge const props: {:?} {src:?} -> {dst:?}",
            res
        );
    }

    assert!(graph.edges().num_edges() > 0);

    let check_fn = |edges: &[AddEdge], graph: &GraphStore<NS, ES, EXT>| {
        let mut edge_groups = HashMap::new();
        let mut node_groups: HashMap<VID, Vec<i64>> = HashMap::new();

        // Group temporal edge props and their timestamps
        for (src, dst, t, t_props, _, _) in edges {
            let src = *src;
            let dst = *dst;
            let t = *t;

            for (prop_name, prop) in t_props {
                let prop_values = edge_groups
                    .entry((src, dst, prop_name))
                    .or_insert_with(|| vec![]);
                prop_values.push((t, prop.clone()));
            }
        }

        edge_groups.iter_mut().for_each(|(_, props)| {
            props.sort_unstable_by_key(|(t, _)| *t);
        });

        // Group node additions and their timestamps
        for (src, dst, t, _, _, _) in edges {
            let src = *src;
            let dst = *dst;
            let t = *t;

            // Include src additions
            node_groups.entry(src).or_insert_with(|| vec![]).push(t);

            // Self-edges don't have dst additions, so skip
            if src == dst {
                continue;
            }

            // Include dst additions
            node_groups.entry(dst).or_insert_with(|| vec![]).push(t);
        }

        node_groups.iter_mut().for_each(|(_, ts)| {
            ts.sort_unstable();
        });

        for ((src, dst, prop_name), props) in edge_groups {
            // Check temporal props
            let prop_id = graph
                .edge_meta()
                .temporal_prop_meta()
                .get_id(&prop_name)
                .unwrap_or_else(|| panic!("Failed to get prop id for {}", prop_name));

            let edge = graph
                .nodes()
                .get_edge(src, dst)
                .unwrap_or_else(|| panic!("Failed to get edge ({:?}, {:?}) from graph", src, dst));
            let edge = graph.edges().edge(edge);
            let e = edge.as_ref();
            let actual_props = e.t_prop(prop_id).iter_t().collect::<Vec<_>>();

            assert_eq!(
                actual_props, props,
                "Expected properties for edge ({:?}, {:?}) to be {:?}, but got {:?}",
                src, dst, props, actual_props
            );

            // Check const props
            if let Some(exp_const_props) = const_props.get(&(src, dst)) {
                for (name, prop) in exp_const_props {
                    let prop_id = graph
                        .edge_meta()
                        .const_prop_meta()
                        .get_id(name)
                        .unwrap_or_else(|| panic!("Failed to get prop id for {}", name));
                    let actual_props = e.c_prop(prop_id);
                    assert_eq!(
                        actual_props.as_ref(),
                        Some(prop),
                        "Expected const properties for edge ({:?}, {:?}) to be {:?}, but got {:?}",
                        src,
                        dst,
                        prop,
                        actual_props
                    );
                }
            }
        }

        // Check node additions and their timestamps
        for (node_id, ts) in node_groups {
            let node = graph.nodes().node(node_id);
            let node_entry = node.as_ref();
            let actual_additions_ts = node_entry.additions().iter_t().collect::<Vec<_>>();

            assert_eq!(
                actual_additions_ts, ts,
                "Expected node additions for node ({:?}) to be {:?}, but got {:?}",
                node_id, ts, actual_additions_ts
            );
        }
    };

    check_fn(edges, &graph);

    if check_load {
        // Load the graph from disk and check again
        drop(graph);

        let graph = GraphStore::<NS, ES, EXT>::load(graph_dir.path()).unwrap();
        check_fn(edges, &graph);
    }
}

// pub fn check_load_support<
//     EXT: Clone + Default + Send + Sync,
//     NS: NodeSegmentOps<Extension = EXT>,
//     ES: EdgeSegmentOps<Extension = EXT>,
// >(
//     edges: &[(i64, u64, u64)],
//     check_load: bool,
//     make_graph: impl FnOnce(&Path) -> GraphStore<NS, ES, EXT>,
// ) {
//     // Create temporary directory and CSV file
//     let temp_dir = tempfile::tempdir().unwrap();
//     let csv_path = temp_dir.path().join("edges.csv");

//     // Write edges to CSV file
//     let mut file = File::create(&csv_path).unwrap();
//     writeln!(file, "src,time,dst,test").unwrap();
//     for (time, src, dst) in edges {
//         writeln!(file, "{},{},{},a", src, time, dst).unwrap();
//     }
//     file.flush().unwrap();

//     // Create graph store
//     let graph_dir = temp_dir.path().join("graph");
//     std::fs::create_dir_all(&graph_dir).unwrap();
//     let graph = make_graph(&graph_dir);

//     // Create loader and load data
//     let loader = Loader::new(
//         &csv_path,
//         Left("src"),
//         Left("dst"),
//         Left("time"),
//         FileFormat::CSV {
//             delimiter: b',',
//             has_header: true,
//             sample_records: 10,
//         },
//     )
//     .unwrap();

//     let resolver = loader.load_into(&graph, 1024).unwrap();

//     fn check_graph<
//         NS: NodeSegmentOps<Extension = EXT>,
//         ES: EdgeSegmentOps<Extension = EXT>,
//         EXT: Clone + Default + Send + Sync,
//     >(
//         edges: &[(i64, u64, u64)],
//         graph: &GraphStore<NS, ES, EXT>,
//         resolver: &Mapping,
//         label: &str,
//     ) {
//         // Create expected adjacency data
//         let mut expected_out_edges: HashMap<u64, Vec<u64>> = HashMap::new();
//         let mut expected_in_edges: HashMap<u64, Vec<u64>> = HashMap::new();
//         let mut reverse_resolver = vec![0; resolver.len()];

//         for &(_, src, dst) in edges {
//             expected_out_edges.entry(src).or_default().push(dst);
//             expected_in_edges.entry(dst).or_default().push(src);
//             let id = resolver
//                 .get_u64(src)
//                 .unwrap_or_else(|| panic!("Missing src node {}", src));
//             reverse_resolver[id.0] = src;
//             let id = resolver
//                 .get_u64(dst)
//                 .unwrap_or_else(|| panic!("Missing dst node {}", dst));
//             reverse_resolver[id.0] = dst;
//         }

//         // Deduplicate expected edges
//         for values in expected_out_edges.values_mut() {
//             values.sort_unstable();
//             values.dedup();
//         }

//         for values in expected_in_edges.values_mut() {
//             values.sort_unstable();
//             values.dedup();
//         }

//         let expected_num_edges = expected_out_edges.values().map(Vec::len).sum::<usize>();
//         // let expected_num_nodes = expected_out_edges.keys().chain(expected_in_edges.keys()).collect::<HashSet<_>>().len();

//         // Verify graph structure
//         let nodes = graph.nodes();
//         let edges_store = graph.edges();

//         // assert_eq!(nodes.num_nodes(), expected_num_nodes);
//         assert_eq!(
//             edges_store.num_edges(),
//             expected_num_edges,
//             "Bad number of edges {label}"
//         );

//         for (exp_src, expected_outs) in expected_out_edges {
//             for &exp_dst in &expected_outs {
//                 let src_vid = resolver.get_u64(exp_src).unwrap();
//                 let dst_vid = resolver.get_u64(exp_dst).unwrap();

//                 let edge_id = graph
//                     .nodes()
//                     .get_edge(src_vid, dst_vid)
//                     .expect("Edge not found");
//                 let edge = edges_store.edge(edge_id);
//                 let (src, dst) = edge.as_ref().edge().unwrap();
//                 let (src_act, dst_act) = (reverse_resolver[src.0], reverse_resolver[dst.0]);

//                 assert_eq!(
//                     (src_act, dst_act),
//                     (exp_src, exp_dst),
//                     "{label} Bad Edge {} -> {}",
//                     exp_src,
//                     exp_dst,
//                 );
//             }

//             let adj = graph.nodes().node(resolver.get_u64(exp_src).unwrap());
//             let adj = adj.as_ref();

//             let mut out_neighbours: Vec<_> = adj
//                 .out_nbrs_sorted()
//                 .map(|VID(id)| reverse_resolver[id])
//                 .collect();
//             out_neighbours.sort_unstable();
//             let mut expected_outs: Vec<_> = expected_outs.iter().copied().collect();
//             expected_outs.sort_unstable();

//             assert_eq!(
//                 out_neighbours, expected_outs,
//                 "{label} Outbound edges don't match for node {}",
//                 exp_src
//             );

//             // Check edge lookup works and edge_id points to the right (src, dst)
//             for (exp_dst, edge_id) in adj.out_edges() {
//                 let (VID(src), dst) = edges_store.get_edge(edge_id).unwrap();
//                 assert_eq!(reverse_resolver[src], exp_src);
//                 assert_eq!(dst, exp_dst);
//             }
//         }

//         for (exp_dst, expected_ins) in expected_in_edges {
//             let adj = nodes.node(resolver.get_u64(exp_dst).unwrap());
//             let adj = adj.as_ref();

//             let mut in_neighbours: Vec<_> = adj
//                 .inb_nbrs_sorted()
//                 .map(|VID(id)| reverse_resolver[id])
//                 .collect();
//             in_neighbours.sort_unstable();
//             let mut expected_ins: Vec<_> = expected_ins.iter().copied().collect();
//             expected_ins.sort_unstable();

//             assert_eq!(
//                 in_neighbours, expected_ins,
//                 "Inbound edges don't match for node {}",
//                 exp_dst
//             );

//             // Check edge lookup works
//             for (exp_src, edge_id) in adj.inb_edges() {
//                 let (src, VID(dst)) = edges_store.get_edge(edge_id).unwrap();
//                 assert_eq!(reverse_resolver[dst], exp_dst);
//                 assert_eq!(src, exp_src);
//             }
//         }
//     }

//     check_graph(edges, &graph, &resolver, "pre-drop");
//     if check_load {
//         drop(graph);

//         // Reload graph and check again
//         let graph = GraphStore::<NS, ES, EXT>::load(&graph_dir).unwrap();
//         check_graph(edges, &graph, &resolver, "post-drop");
//     }
// }

pub fn edges_strat(size: usize) -> impl Strategy<Value = Vec<(VID, VID)>> {
    (1..=size).prop_flat_map(|num_nodes| {
        let num_edges = 0..(num_nodes * num_nodes);
        let srcs = (0usize..num_nodes).prop_map(VID);
        let dsts = (0usize..num_nodes).prop_map(VID);
        num_edges.prop_flat_map(move |num_edges| {
            collection::vec((srcs.clone(), dsts.clone()), num_edges as usize)
        })
    })
}

pub type AddEdge = (
    VID,
    VID,
    i64,
    Vec<(String, Prop)>,
    Vec<(String, Prop)>,
    Option<&'static str>,
);

#[derive(Debug)]
pub struct NodeFixture {
    pub temp_props: Vec<(VID, i64, Vec<(String, Prop)>)>,
    pub const_props: Vec<(VID, Vec<(String, Prop)>)>,
}

#[derive(Debug)]
pub struct Fixture {
    pub edges: Vec<AddEdge>,
    pub const_props: HashMap<(VID, VID), Vec<(String, Prop)>>,
}

impl From<Vec<AddEdge>> for Fixture {
    fn from(edges: Vec<AddEdge>) -> Self {
        let mut const_props = HashMap::new();
        for (src, dst, _, _, c_props, _) in &edges {
            for (k, v) in c_props {
                const_props
                    .entry((*src, *dst))
                    .or_insert_with(|| vec![])
                    .push((k.clone(), v.clone()));
            }
        }
        const_props.iter_mut().for_each(|(_, v)| {
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v.dedup_by(|a, b| a.0 == b.0);
        });
        Self { edges, const_props }
    }
}

pub fn make_edges(num_edges: usize, num_nodes: usize) -> impl Strategy<Value = Fixture> {
    assert!(num_edges > 0);
    assert!(num_nodes > 0);
    (1..=num_edges, 1..=num_nodes)
        .prop_flat_map(|(len, num_nodes)| build_raw_edges(len, num_nodes))
        .prop_map(|edges| edges.into())
}

pub fn make_nodes(num_nodes: usize) -> impl Strategy<Value = NodeFixture> {
    assert!(num_nodes > 0);
    let schema = proptest::collection::hash_map(
        (0i32..1000).prop_map(|i| i.to_string()),
        prop_type(),
        0..30,
    );

    schema.prop_flat_map(move |schema| {
        let (t_props, c_props) = make_props(&schema);
        let temp_props = proptest::collection::vec(
            ((0..num_nodes).prop_map(VID), 0i64..1000, t_props),
            1..=num_nodes,
        );

        let const_props =
            proptest::collection::vec(((0..num_nodes).prop_map(VID), c_props), 1..=num_nodes);

        (temp_props, const_props).prop_map(|(temp_props, const_props)| NodeFixture {
            temp_props,
            const_props,
        })
    })
}

pub fn build_raw_edges(
    len: usize,
    num_nodes: usize,
) -> impl Strategy<
    Value = Vec<(
        VID,
        VID,
        i64,
        Vec<(String, Prop)>,
        Vec<(String, Prop)>,
        Option<&'static str>,
    )>,
> {
    proptest::collection::hash_map((0i32..1000).prop_map(|i| i.to_string()), prop_type(), 0..20)
        .prop_flat_map(move |schema| {
            let (t_props, c_props) = make_props(&schema);

            proptest::collection::vec(
                (
                    (0..num_nodes).prop_map(VID),
                    (0..num_nodes).prop_map(VID),
                    0i64..(num_nodes as i64 * 5),
                    t_props,
                    c_props,
                    proptest::sample::select(vec![Some("a"), Some("b"), None]),
                ),
                1..=len,
            )
        })
}

pub fn prop_type() -> impl Strategy<Value = PropType> {
    let leaf = proptest::sample::select(&[
        PropType::Str,
        PropType::I64,
        PropType::F64,
        PropType::F32,
        PropType::I32,
        PropType::U8,
        PropType::Bool,
        PropType::DTime,
        PropType::NDTime,
        PropType::Decimal { scale: 7 }, // decimal breaks the tests because of polars-parquet
    ]);

    // leaf.prop_recursive(3, 10, 10, |inner| {
    //     let dict = proptest::collection::hash_map(r"\w{1,10}", inner.clone(), 1..10)
    //         .prop_map(|map| PropType::map(map));
    //     let list = inner
    //         .clone()
    //         .prop_map(|p_type| PropType::List(Box::new(p_type)));
    //     prop_oneof![inner, list, dict]
    // })
    leaf
}

pub fn make_props(
    schema: &HashMap<String, PropType>,
) -> (
    BoxedStrategy<Vec<(String, Prop)>>,
    BoxedStrategy<Vec<(String, Prop)>>,
) {
    let mut iter = schema.iter();

    // split in half, one temporal one constant
    let t_prop_s = (&mut iter)
        .take(schema.len() / 2)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let c_prop_s = iter
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();

    let num_tprops = t_prop_s.len();
    let num_cprops = c_prop_s.len();

    let t_props = proptest::sample::subsequence(t_prop_s, 0..=num_tprops).prop_flat_map(|schema| {
        schema
            .into_iter()
            .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
            .collect::<Vec<_>>()
    });
    let c_props = proptest::sample::subsequence(c_prop_s, 0..=num_cprops).prop_flat_map(|schema| {
        schema
            .into_iter()
            .map(|(k, v)| prop(&v).prop_map(move |prop| (k.clone(), prop)))
            .collect::<Vec<_>>()
    });
    (t_props.boxed(), c_props.boxed())
}

pub(crate) fn prop(p_type: &PropType) -> impl Strategy<Value = Prop> + use<> {
    match p_type {
        PropType::Str => (0i32..1000).prop_map(|s| Prop::str(s.to_string())).boxed(),
        PropType::I64 => any::<i64>().prop_map(Prop::I64).boxed(),
        PropType::I32 => any::<i32>().prop_map(Prop::I32).boxed(),
        PropType::F64 => any::<f64>().prop_map(Prop::F64).boxed(),
        PropType::F32 => any::<f32>().prop_map(Prop::F32).boxed(),
        PropType::U8 => any::<u8>().prop_map(Prop::U8).boxed(),
        PropType::Bool => any::<bool>().prop_map(Prop::Bool).boxed(),
        PropType::DTime => (1900..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
            .prop_map(|(year, month, day, h, m, s)| {
                Prop::DTime(
                    format!(
                        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                        year, month, day, h, m, s
                    )
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
                )
            })
            .boxed(),
        PropType::NDTime => (1970..2024, 1..=12, 1..28, 0..24, 0..60, 0..60)
            .prop_map(|(year, month, day, h, m, s)| {
                // 2015-09-18T23:56:04
                Prop::NDTime(
                    format!(
                        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                        year, month, day, h, m, s
                    )
                    .parse::<NaiveDateTime>()
                    .unwrap(),
                )
            })
            .boxed(),
        PropType::List(p_type) => proptest::collection::vec(prop(p_type), 0..10)
            .prop_map(|props| Prop::List(props.into()))
            .boxed(),
        PropType::Map(p_types) => {
            let prop_types: Vec<BoxedStrategy<(String, Prop)>> = p_types
                .iter()
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect::<Vec<_>>()
                .into_iter()
                .map(|(name, p_type)| {
                    let pt_strat = prop(&p_type)
                        .prop_map(move |prop| (name.clone(), prop.clone()))
                        .boxed();
                    pt_strat
                })
                .collect_vec();

            let props = proptest::sample::select(prop_types).prop_flat_map(|prop| prop);

            proptest::collection::vec(props, 1..10)
                .prop_map(|props| Prop::map(props))
                .boxed()
        }
        PropType::Decimal { scale } => {
            let scale = *scale;
            let dec_max = DECIMAL_MAX;
            ((scale as i128)..dec_max)
                .prop_map(move |int| Prop::Decimal(BigDecimal::new(int.into(), scale)))
                .boxed()
        }
        pt => {
            panic!("Unsupported prop type: {:?}", pt);
        }
    }
}
