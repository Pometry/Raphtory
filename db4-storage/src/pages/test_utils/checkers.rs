use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::{prop::Prop, tprop::TPropOps},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{ELID, VID},
    storage::timeindex::TimeIndexOps,
};
use rayon::prelude::*;

use crate::{
    api::{
        edges::{EdgeEntryOps, EdgeRefOps, EdgeSegmentOps},
        nodes::{NodeEntryOps, NodeRefOps, NodeSegmentOps},
    },
    error::DBV4Error,
    pages::GraphStore,
};

use super::fixtures::{AddEdge, Fixture, NodeFixture};

pub fn check_edges_support<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    EXT: Clone + Default + Send + Sync + std::fmt::Debug,
>(
    edges: Vec<(impl Into<VID>, impl Into<VID>, Option<usize>)>, // src, dst, optional layer_id
    par_load: bool,
    check_load: bool,
    make_graph: impl FnOnce(&Path) -> GraphStore<NS, ES, EXT>,
) {
    let mut edges = edges
        .into_iter()
        .map(|(src, dst, layer_id)| (src.into(), dst.into(), layer_id))
        .collect::<Vec<_>>();

    let graph_dir = tempfile::tempdir().unwrap();
    let graph = make_graph(graph_dir.path());
    let mut nodes = HashSet::new();

    for (src, dst, _) in &edges {
        nodes.insert(*src);
        nodes.insert(*dst);
    }

    if par_load {
        edges
            .par_iter()
            .try_for_each(|(src, dst, layer_id)| {
                let lsn = 0;
                let timestamp = 0;

                if let Some(layer_id) = layer_id {
                    let mut session = graph.write_session(*src, *dst, None);
                    let eid = session.add_static_edge(*src, *dst, lsn);
                    let elid = eid.map(|eid| eid.with_layer(*layer_id));

                    session.add_edge_into_layer(timestamp, *src, *dst, elid, lsn, []);
                } else {
                    let _ = graph.add_edge(timestamp, *src, *dst)?;
                }

                Ok::<_, DBV4Error>(())
            })
            .expect("Failed to add edge");
    } else {
        edges
            .iter()
            .try_for_each(|(src, dst, layer_id)| {
                let lsn = 0;
                let timestamp = 0;

                if let Some(layer_id) = layer_id {
                    let mut session = graph.write_session(*src, *dst, None);
                    let eid = session.add_static_edge(*src, *dst, lsn).inner();
                    let elid = eid.with_layer(*layer_id);

                    session.add_edge_into_layer(
                        timestamp,
                        *src,
                        *dst,
                        MaybeNew::Existing(elid),
                        lsn,
                        [],
                    );
                } else {
                    let _ = graph.add_edge(timestamp, *src, *dst)?;
                }

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
        EXT: Clone + Default + std::fmt::Debug,
    >(
        stage: &str,
        expected_edges: &[(VID, VID, Option<usize>)], // (src, dst, layer_id)
        graph: &GraphStore<NS, ES, EXT>,
    ) {
        let nodes = graph.nodes();
        let edges = graph.edges();

        if !expected_edges.is_empty() {
            assert!(nodes.pages().count() > 0, "{stage}");
        }

        // Group edges by layer_id first
        let mut edges_by_layer: HashMap<usize, Vec<(VID, VID)>> = HashMap::new();
        for (src, dst, layer_id) in expected_edges {
            edges_by_layer
                .entry(layer_id.unwrap_or(0)) // Default layer_id to 0
                .or_default()
                .push((*src, *dst));
        }

        // For each layer, build the expected graph structure
        for (layer_id, layer_edges) in edges_by_layer {
            let mut expected_graph: HashMap<VID, (Vec<VID>, Vec<VID>)> = layer_edges
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

            let mut edges_sorted_by_dest = layer_edges.clone();
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
                let out_nbrs: Vec<_> = adj.out_nbrs_sorted(layer_id).collect();
                assert_eq!(
                    out_nbrs, exp_out,
                    "{stage} node: {:?} layer: {}",
                    n, layer_id
                );

                let in_nbrs: Vec<_> = adj.inb_nbrs_sorted(layer_id).collect();
                assert_eq!(
                    in_nbrs, exp_inb,
                    "{stage} node: {:?} layer: {}",
                    n, layer_id
                );

                for (exp_dst, eid) in adj.out_edges(layer_id) {
                    let elid = ELID::new(eid, layer_id);
                    let (src, dst) = edges.get_edge(elid).unwrap();

                    assert_eq!(src, n, "{stage} layer: {}", layer_id);
                    assert_eq!(dst, exp_dst, "{stage} layer: {}", layer_id);
                }

                for (exp_src, eid) in adj.inb_edges(layer_id) {
                    let elid = ELID::new(eid, layer_id);
                    let (src, dst) = edges.get_edge(elid).unwrap();

                    assert_eq!(src, exp_src, "{stage} layer: {}", layer_id);
                    assert_eq!(dst, n, "{stage} layer: {}", layer_id);
                }
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
        let err = graph.add_node_props(*t, *node, 0, t_props.clone());

        assert!(err.is_ok(), "Failed to add node: {:?}", err);
    }

    for (node, const_props) in const_props {
        let err = graph.update_node_const_props(*node, 0, const_props.clone());

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
            let actual: Vec<_> = node_entry.additions(0).iter_t().collect();
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
            let actual_props = node_entry.c_prop(0, prop_id);

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
            let actual_props = node_entry.t_prop(0, prop_id).iter_t().collect::<Vec<_>>();

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
        let layer_id = 0;
        let eid = graph
            .nodes()
            .get_edge(*src, *dst, layer_id)
            .unwrap_or_else(|| panic!("Failed to get edge ({:?}, {:?}) from graph", src, dst));
        let elid = ELID::new(eid, layer_id);
        let res = graph.update_edge_const_props(elid, const_props.clone());

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
                .get_edge(src, dst, 0)
                .unwrap_or_else(|| panic!("Failed to get edge ({:?}, {:?}) from graph", src, dst));
            let edge = graph.edges().edge(edge);
            let e = edge.as_ref();
            let layer_id = 0;
            let actual_props = e.t_prop(layer_id, prop_id).iter_t().collect::<Vec<_>>();

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
                    let actual_props = e.c_prop(layer_id, prop_id);
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
            let actual_additions_ts = node_entry.additions(0).iter_t().collect::<Vec<_>>();

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
