use crate::{
    arrow::{
        graph_impl::ArrowGraph,
        query::{
            ast::{Hop, Query, Sink},
            state::{HopState, StaticGraphHopState},
            NodeSource,
        },
    },
    core::{entities::VID, Direction},
    db::{api::view::StaticGraphViewOps, graph::node::NodeView},
    prelude::*,
};
use itertools::Itertools;
use raphtory_arrow::{nodes::Node, RAError};
use rayon::{current_thread_index, Scope, ThreadPoolBuilder};
use std::{cell::RefCell, fs::File, io::BufWriter, path::Path, sync::Arc};

pub fn execute<S: HopState + 'static>(
    query: Query<S>,
    source: NodeSource,
    graph: &ArrowGraph,
    make_state: impl Fn(Node) -> S + Send + Sync,
) -> Result<(), RAError> {
    let tp = ThreadPoolBuilder::new()
        .build()
        .expect("failed to make a thread pool what's the point!");

    let chunk_size = 10;

    if let Sink::Path(dir, _) = query.sink.as_ref() {
        std::fs::create_dir_all(dir)?;
    }

    let tl = Arc::new(thread_local::ThreadLocal::new());
    tp.scope(|s| {
        let tl = &tl;
        for node_chunk in source.into_iter(graph).chunks(chunk_size).into_iter() {
            let node_chunk = node_chunk.collect::<Vec<_>>();

            s.spawn(|s| {
                let hop = query.get_hop(0).expect("No hops");
                let layer = lookup_layer(&hop.layer, graph);
                for node in node_chunk {
                    let node = graph.inner.layer(layer).node(node);
                    let state = (make_state)(node);
                    hop_arrow_graph(node, &query, 0, state, graph, s, tl);
                }
            })
        }
    });

    Ok(())
}

pub fn execute_static_graph<G: StaticGraphViewOps, S: StaticGraphHopState + 'static>(
    query: Query<S>,
    source: NodeSource,
    graph: G,
    start_state: S,
) -> Result<(), RAError> {
    let tp = ThreadPoolBuilder::new()
        .build()
        .expect("failed to make a thread pool what's the point!");

    if let Sink::Path(dir, _) = query.sink.as_ref() {
        std::fs::create_dir_all(dir)?;
    }

    let tl = Arc::new(thread_local::ThreadLocal::new());
    tp.scope(|s| {
        let tl = &tl;
        let start_state = &start_state;
        let graph = &graph;
        let query = &query;
        for node in source.into_iter_static_g(graph.clone()) {
            s.spawn(move |s| {
                let node = node_view(s, graph, node);
                let state = start_state.start(&node);
                hop_static_graph_view(node, query, 0, state, graph, s, tl);
            })
        }
    });

    Ok(())
}

fn node_view<'a, 'b, G: StaticGraphViewOps>(
    _s: &'b Scope<'a>,
    graph: &'a G,
    node: VID,
) -> NodeView<&'a G> {
    NodeView::new_internal(graph, node)
}

fn lookup_layer(layer: &str, graph: &ArrowGraph) -> usize {
    graph.inner.find_layer_id(layer).expect("No layer")
}

fn get_writer<'a>(
    dir: impl AsRef<Path>,
    tl: &'a thread_local::ThreadLocal<RefCell<BufWriter<File>>>,
) -> &'a RefCell<BufWriter<File>> {
    let out = tl.get_or(|| {
        let thread_index = current_thread_index().expect("No thread index");
        let path = dir.as_ref().join(format!("part_{}.bin", thread_index));
        RefCell::new(BufWriter::with_capacity(
            256 * 1024,
            File::create(path).expect("Cannot create file"),
        ))
    });
    out
}

fn hop_arrow_graph<'a, S: HopState + 'a>(
    node: Node,
    query: &'a Query<S>,
    step: usize,
    state: S,
    graph: &'a ArrowGraph,
    s: &rayon::Scope<'a>,
    tl: &'a Arc<thread_local::ThreadLocal<RefCell<BufWriter<File>>>>,
) {
    let vid = node.vid();
    let Query { sink, .. } = query;
    if let Some(Hop {
        dir,
        variable,
        layer,
        limit,
    }) = query.get_hop(step)
    {
        let layer = lookup_layer(layer, graph);
        if *variable {
            do_sink(sink, s, state.clone(), vid.into(), tl);
        }
        let limit = limit.unwrap_or(usize::MAX);
        match dir {
            Direction::OUT => s.spawn(move |s| {
                let layer = graph.inner.layer(layer);
                layer
                    .nodes_storage()
                    .out_adj_list(vid)
                    .map(|(eid, vid)| (layer.edge(eid), layer.node(vid)))
                    .filter_map(|(edge, node)| {
                        state
                            .hop_with_state(node, edge)
                            .map(|new_state| (edge, node, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_arrow_graph(node, query, step + 1, state, graph, s, tl);
                    });
            }),
            Direction::IN => s.spawn(move |s| {
                let layer = graph.inner.layer(layer);
                layer
                    .nodes_storage()
                    .in_adj_list(vid)
                    .map(|(eid, vid)| (layer.edge(eid), layer.node(vid)))
                    .filter_map(|(edge, node)| {
                        state
                            .hop_with_state(node, edge)
                            .map(|new_state| (edge, node, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_arrow_graph(node, query, step + 1, state, graph, s, tl);
                    });
            }),
            Direction::BOTH => {
                todo!()
            }
        }
    } else {
        do_sink(sink, s, state, vid.into(), tl);
    }
}

fn hop_static_graph_view<'a, G: GraphViewOps<'a>, S: StaticGraphHopState + 'a>(
    node: NodeView<&'a G>,
    query: &'a Query<S>,
    step: usize,
    state: S,
    graph: &'a G,
    s: &rayon::Scope<'a>,
    tl: &'a Arc<thread_local::ThreadLocal<RefCell<BufWriter<File>>>>,
) {
    let vid = node.node;
    let Query { sink, .. } = query;
    if let Some(Hop {
        dir,
        variable,
        layer,
        limit,
    }) = query.get_hop(step)
    {
        if *variable {
            do_sink(sink, s, state.clone(), vid, tl);
        }
        let limit = limit.unwrap_or(usize::MAX);
        match dir {
            Direction::OUT => s.spawn(move |s| {
                node.valid_layers(layer.as_ref())
                    .out_edges()
                    .iter()
                    .filter_map(|edge| {
                        let dst = edge.dst();
                        state
                            .hop_with_state(&dst, &edge.reset_filter())
                            .map(|new_state| (edge, dst, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_static_graph_view::<G, S>(node, query, step + 1, state, graph, s, tl);
                    });
            }),
            Direction::IN => s.spawn(move |s| {
                node.valid_layers(layer.as_ref())
                    .in_edges()
                    .iter()
                    .filter_map(|edge| {
                        let src = edge.src();
                        state
                            .hop_with_state(&src, &edge.reset_filter())
                            .map(|new_state| (edge, src, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_static_graph_view::<G, S>(node, query, step + 1, state, graph, s, tl);
                    });
            }),
            Direction::BOTH => {
                todo!()
            }
        }
    } else {
        do_sink(sink, s, state, vid, tl);
    }
}

fn do_sink<'a, 'b: 'a, 'c, S: Clone + std::fmt::Debug + Send + Sync>(
    sink: &'b Sink<S>,
    s: &'c rayon::Scope<'a>,
    state: S,
    vid: VID,
    tl: &thread_local::ThreadLocal<RefCell<BufWriter<File>>>,
) {
    match sink {
        Sink::Channel(senders) => {
            s.spawn(move |_| {
                let sender = &senders[vid.0 % senders.len()];

                sender
                    .send((state.clone(), vid))
                    .expect("Failed to send node id");
            });
        }
        Sink::Void => {}
        Sink::Print => {
            println!("Node: {:?}, State: {:?}", vid, state);
        }
        Sink::Path(dir, writer_fn) => {
            let local_writer = get_writer(dir, tl);
            let mut writer = local_writer.borrow_mut();
            (writer_fn)(&mut writer, state);
        }
    }
}
