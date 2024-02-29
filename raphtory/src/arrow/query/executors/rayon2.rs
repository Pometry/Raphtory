use std::{cell::RefCell, fs::File, io::BufWriter, path::Path, sync::Arc};

use itertools::Itertools;
use rayon::{current_thread_index, ThreadPoolBuilder};

use crate::{
    arrow::{
        graph_fragment::TempColGraphFragment,
        graph_impl::Graph2,
        nodes::Node,
        query::{
            ast::{Hop, Query, Sink},
            state::HopState,
            NodeSource,
        },
        Error,
    },
    core::{entities::VID, Direction},
    db::api::view::internal::CoreGraphOps,
};

pub fn execute<S: HopState + 'static>(
    query: Query<S>,
    source: NodeSource,
    graph: &Graph2,
    make_state: impl Fn(Node) -> S + Send + Sync,
) -> Result<(), Error> {
    let tp = ThreadPoolBuilder::new()
        .build()
        .expect("failed to make a thread pool what's the point!");

    let chunk_size = 10;

    if let Sink::Path(dir, _) = &query.sink {
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
                    let node = graph.layer(layer).node(node);
                    let state = (make_state)(node);
                    hop_node(node, &query, 0, state, graph, s, tl);
                }
            })
        }
    });

    Ok(())
}

fn lookup_layer(layer: &str, graph: &Graph2) -> usize {
    graph.find_layer_id(layer).expect("No layer")
}

fn get_writer<'a, S>(
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

fn node_range(shard_id: usize, segment_size: usize, num_nodes: usize) -> std::ops::Range<usize> {
    let start = shard_id * segment_size;
    let end = ((shard_id + 1) * segment_size).min(num_nodes);
    start..end
}

fn shard_id(vid: VID, segment_size: usize) -> usize {
    vid.0 / segment_size
}

fn hop_node<'a, S: HopState + 'a>(
    node: Node,
    query: &'a Query<S>,
    step: usize,
    state: S,
    graph: &'a Graph2,
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
            do_sink(sink, s, state.clone(), vid, tl);
        }
        let limit = limit.unwrap_or(usize::MAX);
        match dir {
            Direction::OUT => s.spawn(move |s| {
                let layer = graph.layer(layer);
                layer
                    .nodes
                    .out_adj_list(vid)
                    .map(|(eid, vid)| (layer.edge(eid), layer.node(vid)))
                    .filter_map(|(edge, node)| {
                        state
                            .hop_with_state(node, edge)
                            .map(|new_state| (edge, node, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_node(node, query, step + 1, state, graph, s, tl);
                    });
            }),
            Direction::IN => s.spawn(move |s| {
                let layer = graph.layer(layer);
                layer
                    .nodes
                    .in_adj_list(vid)
                    .map(|(eid, vid)| (layer.edge(eid), layer.node(vid)))
                    .filter_map(|(edge, node)| {
                        state
                            .hop_with_state(node, edge)
                            .map(|new_state| (edge, node, new_state))
                    })
                    .take(limit)
                    .for_each(|(_, node, state)| {
                        hop_node(node, query, step + 1, state, graph, s, tl);
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

fn do_sink<'a, 'b: 'a, 'c, S: HopState>(
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
            let local_writer = get_writer::<S>(dir, tl);
            let mut writer = local_writer.borrow_mut();
            (writer_fn)(&mut writer, state);
        }
    }
}
