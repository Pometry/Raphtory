use itertools::Itertools;
use rayon::ThreadPoolBuilder;

use crate::{
    arrow::{
        col_graph2::TempColGraphFragment,
        nodes::Node,
        query::{
            ast::{Hop, Query, Sink},
            state::HopState,
            NodeSource, run_sink,
        },
        Error,
    },
    core::{entities::VID, Direction},
};

pub fn execute<S: HopState + 'static>(
    query: Query<S>,
    source: NodeSource,
    graph: &TempColGraphFragment,
    make_state: impl Fn(Node) -> S + Send + Sync,
) -> Result<(), Error> {
    let tp = ThreadPoolBuilder::new()
        .build()
        .expect("failed to make a thread pool what's the point!");

    let chunk_size = 100_000;

    tp.scope_fifo(|s| {
        for node_chunk in source.into_iter(graph).chunks(chunk_size).into_iter() {
            let node_chunk = node_chunk.collect::<Vec<_>>();
            s.spawn_fifo(|s| {
                for node in node_chunk {
                    let node = graph.node(node);
                    let state = (make_state)(node);
                    hop_node(node, &query, 0, state, graph, s);
                }
            })
        }
    });

    Ok(())
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
    graph: &'a TempColGraphFragment,
    s: &rayon::ScopeFifo<'a>,
) {
    let vid = node.vid();
    let Query { sink, .. } = query;
    if let Some(Hop { dir, filter, variable }) = query.get_hop(step) {
        if *variable {
            run_sink(sink, state.clone(), node);
        }
        match dir {
            Direction::OUT => s.spawn_fifo(move |s| {
                graph
                    .nodes
                    .out_adj_list(vid)
                    .map(|(eid, vid)| (graph.edge(eid), graph.node(vid)))
                    .filter(|(edge, node)| {
                        filter
                            .as_ref()
                            .map(|f| (f)(*node, *edge, &state))
                            .unwrap_or(true)
                    })
                    .for_each(|(edge, node)| {
                        hop_node(node, query, step+1, state.with_next(node, edge), graph, s);
                    });
            }),
            Direction::IN => {
                graph
                    .nodes
                    .in_adj_list(vid)
                    .map(|(eid, vid)| (graph.edge(eid), graph.node(vid)))
                    .filter(|(edge, node)| {
                        filter
                            .as_ref()
                            .map(|f| (f)(*node, *edge, &state))
                            .unwrap_or(true)
                    })
                    .for_each(|(edge, node)| {
                        hop_node(node, query, step+1, state.with_next(node, edge), graph, s);
                    });
            }
            Direction::BOTH => {
                todo!()
            }
        }
    } else {
        fun_name(sink, s, state, vid);
    }
}

fn fun_name<'a, 'b: 'a, 'c, S: HopState>(sink: &'b Sink<S>, s: &'c rayon::ScopeFifo<'a>, state: S, vid: VID) {
    match sink {
        Sink::Channel(sender) => {
            s.spawn_fifo(move |_| {
                sender
                    .send((state.clone(), vid))
                    .expect("Failed to send node id");
            });
        }
        Sink::Void => {}
        Sink::Print => {
            println!("Node: {:?}, State: {:?}", vid, state);
        }
    }
}
