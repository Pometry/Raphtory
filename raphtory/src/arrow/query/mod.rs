use std::sync::{mpsc::Sender, Arc};

use crate::core::{entities::VID, Direction};

use self::{
    ast::{Hop, Query},
    state::HopState,
};

use super::{col_graph2::TempColGraphFragment, edge::Edge, nodes::Node, Error};
use rayon::prelude::*;

pub mod ast;
pub mod executors;
pub mod state;

pub enum NodeSource {
    All,
    NodeIds(Vec<VID>),
    Filter(Arc<dyn Fn(Node) -> bool + Send + Sync>),
}

impl NodeSource {
    fn for_each<OP>(self, graph: &TempColGraphFragment, op: OP)
    where
        OP: Fn(Node) + Sync + Send,
    {
        match self {
            NodeSource::All => {
                graph.all_nodes_par().for_each(op);
            }
            NodeSource::NodeIds(ids) => {
                ids.par_iter()
                    .map(|node_id| graph.node(*node_id))
                    .for_each(op);
            }
            NodeSource::Filter(filter) => {
                graph
                    .all_nodes_par()
                    .filter(|node_id| filter(*node_id))
                    .for_each(op);
            }
        }
    }

    fn into_iter(self, graph: &TempColGraphFragment) -> Box<dyn Iterator<Item = VID> + '_> {
        match self {
            NodeSource::All => Box::new((0..graph.num_nodes()).into_iter().map(VID)),
            NodeSource::NodeIds(ids) => Box::new(ids.into_iter()),
            NodeSource::Filter(filter) => Box::new(
                graph
                    .all_nodes()
                    .filter(move |node| filter(*node))
                    .map(|node| node.vid()),
            ),
        }
    }
}

pub fn execute<S: HopState>(
    query: Query<S>,
    source: NodeSource,
    graph: &TempColGraphFragment,
) -> Result<(), Error> {
    source.for_each(graph, |node| {
        run::<S>(&query, 0, node, None, S::new(node), graph)
    });
    Ok(())
}

fn run<S: HopState>(
    query: &Query<S>,
    step: usize,
    node: Node,
    edge: Option<Edge>,
    state: S,
    graph: &TempColGraphFragment,
) {
    let Query { sink, .. } = query;
    if let Some(Hop {
        dir,
        filter,
        variable,
    }) = query.get_hop(step)
    {
        // if this is a variable hop and we're not at the last step then print the intermediate result
        if *variable && !(query.hops.len() - 1 == step) {
            run_sink(sink, state.clone(), node);
        }
        run_hop_edges::<S>(*dir, step, query, node, edge, filter.as_ref(), state, graph);
    } else {
        run_sink(sink, state, node);
    }
}

fn run_sink<S: HopState>(sink: &ast::Sink<S>, state: S, node: Node<'_>) {
    match sink {
        ast::Sink::Channel(sender) => run_channel(sender, state, node),
        ast::Sink::Void => run_void(),
        ast::Sink::Print => run_print(node),
    }
}

fn run_channel<S>(sender: &Sender<(S, VID)>, state: S, node: Node<'_>) {
    sender
        .send((state, node.vid()))
        .expect("Failed to send node id");
}

fn run_void() {
    // do nothing
}

fn run_hop_edges<S: HopState>(
    direction: Direction,
    step: usize,
    query: &Query<S>,
    node: Node,
    edge: Option<Edge>,
    filter: Option<&Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
    state: S,
    graph: &TempColGraphFragment,
) {
    match direction {
        Direction::OUT => {
            graph
                .nodes
                .par_out_adj_list(node.vid())
                .map(|(eid, vid)| (graph.edge(eid), graph.node(vid), state.clone()))
                .for_each(|(edge, node, state)| {
                    run::<S>(
                        query,
                        step + 1,
                        node,
                        Some(edge),
                        state.with_next(node, edge),
                        graph,
                    )
                });
        }
        Direction::IN => {
            graph
                .nodes
                .par_in_adj_list(node.vid())
                .map(|(eid, vid)| (graph.edge(eid), graph.node(vid), state.clone()))
                .for_each(|(edge, node, state)| {
                    run::<S>(
                        query,
                        step + 1,
                        node,
                        Some(edge),
                        state.with_next(node, edge),
                        graph,
                    )
                });
        }
        Direction::BOTH => {
            todo!()
        }
    }
}

fn run_print(node: Node) {
    println!("{:?}", node.vid());
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::core::storage::timeindex::TimeIndexOps;

    use super::{ast::*, executors::*, state::*, *};

    #[test]
    fn one_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out().channel(sender);

        let graph_dir = tempfile::tempdir().unwrap();

        let graph = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let result =
            rayon2::execute::<NoState>(query, NodeSource::All, &graph, |n| NoState::new(n));
        assert!(result.is_ok());

        let mut actual = receiver.into_iter().collect::<Vec<_>>();
        actual.sort_by_key(|(_, vid)| *vid);
        assert_eq!(actual, vec![(NoState, VID(1)), (NoState, VID(2))]);
    }

    #[test]
    fn two_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out().out().channel(sender);

        let graph_dir = tempfile::tempdir().unwrap();
        let graph = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let result =
            rayon2::execute::<NoState>(query, NodeSource::All, &graph, |n| NoState::new(n));
        assert!(result.is_ok());
        let (_, vid) = receiver.recv().unwrap();
        assert_eq!(vid, VID(2));
    }

    #[test]
    fn two_hop_query_var() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out().out_var().channel(sender);

        let graph_dir = tempfile::tempdir().unwrap();
        let graph = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let result =
            rayon2::execute::<VecState>(query, NodeSource::All, &graph, |n| VecState::new(n));
        assert!(result.is_ok());
        let mut actual = receiver.into_iter().map(|(state, _)| state.0).collect_vec();
        actual.sort();

        let mut expected = vec![
            vec![VID(1), VID(2)],
            vec![VID(0), VID(1)],
            vec![VID(0), VID(1), VID(2)],
        ];

        expected.sort();

        assert_eq!(actual, expected);
    }

    #[test]
    fn two_hop_query_state() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out().out().channel(sender);

        let graph_dir = tempfile::tempdir().unwrap();
        let graph = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let result =
            rayon2::execute::<VecState>(query, NodeSource::All, &graph, |n| VecState::new(n));
        assert!(result.is_ok());
        let (path, vid) = receiver.recv().unwrap();
        assert_eq!(vid, VID(2));
        assert_eq!(path.0, vec![VID(0), VID(1), VID(2)]);
    }

    #[test]
    fn test_fork_2_hop() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let query = Query::new().out().out().channel(sender);

        let graph_dir = tempfile::tempdir().unwrap();
        let graph = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64), (2, 1, 3, 3f64)],
            0,
            100,
        )
        .unwrap();

        let result =
            rayon2::execute::<VecState>(query, NodeSource::All, &graph, |n| VecState::new(n));
        assert!(result.is_ok());

        let mut results = receiver.into_iter().collect::<Vec<_>>();
        results.sort_by_key(|(_, vid)| *vid);

        assert_eq!(results.len(), 2);

        assert_eq!(
            results,
            vec![
                (VecState(vec![VID(0), VID(1), VID(2)]), VID(2)),
                (VecState(vec![VID(0), VID(1), VID(3)]), VID(3)),
            ]
        );
    }

    fn forward_time_filter(_node: Node, edge: Edge, state: &ForwardState) -> bool {
        let ts = edge.into_distinct_timestamps();

        let range = state.time + 1..i64::MAX;
        let iter = ts.range(range);
        iter.first_t().is_some()
    }

    #[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
    struct ForwardState {
        pub time: i64,
        pub path: rpds::ListSync<VID>,
    }

    impl ForwardState {
        fn at_time(node: Node, t: i64) -> Self {
            ForwardState {
                time: t,
                path: rpds::List::new_sync().push_front(node.vid()),
            }
        }

        fn iter(&self) -> impl Iterator<Item = VID> + '_ {
            self.path.iter().copied()
        }
    }

    impl HopState for ForwardState {
        fn new(node: Node) -> Self {
            ForwardState {
                time: i64::MIN,
                path: rpds::List::new_sync().push_front(node.vid()),
            }
        }

        fn with_next(&self, node: Node, edge: Edge) -> Self {
            let ts = edge.into_distinct_timestamps();
            let w = self.time + 1..i64::MAX;
            let next_time = ts.range(w);
            ForwardState {
                time: next_time.first_t().unwrap(),
                path: self.path.push_front(node.vid()),
            }
        }
    }

    #[test]
    fn hop_twice_forward_time() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new()
            .out_filter(Arc::new(forward_time_filter))
            .out_filter(Arc::new(forward_time_filter))
            .channel(sender);

        let mut edges = vec![
            (11, 0u64, 1u64, 0.0f64),
            (10, 0, 2, 0.0),
            (12, 1, 3, 0.0),
            (13, 1, 4, 0.0),
            (5, 3, 5, 0.0),
            (10, 3, 6, 0.0),
            (15, 3, 6, 0.0),
            (14, 4, 3, 0.0),
            (14, 4, 4, 0.0),
            (10, 4, 7, 0.0),
        ];

        edges.sort_by_key(|(t, src, dst, _)| (*src, *dst, *t));

        let graph_dir = tempfile::tempdir().unwrap();
        let mut graph = TempColGraphFragment::from_edges(graph_dir.path(), edges, 0, 100).unwrap();
        graph
            .node_additions(100)
            .expect("Failed to load node additions");

        let t = 10;
        let result = rayon2::execute::<ForwardState>(
            query,
            NodeSource::NodeIds(vec![VID(0), VID(1)]),
            &graph,
            |n| ForwardState::at_time(n, t),
        );
        assert!(result.is_ok());

        let mut results = receiver.into_iter().collect::<Vec<_>>();
        results.sort();

        assert_eq!(results.len(), 5);

        let mut actual = results
            .iter()
            .map(|(state, vid)| (state.time, state.iter().collect::<Vec<_>>(), *vid))
            .collect::<Vec<_>>();
        actual.sort();

        let mut expected = vec![
            (12i64, vec![VID(3), VID(1), VID(0)], VID(3)),
            (14, vec![VID(3), VID(4), VID(1)], VID(3)),
            (14, vec![VID(4), VID(4), VID(1)], VID(4)),
            (13, vec![VID(4), VID(1), VID(0)], VID(4)),
            (15, vec![VID(6), VID(3), VID(1)], VID(6)),
        ];
        expected.sort();
        assert_eq!(actual, expected)
    }
}
