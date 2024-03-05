use std::sync::Arc;

use itertools::Itertools;

use crate::{
    core::entities::{nodes::node_ref::NodeRef, LayerIds, VID},
    db::api::view::StaticGraphViewOps,
    prelude::NodeViewOps,
};

use self::state::HopState;
use crate::core::storage::timeindex::TimeIndexOps;

use super::{edge::Edge, graph_impl::Graph2, nodes::Node, GID};

pub mod ast;
pub mod executors;
pub mod state;

#[derive(Clone)]
pub enum NodeSource {
    All,
    NodeIds(Vec<VID>),
    ExternalIds(Vec<GID>),
    NodeRefs(Vec<NodeRef>),
    Filter(Arc<dyn Fn(VID, &Graph2) -> bool + Send + Sync>),
}

impl NodeSource {
    fn into_iter(self, graph: &Graph2) -> Box<dyn Iterator<Item = VID> + '_> {
        match self {
            NodeSource::All => Box::new((0..graph.num_nodes()).into_iter().map(VID)),
            NodeSource::NodeIds(ids) => Box::new(ids.into_iter()),
            NodeSource::Filter(filter) => {
                Box::new(graph.all_nodes().filter(move |node| filter(*node, graph)))
            }
            NodeSource::ExternalIds(ext_ids) => Box::new(
                ext_ids
                    .into_iter()
                    .filter_map(move |gid| graph.find_node(&gid)),
            ),
            NodeSource::NodeRefs(_) => todo!(),
        }
    }

    fn into_iter_static_g<G: StaticGraphViewOps>(self, graph: G) -> Box<dyn Iterator<Item = VID>> {
        match self {
            NodeSource::All => Box::new(graph.node_refs(LayerIds::All, None)),
            NodeSource::NodeIds(ids) => Box::new(ids.into_iter()),
            NodeSource::NodeRefs(ext_ids) => Box::new(
                ext_ids
                    .into_iter()
                    .filter_map(move |gid| graph.node(gid))
                    .inspect(|node| println!("node: {:?}", node))
                    .map(|node| node.node),
            ),
            NodeSource::ExternalIds(ids) => Box::new(
                ids.into_iter()
                    .filter_map(move |gid| {
                        gid.as_i64()
                            .and_then(|gid| graph.node(NodeRef::External(gid as u64)))
                            .or_else(|| {
                                gid.as_u64()
                                    .and_then(|gid| graph.node(NodeRef::External(gid)))
                            })
                    })
                    .map(|node| node.node)
                    .into_iter(),
            ),
            NodeSource::Filter(_) => todo!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub struct ForwardState {
    pub time: i64,
    pub path: rpds::ListSync<VID>,
    hop_n_limit: usize,
}

impl ForwardState {
    pub fn at_time(node: Node, t: i64, hop_n_limit: usize) -> Self {
        ForwardState {
            time: t,
            path: rpds::List::new_sync().push_front(node.vid()),
            hop_n_limit,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = VID> + '_ {
        self.path.iter().copied()
    }
}

impl HopState for ForwardState {
    fn hop_with_state(&self, node: Node, edge: Edge) -> Option<Self> {
        let ts = edge.timestamps();
        let w = self.time + 1..i64::MAX;
        let next_time = ts.range(w);

        next_time.first_t().map(|t| ForwardState {
            time: t,
            path: self.path.push_front(node.vid()),
            hop_n_limit: self.hop_n_limit,
        })
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::arrow::graph_fragment::TempColGraphFragment;

    use super::{ast::*, executors::*, state::*, *};

    #[test]
    fn one_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out("default").channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let layer = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let graph = Graph2::from_layer(layer);

        let result = rayon2::execute::<NoState>(query, NodeSource::All, &graph, |_| NoState::new());
        assert!(result.is_ok());

        let mut actual = receiver.into_iter().collect::<Vec<_>>();
        actual.sort_by_key(|(_, vid)| *vid);
        assert_eq!(actual, vec![(NoState, VID(1)), (NoState, VID(2))]);
    }

    #[test]
    fn two_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out("default").out("default").channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();
        let layer = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let graph = Graph2::from_layer(layer);

        let result = rayon2::execute::<NoState>(query, NodeSource::All, &graph, |_| NoState::new());
        assert!(result.is_ok());
        let (_, vid) = receiver.recv().unwrap();
        assert_eq!(vid, VID(2));
    }

    #[test]
    fn two_hop_query_var() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new()
            .out("default")
            .out_var("default")
            .channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();
        let layer = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let graph = Graph2::from_layer(layer);

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

        let query = Query::new().out("default").out("default").channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();
        let layer = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64)],
            0,
            100,
        )
        .unwrap();

        let graph = Graph2::from_layer(layer);

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
        let query = Query::new().out("default").out("default").channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();
        let layer = TempColGraphFragment::from_edges(
            graph_dir.path(),
            vec![(0, 0u64, 1, 3f64), (1, 1, 2, 3f64), (2, 1, 3, 3f64)],
            0,
            100,
        )
        .unwrap();

        let graph = Graph2::from_layer(layer);

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

    #[test]
    fn hop_twice_forward_time() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out("default").out("default").channel([sender]);

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
        let mut layer = TempColGraphFragment::from_edges(graph_dir.path(), edges, 0, 100).unwrap();
        layer
            .node_additions(100)
            .expect("Failed to load node additions");

        let graph = Graph2::from_layer(layer);

        let t = 10;
        let result = rayon2::execute::<ForwardState>(
            query,
            NodeSource::NodeIds(vec![VID(0), VID(1)]),
            &graph,
            |n| ForwardState::at_time(n, t, 100),
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
