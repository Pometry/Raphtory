use pometry_storage::{edge::Edge, GID};
use std::sync::Arc;

use crate::{
    core::entities::{nodes::node_ref::NodeRef, VID},
    db::api::view::StaticGraphViewOps,
};

use self::state::HopState;
use crate::core::storage::timeindex::TimeIndexOps;

use super::graph_impl::DiskGraphStorage;
use pometry_storage::nodes::Node;

pub mod ast;
pub mod executors;
pub mod state;

#[derive(Clone)]
pub enum NodeSource {
    All,
    NodeIds(Vec<VID>),
    ExternalIds(Vec<GID>),
    Filter(Arc<dyn Fn(VID, &DiskGraphStorage) -> bool + Send + Sync>),
}

impl NodeSource {
    fn into_iter(self, graph: &DiskGraphStorage) -> Box<dyn Iterator<Item = VID> + '_> {
        match self {
            NodeSource::All => Box::new((0..graph.inner.num_nodes()).map(VID)),
            NodeSource::NodeIds(ids) => Box::new(ids.into_iter()),
            NodeSource::Filter(filter) => Box::new(
                graph
                    .inner
                    .all_nodes()
                    .filter(move |node| filter(Into::<VID>::into(*node), graph)),
            ),
            NodeSource::ExternalIds(ext_ids) => Box::new(
                ext_ids
                    .into_iter()
                    .filter_map(move |gid| graph.inner.find_node(&gid)),
            ),
        }
    }

    fn into_iter_static_g<G: StaticGraphViewOps>(self, graph: G) -> Box<dyn Iterator<Item = VID>> {
        match self {
            NodeSource::All => Box::new(graph.nodes().iter_refs()),
            NodeSource::NodeIds(ids) => Box::new(ids.into_iter()),
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
                    .map(|node| node.node),
            ),
            NodeSource::Filter(_) => todo!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq, serde::Serialize, serde::Deserialize)]
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
        let ts = edge.timestamps::<i64>();
        let w = self.time + 1..i64::MAX;
        let next_time = ts.range_t(w);

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

    use crate::prelude::{AdditionOps, Graph, NO_PROPS};

    use super::{ast::*, executors::*, state::*, *};

    #[test]
    fn one_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new().out("_default").channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let g = Graph::new();

        g.add_edge(0, 0u64, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1u64, 2, NO_PROPS, None).unwrap();

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

        let result = rayon2::execute::<NoState>(query, NodeSource::All, &graph, |_| NoState::new());
        assert!(result.is_ok());

        let mut actual = receiver.into_iter().collect::<Vec<_>>();
        actual.sort_by_key(|(_, vid)| *vid);
        assert_eq!(actual, vec![(NoState, VID(1)), (NoState, VID(2))]);
    }

    #[test]
    fn two_hop_query() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new()
            .out("_default")
            .out("_default")
            .channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let g = Graph::new();

        g.add_edge(0, 0u64, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1u64, 2, NO_PROPS, None).unwrap();

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

        let result = rayon2::execute::<NoState>(query, NodeSource::All, &graph, |_| NoState::new());
        assert!(result.is_ok());
        let (_, vid) = receiver.recv().unwrap();
        assert_eq!(vid, VID(2));
    }

    #[test]
    fn two_hop_query_var() {
        let (sender, receiver) = std::sync::mpsc::channel();

        let query = Query::new()
            .out("_default")
            .out_var("_default")
            .channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let g = Graph::new();

        g.add_edge(0, 0u64, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1u64, 2, NO_PROPS, None).unwrap();

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

        let result = rayon2::execute::<VecState>(query, NodeSource::All, &graph, VecState::new);
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
        let query = Query::new()
            .out("_default")
            .out("_default")
            .channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let g = Graph::new();

        g.add_edge(0, 0u64, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1u64, 2, NO_PROPS, None).unwrap();

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

        let result = rayon2::execute::<VecState>(query, NodeSource::All, &graph, VecState::new);
        assert!(result.is_ok());
        let (path, vid) = receiver.recv().unwrap();
        assert_eq!(vid, VID(2));
        assert_eq!(path.0, vec![VID(0), VID(1), VID(2)]);
    }

    #[test]
    fn test_fork_2_hop() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let query = Query::new()
            .out("_default")
            .out("_default")
            .channel([sender]);

        let graph_dir = tempfile::tempdir().unwrap();

        let g = Graph::new();

        g.add_edge(0, 0u64, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 1u64, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 1u64, 3, NO_PROPS, None).unwrap();

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

        let result = rayon2::execute::<VecState>(query, NodeSource::All, &graph, VecState::new);
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

        let query = Query::new()
            .out("_default")
            .out("_default")
            .channel([sender]);

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

        let g = Graph::new();

        for (t, src, dst, _) in edges.iter() {
            g.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let graph = DiskGraphStorage::from_graph(&g, graph_dir.path()).unwrap();

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
