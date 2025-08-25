use crate::{
    LocalPOS,
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    error::StorageError,
    pages::{edge_store::ReadLockedEdgeStorage, node_store::ReadLockedNodeStorage},
    persist::strategy::PersistentStrategy,
    properties::props_meta_writer::PropsMetaWriter,
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
};
use edge_page::writer::EdgeWriter;
use edge_store::EdgeStorageInner;
use node_page::writer::{NodeWriter, WriterPair};
use node_store::NodeStorageInner;
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::properties::{meta::Meta, prop::Prop},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{EID, ELID, VID},
    storage::timeindex::TimeIndexEntry,
    utils::time::{InputTime, TryIntoInputTime},
};
use serde::{Deserialize, Serialize};
use session::WriteSession;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};

pub mod edge_page;
pub mod edge_store;
pub mod flush_thread;
pub mod layer_counter;
pub mod locked;
pub mod node_page;
pub mod node_store;
pub mod session;
#[cfg(feature = "test-utils")]
pub mod test_utils;

// graph // (node/edges) // segment // layer_ids (0, 1, 2, ...) // actual graphy bits

#[derive(Debug)]
pub struct GraphStore<NS, ES, EXT> {
    nodes: Arc<NodeStorageInner<NS, EXT>>,
    // node_flush_thread: FlushThread,
    edges: Arc<EdgeStorageInner<ES, EXT>>,
    event_id: AtomicUsize,
    _ext: EXT,
}

#[derive(Debug)]
pub struct ReadLockedGraphStore<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    EXT,
> {
    pub nodes: Arc<ReadLockedNodeStorage<NS, EXT>>,
    pub edges: Arc<ReadLockedEdgeStorage<ES, EXT>>,
    pub graph: Arc<GraphStore<NS, ES, EXT>>,
}

impl<
    NS: NodeSegmentOps<Extension = EXT>,
    ES: EdgeSegmentOps<Extension = EXT>,
    EXT: PersistentStrategy,
> GraphStore<NS, ES, EXT>
{
    pub fn read_locked(self: &Arc<Self>) -> ReadLockedGraphStore<NS, ES, EXT> {
        let nodes = self.nodes.locked().into();
        let edges = self.edges.locked().into();
        ReadLockedGraphStore {
            nodes,
            edges,
            graph: self.clone(),
        }
    }

    pub fn nodes(&self) -> &Arc<NodeStorageInner<NS, EXT>> {
        &self.nodes
    }

    pub fn edges(&self) -> &Arc<EdgeStorageInner<ES, EXT>> {
        &self.edges
    }

    pub fn edge_meta(&self) -> &Meta {
        self.edges.edge_meta()
    }

    pub fn set_event_id(&self, event_id: usize) {
        self.event_id.store(event_id, atomic::Ordering::Relaxed);
    }

    pub fn node_meta(&self) -> &Meta {
        self.nodes.node_meta()
    }

    pub fn earliest(&self) -> i64 {
        self.nodes
            .stats()
            .earliest()
            .min(self.edges.stats().earliest())
    }

    pub fn latest(&self) -> i64 {
        self.nodes.stats().latest().max(self.edges.stats().latest())
    }

    pub fn load(graph_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let nodes_path = graph_dir.as_ref().join("nodes");
        let edges_path = graph_dir.as_ref().join("edges");

        let GraphMeta {
            max_page_len_nodes,
            max_page_len_edges,
        } = read_graph_meta(graph_dir.as_ref())?;

        let ext = EXT::default();

        let edges = Arc::new(EdgeStorageInner::load(
            edges_path,
            max_page_len_edges,
            ext.clone(),
        )?);

        let edge_meta = edges.edge_meta().clone();

        let nodes = Arc::new(NodeStorageInner::load(
            nodes_path,
            max_page_len_nodes,
            edge_meta,
            ext.clone(),
        )?);

        let t_len = edges.t_len();

        Ok(Self {
            // node_flush_thread: FlushThread::new::<_, ES, _>(nodes.clone()),
            nodes,
            edges,
            event_id: AtomicUsize::new(t_len),
            _ext: ext,
        })
    }

    pub fn new_with_meta(
        graph_dir: impl AsRef<Path>,
        max_page_len_nodes: usize,
        max_page_len_edges: usize,
        node_meta: Meta,
        edge_meta: Meta,
    ) -> Self {
        let nodes_path = graph_dir.as_ref().join("nodes");
        let edges_path = graph_dir.as_ref().join("edges");
        let ext = EXT::default();

        let node_meta = Arc::new(node_meta);
        let edge_meta = Arc::new(edge_meta);

        let nodes = Arc::new(NodeStorageInner::new_with_meta(
            nodes_path,
            max_page_len_nodes,
            node_meta,
            edge_meta.clone(),
            ext.clone(),
        ));
        let edges = Arc::new(EdgeStorageInner::new_with_meta(
            edges_path,
            max_page_len_edges,
            edge_meta,
            ext.clone(),
        ));

        let graph_meta = GraphMeta {
            max_page_len_nodes,
            max_page_len_edges,
        };

        write_graph_meta(&graph_dir, graph_meta)
            .expect("Unrecoverable! Failed to write graph meta");

        Self {
            // node_flush_thread: FlushThread::new::<_, ES, _>(nodes.clone()),
            nodes,
            edges,
            event_id: AtomicUsize::new(0),
            _ext: ext,
        }
    }

    pub fn new(
        graph_dir: impl AsRef<Path>,
        max_page_len_nodes: usize,
        max_page_len_edges: usize,
    ) -> Self {
        Self::new_with_meta(
            graph_dir,
            max_page_len_nodes,
            max_page_len_edges,
            Meta::new_for_nodes(),
            Meta::new_for_edges(),
        )
    }

    pub fn add_edge<T: TryIntoInputTime>(
        &self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
    ) -> Result<MaybeNew<ELID>, StorageError> {
        let t = self.as_time_index_entry(t)?;
        self.internal_add_edge(t, src, dst, 0, [])
    }

    pub(crate) fn add_edge_props<PN: AsRef<str>, T: TryIntoInputTime>(
        &self,
        t: T,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        props: Vec<(PN, Prop)>,
        _lsn: u64,
    ) -> Result<MaybeNew<ELID>, StorageError> {
        let t = self.as_time_index_entry(t)?;
        let prop_writer = PropsMetaWriter::temporal(self.edge_meta(), props.into_iter())?;
        self.internal_add_edge(t, src, dst, 0, prop_writer.into_props_temporal()?)
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<MaybeNew<ELID>, StorageError> {
        let src = src.into();
        let dst = dst.into();
        let mut session = self.write_session(src, dst, None);
        let elid = session
            .add_static_edge(src, dst, lsn)
            .map(|eid| eid.with_layer(0));
        session.add_edge_into_layer(t, src, dst, elid, lsn, props);
        Ok(elid)
    }

    fn as_time_index_entry<T: TryIntoInputTime>(
        &self,
        t: T,
    ) -> Result<TimeIndexEntry, StorageError> {
        let input_time = t.try_into_input_time()?;
        let t = match input_time {
            InputTime::Indexed(t, i) => TimeIndexEntry::new(t, i),
            InputTime::Simple(t) => {
                let i = self.event_id.fetch_add(1, atomic::Ordering::Relaxed);
                TimeIndexEntry::new(t, i)
            }
        };
        Ok(t)
    }

    pub fn read_event_id(&self) -> usize {
        self.event_id.load(atomic::Ordering::Relaxed)
    }

    pub fn next_event_id(&self) -> usize {
        self.event_id.fetch_add(1, atomic::Ordering::Relaxed)
    }

    pub fn update_edge_const_props<PN: AsRef<str>>(
        &self,
        eid: impl Into<ELID>,
        props: Vec<(PN, Prop)>,
    ) -> Result<(), StorageError> {
        let eid = eid.into();
        let layer = eid.layer();
        let (_, edge_pos) = self.edges.resolve_pos(eid.edge);
        let mut edge_writer = self.edges.try_get_writer(eid.edge)?;
        let (src, dst) = edge_writer
            .get_edge(layer, edge_pos)
            .expect("Internal Error, EID should be checked at this point!");
        let prop_writer = PropsMetaWriter::constant(self.edge_meta(), props.into_iter())?;

        edge_writer.update_c_props(edge_pos, src, dst, layer, prop_writer.into_props_const()?);

        Ok(())
    }

    pub fn update_node_const_props<PN: AsRef<str>>(
        &self,
        node: impl Into<VID>,
        layer_id: usize,
        props: Vec<(PN, Prop)>,
    ) -> Result<(), StorageError> {
        let node = node.into();
        let (segment, node_pos) = self.nodes.resolve_pos(node);
        let mut node_writer = self.nodes.writer(segment);
        let prop_writer = PropsMetaWriter::constant(self.node_meta(), props.into_iter())?;
        node_writer.update_c_props(node_pos, layer_id, prop_writer.into_props_const()?, 0); // TODO: LSN
        Ok(())
    }

    pub fn add_node_props<PN: AsRef<str>>(
        &self,
        t: impl TryIntoInputTime,
        node: impl Into<VID>,
        layer_id: usize,
        props: Vec<(PN, Prop)>,
    ) -> Result<(), StorageError> {
        let node = node.into();
        let (segment, node_pos) = self.nodes.resolve_pos(node);

        let t = self.as_time_index_entry(t)?;

        let mut node_writer = self.nodes.writer(segment);
        let prop_writer = PropsMetaWriter::temporal(self.node_meta(), props.into_iter())?;
        node_writer.add_props(t, node_pos, layer_id, prop_writer.into_props_temporal()?, 0); // TODO: LSN
        Ok(())
    }

    pub fn write_session(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
    ) -> WriteSession<'_, NS, ES, EXT> {
        let (src_chunk, _) = self.nodes.resolve_pos(src);
        let (dst_chunk, _) = self.nodes.resolve_pos(dst);

        let node_writers = if src_chunk < dst_chunk {
            let src_writer = self.node_writer(src_chunk);
            let dst_writer = self.node_writer(dst_chunk);
            WriterPair::Different {
                src_writer,
                dst_writer,
            }
        } else if src_chunk > dst_chunk {
            let dst_writer = self.node_writer(dst_chunk);
            let src_writer = self.node_writer(src_chunk);
            WriterPair::Different {
                src_writer,
                dst_writer,
            }
        } else {
            let writer = self.node_writer(src_chunk);
            WriterPair::Same { writer }
        };

        let edge_writer = e_id.map(|e_id| self.edge_writer(e_id));

        WriteSession::new(node_writers, edge_writer, self)
    }

    pub fn node_writer(
        &self,
        node_segment: usize,
    ) -> NodeWriter<'_, RwLockWriteGuard<'_, MemNodeSegment>, NS> {
        self.nodes().writer(node_segment)
    }

    pub fn edge_writer(
        &self,
        eid: EID,
    ) -> EdgeWriter<'_, RwLockWriteGuard<'_, MemEdgeSegment>, ES> {
        self.edges().get_writer(eid)
    }

    pub fn get_free_writer(&self) -> EdgeWriter<'_, RwLockWriteGuard<'_, MemEdgeSegment>, ES> {
        self.edges().get_free_writer()
    }
}

fn write_graph_meta(
    graph_dir: impl AsRef<Path>,
    graph_meta: GraphMeta,
) -> Result<(), StorageError> {
    let meta_file = graph_dir.as_ref().join("graph_meta.json");
    let meta_file = std::fs::File::create(meta_file).unwrap();
    serde_json::to_writer_pretty(meta_file, &graph_meta)?;
    Ok(())
}

fn read_graph_meta(graph_dir: impl AsRef<Path>) -> Result<GraphMeta, StorageError> {
    let meta_file = graph_dir.as_ref().join("graph_meta.json");
    let meta_file = std::fs::File::open(meta_file).unwrap();
    let meta = serde_json::from_reader(meta_file)?;
    Ok(meta)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct GraphMeta {
    max_page_len_nodes: usize,
    max_page_len_edges: usize,
}

#[inline(always)]
pub fn resolve_pos<I: Copy + Into<usize>>(i: I, max_page_len: usize) -> (usize, LocalPOS) {
    let chunk = i.into() / max_page_len;
    let pos = i.into() % max_page_len;
    (chunk, pos.into())
}

#[cfg(test)]
mod test {
    use super::GraphStore;
    use crate::{
        Extension, Layer,
        api::nodes::{NodeEntryOps, NodeRefOps},
        pages::test_utils::{
            AddEdge, Fixture, NodeFixture, check_edges_support, check_graph_with_nodes_support,
            check_graph_with_props_support, edges_strat, edges_strat_with_layers, make_edges,
            make_nodes,
        },
    };
    use chrono::{DateTime, NaiveDateTime, Utc};
    use core::panic;
    use proptest::prelude::*;
    use raphtory_api::core::entities::properties::prop::Prop;
    use raphtory_core::{entities::VID, storage::timeindex::TimeIndexOps};

    fn check_edges(
        edges: Vec<(impl Into<VID>, impl Into<VID>)>,
        chunk_size: usize,
        par_load: bool,
    ) {
        // Set optional layer_id to None
        let layer_id = None;
        let edges = edges
            .into_iter()
            .map(|(src, dst)| (src, dst, layer_id))
            .collect();

        check_edges_support(edges, par_load, false, |graph_dir| {
            Layer::<crate::Extension>::new(graph_dir, chunk_size, chunk_size)
        })
    }

    fn check_edges_with_layers(
        edges: Vec<(impl Into<VID>, impl Into<VID>, Option<usize>)>, // src, dst, layer_id
        chunk_size: usize,
        par_load: bool,
    ) {
        check_edges_support(edges, par_load, false, |graph_dir| {
            Layer::<crate::Extension>::new(graph_dir, chunk_size, chunk_size)
        })
    }

    #[test]
    fn test_storage() {
        let edges_strat = edges_strat(10);
        proptest!(|(edges in edges_strat, chunk_size in 1usize .. 100)|{
            check_edges(edges, chunk_size, false);
        });
    }

    #[test]
    fn test_storage_par() {
        let edges_strat = edges_strat(15);
        proptest!(|(edges in edges_strat, chunk_size in 1usize..100)|{
            check_edges(edges, chunk_size, true);
        });
    }

    #[test]
    fn test_storage_par_1024_x2() {
        let edges_strat = edges_strat(50);
        proptest!(|(edges in edges_strat, chunk_size in 1usize..100)|{
            check_edges(edges, chunk_size, true);
        });
    }

    #[test]
    fn test_storage_par_1024() {
        let edges_strat = edges_strat(50);
        proptest!(|(edges in edges_strat, chunk_size in 2usize..100)|{
            check_edges(edges, chunk_size, false);
        });
    }

    #[test]
    fn test_storage_issue1() {
        let edges = vec![(0, 1), (1, 0), (0, 0)];
        check_edges(edges, 2, false);
    }

    #[test]
    fn test_storage_empty() {
        let edges = Vec::<(VID, VID)>::new();
        check_edges(edges, 32, false);
    }

    #[test]
    fn test_one_edge() {
        let edges = vec![(2, 2)];
        check_edges(edges, 2, false);
    }

    #[test]
    fn test_storage_with_layers() {
        let edges_strat = edges_strat_with_layers(10);

        proptest!(|(edges in edges_strat, chunk_size in 1usize .. 100)|{
            check_edges_with_layers(edges, chunk_size, false);
        });
    }

    #[test]
    fn test_storage_with_layers_1() {
        let edges = vec![(VID(4), VID(0), Some(1)), (VID(0), VID(0), Some(6))];
        check_edges_with_layers(edges, 4, false);
    }

    #[test]
    fn test_add_one_edge_get_num_nodes() {
        let graph_dir = tempfile::tempdir().unwrap();
        let g = Layer::<Extension>::new(graph_dir.path(), 32, 32);
        g.add_edge(4, 7, 3).unwrap();
        assert_eq!(g.nodes().num_nodes(), 2);
    }

    #[test]
    fn test_node_additions_1() {
        let graph_dir = tempfile::tempdir().unwrap();
        let g = GraphStore::new(graph_dir.path(), 32, 32);
        g.add_edge(4, 7, 3).unwrap();

        let check = |g: &Layer<()>| {
            assert_eq!(g.nodes().num_nodes(), 2);

            let node = g.nodes().node(3);
            let node_entry = node.as_ref();
            let actual: Vec<_> = node_entry.edge_additions(0).iter_t().collect();
            assert_eq!(actual, vec![4]);
        };

        check(&g);
    }

    #[test]
    fn test_one_edge_par() {
        let edges = vec![(2, 2)];
        check_edges(edges, 2, true);
    }

    #[test]
    fn test_multiple_edges_par() {
        let edges = vec![(2, 2), (2, 3), (3, 2), (3, 3), (3, 4), (4, 3)];
        check_edges(edges, 2, false);
    }

    #[test]
    fn test_multiple_edges_par_x2() {
        let edges = vec![(2, 2), (2, 3), (3, 2), (3, 3), (3, 4), (4, 3)];
        check_edges(edges, 2, true);
    }

    #[test]
    fn some_edges() {
        let edges = vec![(1, 1), (0, 0), (1, 0), (1, 1)];
        check_edges(edges, 89, false);
    }

    #[test]
    fn node_temporal_props() {
        let graph_dir = tempfile::tempdir().unwrap();
        let g = Layer::<Extension>::new(graph_dir.path(), 32, 32);
        g.add_node_props::<String>(1, 0, 0, vec![])
            .expect("Failed to add node props");
        g.add_node_props::<String>(2, 0, 0, vec![])
            .expect("Failed to add node props");
        g.add_node_props::<String>(3, 0, 0, vec![])
            .expect("Failed to add node props");
        g.add_node_props::<String>(4, 0, 0, vec![])
            .expect("Failed to add node props");
        g.add_node_props::<String>(8, 0, 0, vec![])
            .expect("Failed to add node props");

        let node = g.nodes().node(0);

        let edge_ts = node.as_ref().edge_additions(0);
        assert!(edge_ts.iter_t().collect::<Vec<_>>().is_empty());
        let node_ts = node.as_ref().node_additions(0);
        assert_eq!(node_ts.iter_t().collect::<Vec<_>>(), vec![1, 2, 3, 4, 8]);

        let edge_ts = edge_ts.range_t(1..8);
        assert!(edge_ts.iter_t().collect::<Vec<_>>().is_empty());
        let node_ts = node_ts.range_t(1..8);
        assert_eq!(node_ts.iter_t().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn add_one_edge_with_props() {
        let edges = make_edges(1, 1);
        proptest!(|(edges in edges, node_page_len in 1usize..100, edge_page_len in 1usize .. 100)|{
            check_graph_with_props(node_page_len, edge_page_len, &edges);
        });
    }

    #[test]
    fn add_one_edge_with_decimal() {
        let edges = vec![(
            VID(0),
            VID(0),
            0,
            vec![
                (
                    "957".to_owned(),
                    Prop::DTime(DateTime::from_timestamp_millis(0).unwrap()),
                ),
                ("920".to_owned(), Prop::I32(0)),
            ],
            vec![
                ("920".to_owned(), Prop::I32(0)),
                (
                    "957".to_owned(),
                    Prop::DTime(DateTime::from_timestamp_millis(0).unwrap()),
                ),
            ],
            Some("b"),
        )];
        check_graph_with_props(89, 1, &edges.into());
    }

    #[test]
    fn add_one_edge_with_time_props_and_decimal() {
        let edges: Vec<AddEdge> = vec![(
            VID(0),
            VID(0),
            0,
            vec![
                (
                    "767".to_owned(),
                    Prop::DTime(DateTime::from_timestamp_millis(-2208988800000).unwrap()),
                ),
                ("123".to_owned(), Prop::Decimal(123425879.into())),
            ],
            vec![
                (
                    "140".to_owned(),
                    Prop::NDTime(
                        DateTime::from_timestamp_millis(-2208988800001)
                            .unwrap()
                            .naive_utc(),
                    ),
                ),
                ("321".to_owned(), Prop::Decimal(7654321.into())),
            ],
            Some("b"),
        )];

        check_graph_with_props(31, 50, &edges.into());
    }

    #[test]
    fn add_one_node_with_props() {
        let nodes = make_nodes(1);
        proptest!(|(nodes in nodes, node_page_len in 1usize..100, edge_page_len in 1usize .. 100)|{
            check_graph_with_nodes(node_page_len, edge_page_len, &nodes);
        });
    }

    #[test]
    fn add_multiple_node_with_props() {
        let nodes = make_nodes(20);
        proptest!(|(nodes in nodes, node_page_len in 1usize..100, edge_page_len in 1usize .. 100)|{
            check_graph_with_nodes(node_page_len, edge_page_len, &nodes);
        });
    }

    #[test]
    fn add_multiple_edges_with_props_14() {
        let node_fixture = NodeFixture {
            temp_props: vec![
                (VID(0), 0, vec![]),
                (VID(1), 1, vec![]),
                (VID(0), 2, vec![]),
            ],
            const_props: vec![(VID(0), vec![])],
        };

        check_graph_with_nodes(13, 13, &node_fixture);
    }

    #[test]
    fn add_multiple_node_with_props_4() {
        let node_fixture = NodeFixture {
            temp_props: vec![(VID(0), 0, vec![])],
            const_props: vec![(
                VID(0),
                vec![
                    ("399".to_owned(), Prop::I64(498)),
                    ("831".to_owned(), Prop::str("898")),
                    ("857".to_owned(), Prop::F64(2.56)),
                    (
                        "296".to_owned(),
                        Prop::NDTime(NaiveDateTime::from_timestamp(1334043671, 0)),
                    ),
                    (
                        "92".to_owned(),
                        Prop::DTime(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(994032315, 0),
                            Utc,
                        )),
                    ),
                ],
            )],
        };

        check_graph_with_nodes(90, 60, &node_fixture);
    }

    #[test]
    fn add_multiple_node_with_props_3() {
        let node_fixture = NodeFixture {
            temp_props: vec![
                (VID(0), 0, vec![]),
                (VID(0), 0, vec![]),
                (VID(0), 0, vec![]),
                (VID(0), 0, vec![]),
                (VID(0), 0, vec![]),
                (VID(0), 0, vec![]),
            ],
            const_props: vec![(VID(0), vec![]), (VID(0), vec![]), (VID(0), vec![])],
        };
        check_graph_with_nodes(1, 1, &node_fixture);
    }

    #[test]
    fn add_multiple_node_with_props_1() {
        let node_fixture = NodeFixture {
            temp_props: vec![(VID(0), 0, vec![])],
            const_props: vec![
                (VID(0), vec![]),
                (VID(8), vec![("422".to_owned(), Prop::U8(0))]),
                (VID(8), vec![("423".to_owned(), Prop::U8(30))]),
            ],
        };
        check_graph_with_nodes(43, 94, &node_fixture);
    }

    #[test]
    fn add_multiple_node_with_props_2() {
        let node_fixture = NodeFixture {
            temp_props: vec![(VID(0), 0, vec![])],
            const_props: vec![
                (
                    VID(0),
                    vec![
                        ("441".to_owned(), Prop::I64(-3856368215564042936)),
                        ("225".to_owned(), Prop::F64(-202423261.6280773)),
                        ("290".to_owned(), Prop::str("15")),
                        ("54".to_owned(), Prop::U8(226)),
                        ("953".to_owned(), Prop::Bool(false)),
                        ("771".to_owned(), Prop::I64(-6507648222238880768)),
                        ("955".to_owned(), Prop::Bool(true)),
                        ("346".to_owned(), Prop::F64(-1.608025857001021e-308)),
                    ],
                ),
                (VID(1), vec![("953".to_owned(), Prop::Bool(false))]),
                (VID(1), vec![]),
            ],
        };
        check_graph_with_nodes(8, 57, &node_fixture);
    }

    #[test]
    fn add_one_node_with_props_0() {
        let node_fixture = NodeFixture {
            temp_props: vec![(VID(0), 0, vec![])],
            const_props: vec![
                (
                    VID(1),
                    vec![("574".to_owned(), Prop::I64(-28802842553584714))],
                ),
                (
                    VID(1),
                    vec![
                        ("571".to_owned(), Prop::U8(30)),
                        ("618".to_owned(), Prop::Bool(true)),
                        ("431".to_owned(), Prop::F64(-2.7522071060615837e-76)),
                        ("68".to_owned(), Prop::F64(-2.32248037343811e44)),
                        ("620".to_owned(), Prop::I64(1574788428164567343)),
                    ],
                ),
            ],
        };

        check_graph_with_nodes(85, 34, &node_fixture);
    }

    #[test]
    fn add_one_node_with_props_1() {
        let node_fixture = NodeFixture {
            temp_props: vec![(
                VID(1),
                2,
                vec![
                    ("611".to_owned(), Prop::U8(25)),
                    ("590".to_owned(), Prop::str("294")),
                    ("63".to_owned(), Prop::Bool(true)),
                    ("789".to_owned(), Prop::I64(-245071354050338754)),
                ],
            )],
            const_props: vec![(VID(1), vec![("801".to_owned(), Prop::U8(32))])],
        };

        check_graph_with_nodes(85, 34, &node_fixture);
    }

    #[test]
    fn add_one_edge_with_props_0() {
        let edges = vec![(
            VID(0),
            VID(0),
            0,
            vec![("1".to_owned(), Prop::str("0"))],
            vec![],
            Some("a"),
        )];
        check_graph_with_props(82, 82, &edges.into());
    }

    #[test]
    fn add_one_edge_with_props_1() {
        let edges = vec![(
            VID(0),
            VID(0),
            0,
            vec![],
            vec![("877".to_owned(), Prop::F64(0.0))],
            None,
        )];
        check_graph_with_props(82, 82, &edges.into());
    }

    #[test]
    fn add_one_edge_with_props_2() {
        let edges = vec![(
            VID(0),
            VID(0),
            0,
            vec![("0".to_owned(), Prop::str("0"))],
            vec![("1".to_owned(), Prop::str("0"))],
            Some("a"),
        )];
        check_graph_with_props(82, 82, &edges.into());
    }

    #[test]
    fn add_one_edge_with_props_3() {
        let edges = vec![(
            VID(0),
            VID(0),
            0,
            vec![("962".to_owned(), Prop::I64(0))],
            vec![("324".to_owned(), Prop::U8(0))],
            Some("a"),
        )];
        check_graph_with_props(98, 16, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props() {
        let edges = make_edges(20, 20);
        proptest!(|(edges in edges, node_page_len in 1usize..100, edge_page_len in 1usize .. 100)|{
            check_graph_with_props(node_page_len, edge_page_len, &edges);
        });
    }

    #[test]
    fn add_multiple_edges_with_props_13() {
        for _ in 0..10 {
            let edges = vec![
                (
                    VID(12),
                    VID(3),
                    64,
                    vec![("659".to_owned(), Prop::Bool(true))],
                    vec![
                        ("429".to_owned(), Prop::U8(13)),
                        ("991".to_owned(), Prop::F64(9.431610844495756)),
                        ("792".to_owned(), Prop::str("44")),
                    ],
                    Some("a"),
                ),
                (
                    VID(8),
                    VID(0),
                    45,
                    vec![
                        ("374".to_owned(), Prop::F64(-3.2891291943257276)),
                        ("659".to_owned(), Prop::Bool(true)),
                        ("649".to_owned(), Prop::U8(72)),
                        ("877".to_owned(), Prop::F64(5.505566002056544)),
                        ("561".to_owned(), Prop::str("289")),
                    ],
                    vec![
                        ("991".to_owned(), Prop::F64(4.4758924307224585)),
                        ("792".to_owned(), Prop::str("594")),
                    ],
                    None,
                ),
                (
                    VID(14),
                    VID(16),
                    30,
                    vec![
                        ("374".to_owned(), Prop::F64(-2.4044297575008132)),
                        ("561".to_owned(), Prop::str("964")),
                    ],
                    vec![
                        ("899".to_owned(), Prop::F64(4.491626971132711)),
                        ("868".to_owned(), Prop::Bool(true)),
                        ("962".to_owned(), Prop::I64(3133919197295275594)),
                        ("840".to_owned(), Prop::str("578")),
                    ],
                    None,
                ),
            ];
            check_graph_with_props(33, 39, &edges.into());
        }
    }

    #[test]
    fn add_multiple_edges_with_props_11() {
        let edges = vec![
            (
                VID(10),
                VID(7),
                63,
                vec![
                    ("649".to_owned(), Prop::U8(54)),
                    ("868".to_owned(), Prop::Bool(false)),
                    ("361".to_owned(), Prop::I64(6798507933589465750)),
                    ("561".to_owned(), Prop::str("800")),
                ],
                vec![("877".to_owned(), Prop::F64(-4.4595346573113036e-48))],
                Some("b"),
            ),
            (
                VID(7),
                VID(3),
                56,
                vec![],
                vec![
                    ("877".to_owned(), Prop::F64(-9.826757828363747e44)),
                    ("899".to_owned(), Prop::F64(1.6798428870674542e-256)),
                    ("991".to_owned(), Prop::F64(2.246204753092509e144)),
                    ("374".to_owned(), Prop::F64(1.1547300396496702e131)),
                ],
                Some("b"),
            ),
            (
                VID(9),
                VID(9),
                28,
                vec![],
                vec![
                    ("792".to_owned(), Prop::str("426")),
                    ("877".to_owned(), Prop::F64(-1.2304916849909104e-297)),
                    ("899".to_owned(), Prop::F64(2.8623367224991785e75)),
                    ("840".to_owned(), Prop::str("309")),
                    ("991".to_owned(), Prop::F64(-2.1336000912955556e-308)),
                    ("962".to_owned(), Prop::I64(-3475626455764953092)),
                    ("374".to_owned(), Prop::F64(-0.0)),
                ],
                Some("a"),
            ),
            (
                VID(4),
                VID(14),
                10,
                vec![
                    ("868".to_owned(), Prop::Bool(false)),
                    ("361".to_owned(), Prop::I64(-6751088942916859396)),
                ],
                vec![],
                Some("b"),
            ),
        ];

        check_graph_with_props(33, 69, &edges.into());
        // check_graph_with_props::<WriteAndMerge<4>>(33, 69, &edges.into()); different problem
    }

    #[test]
    fn add_multiple_edges_with_props_12() {
        let edges = vec![
            (VID(13), VID(11), 47, vec![], vec![], None),
            (
                VID(2),
                VID(10),
                61,
                vec![
                    ("991".to_owned(), Prop::F64(1.783602448650279e-300)),
                    ("361".to_owned(), Prop::I64(-6635533919809359722)),
                    ("659".to_owned(), Prop::Bool(false)),
                ],
                vec![
                    ("868".to_owned(), Prop::Bool(false)),
                    ("561".to_owned(), Prop::str("443")),
                ],
                None,
            ),
            (
                VID(16),
                VID(7),
                63,
                vec![("962".to_owned(), Prop::I64(-5795311055328182913))],
                vec![
                    ("429".to_owned(), Prop::U8(173)),
                    ("561".to_owned(), Prop::str("821")),
                    ("649".to_owned(), Prop::U8(177)),
                ],
                Some("a"),
            ),
            (
                VID(16),
                VID(6),
                56,
                vec![
                    ("792".to_owned(), Prop::str("551")),
                    ("962".to_owned(), Prop::I64(123378859162979696)),
                    ("361".to_owned(), Prop::I64(-324898360063869285)),
                    ("659".to_owned(), Prop::Bool(true)),
                ],
                vec![],
                None,
            ),
        ];
        check_graph_with_props(24, 31, &edges.into());
    }

    // #[test]
    // #[ignore = "Time index entry can be overwritten"]
    // fn add_multiple_edges_with_props_9() {
    //     let graph_dir = tempfile::tempdir().unwrap();
    //     let gs = Layer::new(graph_dir.path(), 32, 32);

    //     gs.internal_add_edge(TimeIndexEntry(1, 0), 0, 0, 0, vec![("a", Prop::str("b"))])
    //         .unwrap();
    //     gs.internal_add_edge(TimeIndexEntry(1, 0), 0, 0, 0, vec![("c", Prop::str("d"))])
    //         .unwrap();

    //     let edge = gs.edges().edge(0);
    //     let props = edge.as_ref().t_prop(0).iter().collect::<Vec<_>>();
    //     assert_eq!(props, vec![(TimeIndexEntry(1, 0), Prop::str("b")),]);
    //     let props = edge.as_ref().t_prop(1).iter().collect::<Vec<_>>();
    //     assert_eq!(props, vec![(TimeIndexEntry(1, 0), Prop::str("d")),]);
    // }

    // #[test]
    // #[ignore = "Time index entry can be overwritten"]
    // fn add_multiple_edges_with_props_10() {
    //     let graph_dir = tempfile::tempdir().unwrap();
    //     let gs = GraphStore::<WriteAndMerge<16>>::new(graph_dir.path(), 32, 32);

    //     gs.add_edge_props(TimeIndexEntry(1, 0), 0, 0, vec![("a", Prop::str("b"))], 0)
    //         .unwrap();
    //     gs.add_edge_props(TimeIndexEntry(1, 0), 0, 0, vec![("a", Prop::str("d"))], 0)
    //         .unwrap();

    //     let edge = gs.edges().edge(0);
    //     let props = edge.as_ref().t_prop(0).iter().collect::<Vec<_>>();
    //     assert_eq!(
    //         props,
    //         vec![
    //             (TimeIndexEntry(1, 0), Prop::str("b")),
    //             (TimeIndexEntry(1, 0), Prop::str("d"))
    //         ]
    //     );
    // }

    #[test]
    fn add_multiple_edges_with_props_8() {
        let edges = vec![
            (VID(7), VID(8), 0, vec![], vec![], Some("a")),
            (VID(0), VID(0), 0, vec![], vec![], Some("a")),
            (VID(1), VID(0), 0, vec![], vec![], Some("a")),
            (VID(7), VID(8), 66, vec![], vec![], Some("b")),
            (
                VID(7),
                VID(3),
                31,
                vec![("52".to_string(), Prop::U8(202))],
                vec![],
                None,
            ),
            (VID(4), VID(8), 40, vec![], vec![], Some("a")),
            (
                VID(3),
                VID(10),
                9,
                vec![("52".to_string(), Prop::U8(169))],
                vec![],
                None,
            ),
            (
                VID(13),
                VID(4),
                3,
                vec![("52".to_string(), Prop::U8(72))],
                vec![],
                Some("a"),
            ),
            (
                VID(2),
                VID(4),
                9,
                vec![("52".to_string(), Prop::U8(131))],
                vec![],
                Some("b"),
            ),
            (
                VID(2),
                VID(1),
                47,
                vec![("52".to_string(), Prop::U8(55))],
                vec![],
                Some("a"),
            ),
            (
                VID(14),
                VID(3),
                13,
                vec![("52".to_string(), Prop::U8(70))],
                vec![],
                None,
            ),
            (
                VID(8),
                VID(10),
                11,
                vec![("52".to_string(), Prop::U8(47))],
                vec![],
                Some("b"),
            ),
        ];

        check_graph_with_props(88, 83, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_7() {
        let edges = vec![
            (VID(0), VID(0), 1, vec![], vec![], Some("a")),
            (VID(0), VID(1), 2, vec![], vec![], Some("a")),
            (VID(3), VID(3), 3, vec![], vec![], Some("a")),
            (
                VID(3),
                VID(3),
                4,
                vec![("9".to_string(), Prop::I64(0))],
                vec![],
                Some("a"),
            ),
        ];
        check_graph_with_props(90, 2, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_6() {
        let edges = vec![
            (VID(5), VID(6), 0, vec![], vec![], Some("a")),
            (VID(0), VID(0), 0, vec![], vec![], Some("a")),
            (VID(0), VID(1), 0, vec![], vec![], Some("a")),
            (VID(1), VID(0), 0, vec![], vec![], Some("a")),
            (VID(4), VID(7), 0, vec![], vec![], Some("a")),
            (VID(4), VID(7), 0, vec![], vec![], Some("a")),
            (
                VID(5),
                VID(6),
                1,
                vec![("100".to_string(), Prop::Bool(false))],
                vec![],
                Some("a"),
            ),
        ];
        check_graph_with_props(10, 19, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_5() {
        let edges = vec![
            (VID(2), VID(0), 0, vec![], vec![], Some("a")),
            (
                VID(0),
                VID(0),
                0,
                vec![("382".to_string(), Prop::U8(90))],
                vec![],
                Some("a"),
            ),
            (
                VID(3),
                VID(1),
                3,
                vec![("382".to_string(), Prop::U8(227))],
                vec![],
                Some("a"),
            ),
            (VID(2), VID(2), 18, vec![], vec![], None),
            (
                VID(0),
                VID(2),
                15,
                vec![("195".to_string(), Prop::Bool(false))],
                vec![],
                Some("b"),
            ),
            (
                VID(0),
                VID(2),
                12,
                vec![
                    ("287".to_string(), Prop::I64(-5621124784932591697)),
                    ("382".to_string(), Prop::U8(95)),
                ],
                vec![],
                None,
            ),
        ];
        check_graph_with_props(10, 10, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_3() {
        let edges = vec![
            (
                VID(0),
                VID(0),
                0,
                vec![("419".to_string(), Prop::F64(6.839180078867341e80))],
                vec![],
                Some("b"),
            ),
            (
                VID(0),
                VID(0),
                3,
                vec![],
                vec![("419".to_string(), Prop::F64(-0.0))],
                None,
            ),
            (VID(0), VID(0), 4, Vec::new(), Vec::new(), None),
            (VID(0), VID(0), 0, Vec::new(), Vec::new(), Some("b")),
            (
                VID(0),
                VID(0),
                4,
                Vec::new(),
                vec![("419".to_string(), Prop::F64(1.0562500054688134e-99))],
                Some("b"),
            ),
        ];
        check_graph_with_props(43, 86, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_4() {
        let edges = vec![
            (
                VID(0),
                VID(0),
                2,
                vec![("419".to_string(), Prop::F64(0.0))],
                vec![("533".to_string(), Prop::F64(7.22))],
                Some("a"),
            ),
            (
                VID(0),
                VID(0),
                2,
                vec![("419".to_string(), Prop::F64(-4.522))],
                vec![],
                Some("b"),
            ),
        ];
        check_graph_with_props(5, 5, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_2() {
        let edges: Vec<AddEdge> = vec![
            (
                VID(1),
                VID(0),
                5,
                vec![("195".to_string(), Prop::Bool(false))],
                Vec::new(),
                Some("b"),
            ),
            (
                VID(1),
                VID(0),
                16,
                vec![
                    ("921".to_string(), Prop::U8(41)),
                    ("195".to_string(), Prop::Bool(true)),
                    ("287".to_string(), Prop::I64(6720004553605012498)),
                ],
                Vec::new(),
                Some("a"),
            ),
            (
                VID(3),
                VID(1),
                3,
                vec![("287".to_string(), Prop::I64(846481219119638755))],
                Vec::new(),
                Some("a"),
            ),
            (VID(2), VID(2), 18, Vec::new(), Vec::new(), None),
            (
                VID(0),
                VID(2),
                15,
                vec![("921".to_string(), Prop::U8(109))],
                Vec::new(),
                Some("b"),
            ),
            (
                VID(0),
                VID(2),
                12,
                vec![
                    ("195".to_string(), Prop::Bool(false)),
                    ("287".to_string(), Prop::I64(92928934764462282)),
                ],
                Vec::new(),
                None,
            ),
        ];
        check_graph_with_props(10, 10, &edges.into());
    }

    #[test]
    fn add_multiple_edges_with_props_1() {
        let edges = vec![
            (
                VID(0),
                VID(0),
                0i64,
                vec![("607".to_owned(), Prop::Bool(true))],
                vec![
                    ("688".to_owned(), Prop::str("791")),
                    ("59".to_owned(), Prop::I64(-570315263996158600)),
                    ("340".to_owned(), Prop::F64(-3.651023008388272e-78)),
                ],
                None,
            ),
            (
                VID(4),
                VID(4),
                15,
                vec![
                    ("811".to_owned(), Prop::str("24")),
                    ("607".to_owned(), Prop::Bool(false)),
                ],
                vec![
                    ("59".to_owned(), Prop::I64(4022071530038561966)),
                    ("340".to_owned(), Prop::F64(-4.79337077061449e-296)),
                ],
                Some("b"),
            ),
        ];
        check_graph_with_props(10, 10, &edges.into());
    }

    fn check_graph_with_nodes(node_page_len: usize, edge_page_len: usize, fixture: &NodeFixture) {
        check_graph_with_nodes_support(fixture, false, |path| {
            Layer::<()>::new(path, node_page_len, edge_page_len)
        });
    }

    fn check_graph_with_props(node_page_len: usize, edge_page_len: usize, fixture: &Fixture) {
        check_graph_with_props_support(fixture, false, |path| {
            Layer::<()>::new(path, node_page_len, edge_page_len)
        });
    }
}
