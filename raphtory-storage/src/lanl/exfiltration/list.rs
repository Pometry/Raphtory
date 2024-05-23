use crate::lanl::exfiltration::find_active_nodes;
use itertools::{kmerge_by, Itertools};
use raphtory::core::{entities::VID, Direction};
use raphtory_arrow::{
    edge::ExplodedEdge, global_order::GlobalOrder, graph::TemporalGraph,
    graph_fragment::TempColGraphFragment, Time,
};
use rayon::{iter::ParallelIterator, prelude::*};
use std::{cmp::Ordering, collections::VecDeque, fmt::Debug};

#[inline]
fn valid_netflow_events(
    nft_graph: &TempColGraphFragment,
    b_vid: VID,
    bytes_prop_id: usize,
) -> impl Iterator<Item = Event> {
    kmerge_by(
        nft_graph
            .edges_iter(b_vid, Direction::OUT)
            .into_iter()
            .filter(|(_, e_vid)| *e_vid != b_vid)
            .map(move |(edge_id, _)| {
                nft_graph
                    .edge(edge_id)
                    .explode()
                    .filter(move |e| e.prop::<i64>(bytes_prop_id) > Some(100_000_000))
                    .rev()
            }),
        |e1: &ExplodedEdge, e2: &ExplodedEdge| e1.timestamp() >= e2.timestamp(),
    )
    .map(Event::Netflow)
}

#[inline]
fn login_edges(
    events_2v_graph: &TempColGraphFragment,
    b_vid: VID,
    login_event_prop_id: usize,
) -> impl Iterator<Item = Event> {
    kmerge_by(
        events_2v_graph
            .edges_iter(b_vid, Direction::IN)
            .into_iter()
            .filter(|(_, a_vid)| *a_vid != b_vid)
            .map(move |(edge_id, _)| {
                events_2v_graph
                    .edge(edge_id)
                    .explode()
                    .filter(move |e| e.prop::<i64>(login_event_prop_id) == Some(4624))
                    .rev()
            }),
        |e1: &ExplodedEdge, e2: &ExplodedEdge| e1.timestamp() >= e2.timestamp(),
    )
    .map(Event::Login)
}

#[inline]
fn prog1_edges(
    event_1v_graph: &TempColGraphFragment,
    b_vid: VID,
    prog1_event_prop_id: usize,
) -> impl Iterator<Item = Event> {
    event_1v_graph
        .edges_iter(b_vid, Direction::OUT)
        .into_iter()
        .filter(move |(_, vid)| *vid == b_vid)
        .flat_map(move |(eid, _)| {
            event_1v_graph
                .edge(eid)
                .explode()
                .filter(move |e| e.prop::<i64>(prog1_event_prop_id) == Some(4688))
        })
        .rev()
        .map(Event::Prog1)
}

#[derive(Debug, Clone)]
enum Event<'a> {
    Login(ExplodedEdge<'a>),
    Prog1(ExplodedEdge<'a>),
    Netflow(ExplodedEdge<'a>),
}

impl<'a> PartialEq for Event<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Event::Login(e1), Event::Login(e2)) => e1.timestamp() == e2.timestamp(),
            (Event::Prog1(e1), Event::Prog1(e2)) => e1.timestamp() == e2.timestamp(),
            (Event::Netflow(e1), Event::Netflow(e2)) => e1.timestamp() == e2.timestamp(),
            _ => false,
        }
    }
}

impl<'a> Eq for Event<'a> {}

impl<'a> PartialOrd for Event<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// the order of the different events at the same time point is crucial, need Login, then Prog1
// and then Netflow to get the window bounds correct
impl<'a> Ord for Event<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match other.t().cmp(&self.t()) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => match self {
                Event::Login(_) => match other {
                    Event::Login(_) => Ordering::Equal,
                    Event::Prog1(_) => Ordering::Less,
                    Event::Netflow(_) => Ordering::Less,
                },
                Event::Prog1(_) => match other {
                    Event::Login(_) => Ordering::Greater,
                    Event::Prog1(_) => Ordering::Equal,
                    Event::Netflow(_) => Ordering::Less,
                },
                Event::Netflow(_) => match other {
                    Event::Login(_) => Ordering::Greater,
                    Event::Prog1(_) => Ordering::Greater,
                    Event::Netflow(_) => Ordering::Equal,
                },
            },
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl<'a> Event<'a> {
    #[inline]
    fn inner(&self) -> &ExplodedEdge<'a> {
        match self {
            Event::Login(e) => e,
            Event::Prog1(e) => e,
            Event::Netflow(e) => e,
        }
    }

    #[inline]
    fn t(&self) -> Time {
        self.inner().timestamp()
    }
}

struct MergeIter<'a, I: Iterator<Item = Event<'a>>> {
    events: I,
    active_netflow: VecDeque<(Time, usize, ExplodedEdge<'a>)>,
    active_prog1: VecDeque<ExplodedEdge<'a>>,
    inner_state: Option<MergeInnerState<'a>>,
    event_count: usize,
    window: i64,
}

impl<'a, I: Iterator<Item = Event<'a>>> MergeIter<'a, I> {
    #[inline]
    fn new(events: I, window: i64) -> Self {
        Self {
            events,
            active_netflow: Default::default(),
            active_prog1: Default::default(),
            inner_state: None,
            event_count: 0,
            window,
        }
    }
    #[inline]
    fn oldest_window_t(&self) -> Option<Time> {
        self.active_netflow.front().map(|(t, _, _)| *t)
    }

    #[inline]
    fn oldest_count(&self) -> Option<usize> {
        self.active_netflow.front().map(|(_, count, _)| *count)
    }

    #[inline]
    fn advance_windows(&mut self, new_t: Time) {
        while self.oldest_window_t() > Some(new_t) {
            let (_, index, _) = self.active_netflow.pop_front().unwrap();
            if let Some(next_index) = self.oldest_count() {
                let to_remove = next_index - index;
                let new_len = self.active_prog1.len() - to_remove;
                self.active_prog1.truncate(new_len);
            } else {
                self.active_prog1.clear()
            }
        }
    }

    #[inline]
    fn handle_next_event(&mut self) -> bool {
        if let Some(next_event) = self.events.next() {
            self.advance_windows(next_event.t());
            match next_event {
                Event::Login(login) => {
                    if !self.active_prog1.is_empty() {
                        for (index, (_, count, netflow)) in self.active_netflow.iter().enumerate() {
                            if login.src() != netflow.dst() {
                                self.inner_state = Some(MergeInnerState::new(
                                    login,
                                    index,
                                    self.event_count - count,
                                ));
                                break;
                            }
                        }
                    }
                }
                Event::Prog1(prog1) => {
                    if !self.active_netflow.is_empty() {
                        self.active_prog1.push_front(prog1); // most recent in front for easier truncation
                        self.event_count += 1; // keep track of prog1 events
                    }
                }
                Event::Netflow(nf) => {
                    let t = nf.timestamp();
                    self.active_netflow
                        .push_back((t - self.window, self.event_count, nf));
                }
            }
            true
        } else {
            false
        }
    }

    #[inline]
    fn next_inner(&mut self) -> (ExplodedEdge<'a>, ExplodedEdge<'a>, ExplodedEdge<'a>) {
        let inner_state = self.inner_state.as_mut().unwrap();
        let (_, _, netflow) = self.active_netflow[inner_state.netflow_index].clone();
        let login = inner_state.login.clone();
        let prog1 = self.active_prog1[inner_state.prog1_index].clone();
        if inner_state.advance() {
            if inner_state.netflow_index >= self.active_netflow.len() {
                self.inner_state = None;
            } else {
                let (_, count, _) = self.active_netflow[inner_state.netflow_index];
                let new_len = self.event_count - count;
                if new_len == 0 {
                    self.inner_state = None;
                } else {
                    inner_state.prog1_len = new_len;
                }
            }
        }
        (login, prog1, netflow)
    }
}

impl<'a, I: Iterator<Item = Event<'a>>> Iterator for MergeIter<'a, I> {
    type Item = (ExplodedEdge<'a>, ExplodedEdge<'a>, ExplodedEdge<'a>);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.inner_state.is_none() {
            if !self.handle_next_event() {
                return None;
            }
        }
        Some(self.next_inner())
    }
}

struct MergeInnerState<'a> {
    login: ExplodedEdge<'a>,
    prog1_index: usize,
    netflow_index: usize,
    prog1_len: usize,
}

impl<'a> MergeInnerState<'a> {
    #[inline]
    fn new(login: ExplodedEdge<'a>, netflow_index: usize, prog1_len: usize) -> Self {
        Self {
            login,
            prog1_index: 0,
            netflow_index,
            prog1_len,
        }
    }

    #[inline]
    fn advance(&mut self) -> bool {
        self.prog1_index += 1;
        if self.prog1_index >= self.prog1_len {
            self.prog1_index = 0;
            self.netflow_index += 1;
            true
        } else {
            false
        }
    }
}

pub fn query_per_vertex<'a, GO: GlobalOrder>(
    g: &'a TemporalGraph<GO>,
    window: i64,
    nodes: impl IntoParallelIterator<Item = VID> + 'a,
) -> Option<
    impl ParallelIterator<
            Item = (
                VID,
                impl Iterator<Item = (ExplodedEdge<'a>, ExplodedEdge<'a>, ExplodedEdge<'a>)>,
            ),
        > + 'a,
> {
    //   MATCH
    //     (E)<-[nf1:Netflow]-(B)<-[login1:Events2v]-(A), (B)<-[prog1:Events1v]-(B)
    //   WHERE A <> B AND B <> E AND A <> E
    //     AND login1.eventID = 4624
    //     AND prog1.eventID = 4688
    //     AND nf1.dstBytes > 100000000
    //     // time constraints within each path
    //     AND login1.epochtime < prog1.epochtime
    //     AND prog1.epochtime < nf1.epochtime
    //     AND nf1.epochtime - login1.epochtime <= 30
    //   RETURN count(*)

    // Launched job 7
    // Number of answers: 2,992,551
    // CPU times: user 25.9 ms, sys: 11.7 ms, total: 37.6 ms
    // Wall time: 21.8 s

    // Devices (vertices): 159,245
    // Netflow (edges): 317,164,045
    // Host event 1-vertex (edges): 33,480,483
    // Host event 2-vertex (edges): 97,716,529
    // Total (edges): 448,361,057
    let nft = g.find_layer_id("netflow")?;
    let nft_graph = g.layer(nft);
    let events_1v = g.find_layer_id("events_1v")?;
    let events_1v_graph = g.layer(events_1v);
    let events_2v = g.find_layer_id("events_2v")?;
    let events_2v_graph = g.layer(events_2v);

    let bytes_prop_id = g.edge_property_id("dst_bytes", nft)?;
    let event_id_prop_id_2v = g.edge_property_id("event_id", events_2v)?;
    let prog1_prop_id = g.edge_property_id("event_id", events_1v)?;

    let iter = nodes.into_par_iter().map(move |b_vid| {
        let logins = login_edges(events_2v_graph, b_vid, event_id_prop_id_2v);
        let prog1s = prog1_edges(events_1v_graph, b_vid, prog1_prop_id);
        let netflows = valid_netflow_events(nft_graph, b_vid, bytes_prop_id);
        let events = logins.merge(prog1s).merge(netflows);
        (
            b_vid,
            MergeIter::new(events, window).filter(|(l, _, n)| l.src() != n.dst()),
        )
    });
    Some(iter)
}

#[inline]
pub fn query_all_log_vertices<GO: GlobalOrder>(
    g: &TemporalGraph<GO>,
    window: i64,
) -> Option<
    impl ParallelIterator<
            Item = (
                VID,
                impl Iterator<Item = (ExplodedEdge, ExplodedEdge, ExplodedEdge)>,
            ),
        > + '_,
> {
    let events_1v = g.find_layer_id("events_1v")?;
    let events_1v_graph = g.layer(events_1v);
    query_per_vertex(g, window, find_active_nodes(events_1v_graph))
}

pub fn query<GO: GlobalOrder>(
    g: &TemporalGraph<GO>,
    window: i64,
) -> impl ParallelIterator<Item = (ExplodedEdge, ExplodedEdge, ExplodedEdge)> + '_ {
    query_all_log_vertices(g, window)
        .into_par_iter()
        .flatten()
        .flat_map_iter(|(_, iter)| iter)
}

pub fn query_count<GO: GlobalOrder>(g: &TemporalGraph<GO>, window: i64) -> usize {
    query(g, window).count()
}

#[cfg(test)]
mod test {
    use crate::lanl::exfiltration::list::query;
    use polars_arrow::{
        array::{PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
    };
    use raphtory_arrow::{
        global_order::GlobalMap, graph::TemporalGraph, graph_fragment::TempColGraphFragment,
    };
    use rayon::prelude::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_one_path() {
        let test_dir = TempDir::new().unwrap();
        let go = Arc::new(GlobalMap::from(vec![1u64, 2u64, 3u64]));
        let vertices = PrimitiveArray::from_vec(vec![1u64, 2u64, 3u64]).boxed();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1, 1, 1, 1, 1]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 2, 2, 2, 2, 2]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1, 1, 10, 10, 11]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![4624i64, 4624, 4624, 4624, 4624, 23]).boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("event_id", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_events2v = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("events2v"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let srcs = PrimitiveArray::from_vec(vec![2u64, 2, 2]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 2, 2]).boxed();
        let time = PrimitiveArray::from_vec(vec![1i64, 11, 12]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![4688i64, 4688, 4687]).boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("event_id", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_events1v = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("events1v"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let srcs = PrimitiveArray::from_vec(vec![2u64, 2u64, 2, 2]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![3u64, 3u64, 3, 3]).boxed();
        let time = PrimitiveArray::from_vec(vec![2i64, 3, 4, 31]).boxed();
        let event_id =
            PrimitiveArray::from_vec(vec![100_000_005i64, 100_000_005i64, 10, 100_000_005i64])
                .boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("dst_bytes", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_netflow = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("netflow"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let graph = TemporalGraph::new_from_layers(
            vertices,
            go,
            vec![graph_events2v, graph_events1v, graph_netflow],
            vec![
                "events_2v".to_owned(),
                "events_1v".to_owned(),
                "netflow".to_owned(),
            ],
        );
        let actual: Vec<_> = query(&graph, 30).collect();
        println!("{:?}", actual);
        assert_eq!(actual.len(), 6);
    }

    #[test]
    fn test_loop() {
        let test_dir = TempDir::new().unwrap();
        let gids = vec![1u64, 2u64, 3u64, 4u64];
        let go = Arc::new(GlobalMap::from(gids.clone()));
        let vertices = PrimitiveArray::from_vec(gids).boxed();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 3, 4]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 2, 2]).boxed();
        let time = PrimitiveArray::from_vec(vec![2i64, 1, 3]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![4624i64, 4624, 4624]).boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("event_id", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_events2v = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("events2v"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let srcs = PrimitiveArray::from_vec(vec![2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![10i64]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![4688i64]).boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("event_id", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_events1v = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("events1v"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let srcs = PrimitiveArray::from_vec(vec![2u64]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![1u64]).boxed();
        let time = PrimitiveArray::from_vec(vec![20i64]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![100_000_005i64]).boxed();
        let chunk = StructArray::new(
            DataType::Struct(vec![
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
                Field::new("time", DataType::Int64, false),
                Field::new("dst_bytes", DataType::Int64, false),
            ]),
            vec![srcs, dsts, time, event_id],
            None,
        );

        let graph_netflow = TempColGraphFragment::load_from_edge_list(
            &test_dir.path().join("netflow"),
            0,
            100,
            100,
            go.clone(),
            vertices.clone(),
            0,
            1,
            2,
            vec![chunk],
        )
        .unwrap();

        let graph = TemporalGraph::new_from_layers(
            vertices,
            go,
            vec![graph_events2v, graph_events1v, graph_netflow],
            vec![
                "events_2v".to_owned(),
                "events_1v".to_owned(),
                "netflow".to_owned(),
            ],
        );
        let actual: Vec<_> = query(&graph, 30).collect();
        println!("{:?}", actual);
        assert_eq!(actual.len(), 2);
    }
}
