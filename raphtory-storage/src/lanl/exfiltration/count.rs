use crate::lanl::exfiltration::find_active_nodes;
use itertools::kmerge_by;
use raphtory::{
    arrow::Time,
    core::{
        entities::{EID, VID},
        Direction,
    },
};
use raphtory_arrow::{
    edge::Edge, global_order::GlobalOrder, graph::TemporalGraph,
    graph_fragment::TempColGraphFragment, prelude::*,
};
use rayon::prelude::*;
use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::Instant,
};

#[derive(Debug, Eq, Copy, Clone)]
enum Window {
    Start { t: i64 },
    End { t: i64 },
}

impl PartialOrd for Window {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.t().partial_cmp(&other.t())
    }
}

impl Ord for Window {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.t().cmp(&other.t())
    }
}

impl PartialEq for Window {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.t() == other.t()
    }
}

impl Window {
    #[inline]
    pub fn t(&self) -> i64 {
        match self {
            Window::Start { t, .. } => *t,
            Window::End { t, .. } => *t,
        }
    }
}
#[inline]
fn window_bounds(t: i64, w: i64) -> [Window; 2] {
    [Window::Start { t: t - w }, Window::End { t }]
}
#[inline]
fn valid_netflow_events(
    nft_graph: &TempColGraphFragment,
    b_vid: VID,
    bytes_prop_id: usize,
    window: i64,
) -> Option<Vec<(VID, Vec<Window>)>> {
    let nft_events: Vec<_> = nft_graph
        .out_edges_par(b_vid)
        .filter(|(_, e_vid)| *e_vid != b_vid)
        .filter_map(|(edge_id, e_vid)| {
            let nf1 = nft_graph.edge(edge_id);
            let mut valid_events: Vec<_> = nf1
                .par_prop_items_unchecked::<i64>(bytes_prop_id)
                .unwrap()
                .filter_map(move |(t, v)| (v > 100_000_000).then(|| window_bounds(t, window)))
                .flatten()
                .collect();
            if valid_events.is_empty() {
                None
            } else {
                valid_events.sort_by(|a, b| b.cmp(a)); // reverse sort
                Some((e_vid.into(), valid_events))
            }
        })
        .collect();
    if nft_events.is_empty() {
        None
    } else {
        Some(nft_events)
    }
}

#[derive(Debug, Default)]
struct MergeCounter {
    count: usize,
    active_windows: VecDeque<usize>,
    lookup: Vec<(Time, usize)>,
    event_count: usize,
}

impl MergeCounter {
    #[inline]
    fn update_lookup(&mut self, t: Time) {
        if let Some(last) = self.lookup.last_mut().filter(|(last_t, _)| last_t == &t) {
            last.1 = self.count;
        } else {
            self.lookup.push((t, self.count))
        }
    }

    #[inline]
    fn update_window_start(&mut self, t: Time) {
        //finished a window
        let index = self.active_windows.pop_front().unwrap();
        let num_events = self.event_count - index;
        if num_events != 0 {
            self.count -= num_events;
            self.update_lookup(t);
        }
    }
    #[inline]
    fn update_nft(&mut self, event: Window) {
        match event {
            Window::Start { t } => self.update_window_start(t),
            Window::End { .. } => {
                //start a new window
                self.active_windows.push_back(self.event_count);
            }
        }
    }
    #[inline]
    fn update_prog1(&mut self, prog1: i64) {
        if !self.active_windows.is_empty() {
            self.event_count += 1;
            self.count += self.active_windows.len();
            self.update_lookup(prog1);
        }
    }
    #[inline]
    fn finish(
        mut self,
        remaining_windows: impl Iterator<Item = Window>,
    ) -> Option<Vec<(Time, usize)>> {
        if !self.active_windows.is_empty() {
            for w in remaining_windows {
                match w {
                    Window::Start { t } => self.update_window_start(t),
                    _ => {}
                }
                if self.active_windows.is_empty() {
                    break;
                }
            }
        }
        if self.lookup.is_empty() {
            None
        } else {
            Some(self.lookup)
        }
    }
}

#[inline]
fn merge_nft_prog1(
    events_1v_edge: &Edge,
    nft_events: impl IntoIterator<Item = Window>,
    prop_id: usize,
) -> Option<Vec<(Time, usize)>> {
    let prog_events = events_1v_edge
        .prop_items_unchecked::<i64>(prop_id)
        .filter_map(|(t, v)| (v == 4688).then_some(t))
        .rev();
    let mut nft_events_iter = nft_events.into_iter();
    let mut next_nft_event = nft_events_iter.next();
    let mut merge_counter = MergeCounter::default();
    for t in prog_events {
        while next_nft_event.filter(|event| event.t() > t).is_some() {
            merge_counter.update_nft(next_nft_event.unwrap());
            next_nft_event = nft_events_iter.next();
        }
        if next_nft_event.is_none() {
            // no more windows
            break;
        }
        merge_counter.update_prog1(t);
    }
    // finalise any remaining open windows!
    merge_counter.finish(next_nft_event.into_iter().chain(nft_events_iter))
}

#[inline]
fn loop_events(
    a_vid: VID,
    events_1v_edge: &Edge,
    login_edge: &Edge,
    nft_events: &[(VID, Vec<Window>)],
    prog1_prop_id: usize,
    login_prop_id: usize,
) -> Option<usize> {
    let index = nft_events.binary_search_by_key(&a_vid, |(v, _)| *v).ok()?;
    let nf_events_iter = nft_events[index].1.iter().copied();
    let prog1_map = merge_nft_prog1(events_1v_edge, nf_events_iter, prog1_prop_id)?;
    local_login_count(login_edge, login_prop_id, &prog1_map)
}

#[inline]
fn local_login_count(
    login_edge: &Edge,
    prop_id: usize,
    prog1_map: &[(Time, usize)],
) -> Option<usize> {
    if login_edge.timestamp_slice().iter().next()? >= prog1_map.first()?.0 {
        return None;
    }
    login_edge
        .par_prop_items_unchecked::<i64>(prop_id)
        .map(|iter| {
            iter.filter(|(_, id)| *id == 4624)
                .map(|(t, _)| {
                    let index = prog1_map.partition_point(|(ti, _)| ti > &t);
                    if index > 0 {
                        prog1_map[index - 1].1
                    } else {
                        0
                    }
                })
                .sum()
        })
}

#[inline]
fn count_logins(
    b_vid: VID,
    prog1_map: &[(Time, usize)],
    nft_events: &[(VID, Vec<Window>)],
    events_2v_graph: &TempColGraphFragment,
    events_1v_edge: &Edge,
    edges_par_iter: impl IndexedParallelIterator<Item = (EID, VID)>,
    login_prop_id: usize,
    prog1_prop_id: usize,
) -> usize {
    edges_par_iter
        .filter(|(_, a_vid)| *a_vid != b_vid)
        .filter_map(|(eid, a_vid)| {
            let edge = events_2v_graph.edge(eid);
            local_login_count(&edge, login_prop_id, prog1_map).map(move |c| (a_vid, edge, c))
        })
        .filter(|(_, _, count)| *count != 0)
        .map(|(a_vid, edge, count)| {
            let loop_count = loop_events(
                a_vid,
                events_1v_edge,
                &edge,
                nft_events,
                prog1_prop_id,
                login_prop_id,
            )
            .unwrap_or(0);
            count - loop_count
        })
        .sum()
}

#[inline]
pub fn query<GO: GlobalOrder>(
    g: &TemporalGraph<GO>,
    window: i64,
) -> Option<impl ParallelIterator<Item = (VID, usize)> + '_> {
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

    let log_nodes = find_active_nodes(events_1v_graph);

    let valid_netflow_events_ms = Arc::new(AtomicU64::default());
    let valid_netflow_events_ms_ref = valid_netflow_events_ms.clone();
    let prog1_merge_ms = Arc::new(AtomicU64::default());
    let prog1_merge_ms_ref = prog1_merge_ms.clone();
    let count_login_ms = Arc::new(AtomicU64::default());
    let count_login_ms_ref = count_login_ms.clone();

    let count = log_nodes.into_par_iter().flat_map(move |b_vid| {
        let login_edges = events_2v_graph
            .in_edges_par(b_vid)
            .map(|(eid, a_vid)| (eid.into(), a_vid.into()));
        let (self_loop, _) = events_1v_graph
            .edges_iter(b_vid, Direction::OUT)
            .filter(|(_, n_vid)| *n_vid == b_vid)
            .next()?;

        let now = Instant::now();
        let nft_events = valid_netflow_events(nft_graph, b_vid, bytes_prop_id, window)?;
        valid_netflow_events_ms_ref.fetch_add(now.elapsed().as_millis() as u64, Relaxed);

        let now = Instant::now();
        let merged_nft_events = kmerge_by(
            nft_events.iter().map(|(_, windows)| windows),
            |a: &&Window, b: &&Window| a >= b,
        )
        .copied();
        let events_1v_edge = events_1v_graph.edge(self_loop);

        let prog1_eventmap = merge_nft_prog1(&events_1v_edge, merged_nft_events, prog1_prop_id)?;
        prog1_merge_ms_ref.fetch_add(now.elapsed().as_millis() as u64, Relaxed);

        let now = Instant::now();
        let count = count_logins(
            b_vid,
            &prog1_eventmap,
            &nft_events,
            events_2v_graph,
            &events_1v_edge,
            login_edges,
            event_id_prop_id_2v,
            prog1_prop_id,
        );
        count_login_ms_ref.fetch_add(now.elapsed().as_millis() as u64, Relaxed);
        Some((b_vid, count))
    });
    println!(
        "finding valid netflow took {}ms",
        valid_netflow_events_ms.load(Relaxed)
    );
    println!(
        "merging netflow with prog1 took {}ms",
        prog1_merge_ms.load(Relaxed)
    );
    println!("counting logins took {}ms", count_login_ms.load(Relaxed));
    Some(count)
}

pub fn query_total<GO: GlobalOrder>(g: &TemporalGraph<GO>, window: i64) -> usize {
    query(g, window)
        .into_par_iter()
        .flatten()
        .map(|(_, c)| c)
        .sum()
}

#[cfg(test)]
mod test {
    use super::query_total;
    use polars_arrow::{
        array::{PrimitiveArray, StructArray},
        datatypes::{ArrowDataType as DataType, Field},
    };
    use raphtory_arrow::{
        global_order::GlobalMap, graph::TemporalGraph, graph_fragment::TempColGraphFragment,
    };
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_one_path() {
        let test_dir = TempDir::new().unwrap();
        let go = Arc::new(GlobalMap::from(vec![1u64, 2u64, 3u64]));
        let vertices = PrimitiveArray::from_vec(vec![1u64, 2u64, 3u64]).boxed();

        let srcs = PrimitiveArray::from_vec(vec![1u64, 1, 1, 1]).boxed();
        let dsts = PrimitiveArray::from_vec(vec![2u64, 2, 2, 2]).boxed();
        let time = PrimitiveArray::from_vec(vec![0i64, 1, 10, 11]).boxed();
        let event_id = PrimitiveArray::from_vec(vec![4624i64, 4624, 4624, 23]).boxed();
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
        let actual = query_total(&graph, 30);
        assert_eq!(actual, 4);
    }
}
