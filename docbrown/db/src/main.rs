#![allow(unused_imports)]
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::{env, thread};

use chrono::{DateTime, Utc};
use csv::StringRecord;
use docbrown_core::graph::TemporalGraph;
use docbrown_core::{Direction, Prop};
use docbrown_db::loaders::csv::CsvLoader;
use flume::{unbounded, Receiver, Sender};
use itertools::Itertools;
use regex::Regex;
use replace_with::{replace_with, replace_with_or_abort, replace_with_or_abort_and_return};
use serde::Deserialize;
use std::time::Instant;

use flate2;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{prelude::*, BufReader, LineWriter};

use docbrown_db::graphdb::GraphDB;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

enum Msg {
    AddVertex(u64, i64),
    AddEdge(u64, u64, Vec<(String, Prop)>, i64),
    AddOutEdge(u64, u64, Vec<(String, Prop)>, i64),
    AddIntoEdge(u64, u64, Vec<(String, Prop)>, i64),
    GetGraphLength(futures::channel::oneshot::Sender<usize>),
    MsgBatch(Vec<Msg>),
    FlushMsgs,
}

struct TGraphShardActor {
    queue: Receiver<Msg>,
    tg: TemporalGraph,
}

impl TGraphShardActor {
    fn new(receiver: Receiver<Msg>) -> Self {
        TGraphShardActor {
            queue: receiver,
            tg: TemporalGraph::default(),
        }
    }

    #[inline]
    fn handle_message(&mut self, msg: Msg) -> () {
        match msg {
            Msg::MsgBatch(msgs) => {
                for m in msgs {
                    self.handle_message(m);
                }
            }
            Msg::AddVertex(v, t) => {
                self.tg.add_vertex(v, t);
            }
            Msg::AddEdge(src, dst, props, t) => {
                self.tg.add_edge_with_props(src, dst, t, &props);
            }
            Msg::AddOutEdge(src, dst, props, t) => {
                self.tg.add_edge_remote_out(src, dst, t, &props);
            }
            Msg::AddIntoEdge(src, dst, props, t) => {
                self.tg.add_edge_remote_into(src, dst, t, &props);
            }
            Msg::GetGraphLength(tx) => {
                tx.send(self.tg.len())
                    .expect("Failed to send response to TemporalGraph::len()");
            }
            _ => {}
        };
    }
}

struct TGraphShard {
    sender: Sender<Msg>,
    handle: JoinHandle<()>,
    msg_buffer: Vec<Msg>,
    buffer_capacity: usize,
}

impl TGraphShard {
    pub fn new() -> Self {
        fn run_tgraph_shard(mut actor: TGraphShardActor) {
            loop {
                let msg = actor.queue.recv();
                match msg {
                    Ok(Msg::FlushMsgs) | Err(_) => {
                        return;
                    }
                    Ok(msg) => {
                        actor.handle_message(msg);
                    }
                }
            }
        }

        let (sender, receiver) = unbounded::<Msg>();
        let actor = TGraphShardActor::new(receiver);
        let handle = thread::spawn(|| {
            println!("TGraphShardActor started");
            run_tgraph_shard(actor);
            println!("TGraphShardActor finished");
        });
        let capacity = 16 * 1024;

        Self {
            sender,
            handle,
            msg_buffer: Vec::with_capacity(capacity),
            buffer_capacity: capacity,
        }
    }

    fn send_or_buffer_msg(&mut self, m: Msg, force: bool) {
        if self.msg_buffer.len() < self.buffer_capacity && !force {
            self.msg_buffer.push(m)
        } else {
            let buffered_msgs = replace_with_or_abort_and_return(self, |_self| match _self {
                TGraphShard {
                    sender,
                    handle,
                    msg_buffer,
                    buffer_capacity,
                } => (
                    msg_buffer,
                    TGraphShard {
                        sender,
                        handle,
                        msg_buffer: Vec::with_capacity(_self.buffer_capacity),
                        buffer_capacity,
                    },
                ),
            });

            self.sender
                .send(Msg::MsgBatch(buffered_msgs))
                .expect("Failed to send batch")
        }
    }

    #[inline(always)]
    pub fn add_vertex(&mut self, v: u64, t: i64) {
        self.send_or_buffer_msg(Msg::AddVertex(v, t), false)
    }

    #[inline(always)]
    pub fn add_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: i64) {
        self.send_or_buffer_msg(Msg::AddEdge(src, dst, props, t), false)
    }

    #[inline(always)]
    pub fn add_remote_out_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: i64) {
        self.send_or_buffer_msg(Msg::AddOutEdge(src, dst, props, t), false)
    }

    #[inline(always)]
    pub fn add_remote_into_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: i64) {
        self.send_or_buffer_msg(Msg::AddIntoEdge(src, dst, props, t), false)
    }

    pub fn flush(mut self) {
        // done shutsdown the shard and flushes all messages
        self.send_or_buffer_msg(Msg::FlushMsgs, true)
    }

    pub fn len(&self) -> usize {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self.sender.send(Msg::GetGraphLength(tx));
        futures::executor::block_on(rx).unwrap()
    }
}

fn local_actor_single_threaded_temporal_graph(gs: &mut Vec<TGraphShard>, args: Vec<String>) {

    fn get_shard_num_from_vertex_id(v_id: u64, n_shards: usize) -> usize {
        let v: usize = v_id.try_into().unwrap();
        v % n_shards
    }

    fn parse_record(rec: &StringRecord) -> Option<(u64, u64, i64, u64)> {
        let src = rec.get(3).and_then(|s| s.parse::<u64>().ok())?;
        let dst = rec.get(4).and_then(|s| s.parse::<u64>().ok())?;
        let t = rec.get(5).and_then(|s| s.parse::<i64>().ok())?;
        let amount = rec.get(7).and_then(|s| s.parse::<u64>().ok())?;
        Some((src, dst, t, amount))
    }

    let now = Instant::now();

    if let Some(file_name) = args.get(1) {
        let f = File::open(file_name).expect(&format!("Can't open file {file_name}"));
        let mut csv_gz_reader = csv::Reader::from_reader(BufReader::new(GzDecoder::new(f)));

        let mut rec = csv::StringRecord::new();
        let mut count = 0;
        let mut msg_count = 0;
        while csv_gz_reader.read_record(&mut rec).unwrap() {
            if let Some((src, dst, t, amount)) = parse_record(&rec) {
                count += 1;

                let src_shard = get_shard_num_from_vertex_id(src, gs.len());
                let dst_shard = get_shard_num_from_vertex_id(dst, gs.len());

                gs[src_shard].add_vertex(src, t);
                gs[dst_shard].add_vertex(dst, t);

                msg_count += 2;
                if src_shard == dst_shard {
                    gs[src_shard].add_edge(src, dst, vec![("amount".into(), Prop::U64(amount))], t);
                    msg_count += 1;
                } else {
                    gs[src_shard].add_remote_out_edge(
                        src,
                        dst,
                        vec![("amount".into(), Prop::U64(amount))],
                        t,
                    );
                    gs[dst_shard].add_remote_into_edge(
                        src,
                        dst,
                        vec![("amount".into(), Prop::U64(amount))],
                        t,
                    );
                    msg_count += 2;
                }
            }
        }

        println!(
            "Shipping {count} lines  and {msg_count} messages took {} seconds",
            now.elapsed().as_secs()
        );
        let mut len = 0;
        for g in gs {
            len += g.len();
        }
        println!(
            "Loading {len} vertices, took {} seconds",
            now.elapsed().as_secs()
        );
    }
}

fn load_multiple_threads() {
    let args: Vec<String> = env::args().collect();
    let threads = 2;
    let mut shards = vec![];

    for _ in 0..threads {
        shards.push(TGraphShard::new())
    }

    local_actor_single_threaded_temporal_graph(&mut shards, args);

    for g in shards {
        g.flush();
    }
}

#[derive(Deserialize, std::fmt::Debug)]
pub struct Sent {
    addr: String,
    txn: String,
    amount_btc: u64,
    amount_usd: f64,
    #[serde(with = "custom_date_format")]
    time: DateTime<Utc>,
}

#[derive(Deserialize, std::fmt::Debug)]
pub struct Received {
    txn: String,
    addr: String,
    amount_btc: u64,
    amount_usd: f64,
    #[serde(with = "custom_date_format")]
    time: DateTime<Utc>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if let Some(input_folder) = args.get(1) {
        // if input_folder/graphdb.bincode exists, use bincode to load the graph
        // otherwise, load the graph from the csv files

        let test_v = calculate_hash(&"139eeGkMGR6F9EuJQ3qYoXebfkBbNAsLtV:btc");

        let path: PathBuf = [input_folder, "graphdb.bincode"].iter().collect();
        let graph = if path.exists() {
            let now = Instant::now();
            let g = GraphDB::load_from_file(path.as_path()).expect("Failed to load graph");

            println!(
                "Loaded graph from path {} with {} vertices, {} edges, took {} seconds",
                path.to_str().unwrap(),
                g.len(),
                g.edges_len(),
                now.elapsed().as_secs()
            );
            g
        } else {
            let g = GraphDB::new(16);

            let now = Instant::now();

            let _ = CsvLoader::new(input_folder)
                .with_filter(Regex::new(r".+(sent|received)").unwrap())
                .load_into_graph(&g, |sent: Sent, g: &GraphDB| {
                    let src = calculate_hash(&sent.addr);
                    let dst = calculate_hash(&sent.txn);
                    let t = sent.time.timestamp();

                    if src == test_v || dst == test_v {
                        println!("{} sent {} to {}", sent.addr, sent.amount_btc, sent.txn);
                    }

                    g.add_edge(
                        src,
                        dst,
                        t.try_into().unwrap(),
                        &vec![("amount".to_string(), Prop::U64(sent.amount_btc))],
                    )
                })
                .expect("Failed to load graph");

            println!(
                "Loaded {} vertices, {} edges, took {} seconds",
                g.len(),
                g.edges_len(),
                now.elapsed().as_secs()
            );

            g.save_to_file(path).expect("Failed to save graph");

            g
        };

        assert!(graph.contains(test_v));
        let deg_out = graph
            .neighbours_window(0, i64::MAX, test_v, Direction::OUT)
            .count();
        let deg_in = graph
            .neighbours_window(0, i64::MAX, test_v, Direction::IN)
            .count();

        println!(
            "{} has {} out degree and {} in degree",
            test_v, deg_out, deg_in
        );
    }
}

mod custom_date_format {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}
