#![allow(unused_imports)]
use std::collections::HashMap;
use std::marker::PhantomData;
use std::thread::JoinHandle;
use std::{env, thread};

use chrono::{DateTime, Utc};
use csv::StringRecord;
use docbrown_core::graph::TemporalGraph;
use docbrown_core::Prop;
use docbrown_db::loaders::csv::CsvLoader;
use docbrown_db::GraphDB;
use flume::{unbounded, Receiver, Sender};
use itertools::Itertools;
use regex::Regex;
use replace_with::{replace_with, replace_with_or_abort, replace_with_or_abort_and_return};
use serde::Deserialize;
use std::time::Instant;

use flate2; // 1.0
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{prelude::*, BufReader, LineWriter};

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

fn parse_record(rec: &StringRecord) -> Option<(u64, u64, u64, u64)> {
    let src = rec.get(3).and_then(|s| s.parse::<u64>().ok())?;
    let dst = rec.get(4).and_then(|s| s.parse::<u64>().ok())?;
    let t = rec.get(5).and_then(|s| s.parse::<u64>().ok())?;
    let amount = rec.get(7).and_then(|s| s.parse::<u64>().ok())?;
    Some((src, dst, t, amount))
}

// fn local_single_threaded_temporal_graph(args: Vec<String>) {
//     let mut g = TemporalGraph::default();

//     let now = Instant::now();

//     if let Some(file_name) = args.get(1) {
//         let f = File::open(file_name).expect(&format!("Can't open file {file_name}"));
//         let mut csv_gz_reader = csv::Reader::from_reader(BufReader::new(GzDecoder::new(f)));

//         for rec_res in csv_gz_reader.records() {
//             if let Ok(rec) = rec_res {
//                 if let Some((src, dst, t, amount)) = parse_record(&rec) {
//                     g.add_vertex(src, t);
//                     g.add_vertex(dst, t);
//                     g.add_edge_props(src, dst, t, &vec![("amount".into(), Prop::U64(amount))]);
//                 }
//             }
//         }

//         println!(
//             "Loaded {} vertices, took {} seconds",
//             g.len(),
//             now.elapsed().as_secs()
//         );

//         let now = Instant::now();

//         let iter = g.iter_vertices().map(|v| {
//             let id = v.global_id();
//             let out_d = v.outbound_degree();
//             let in_d = v.inbound_degree();
//             let deg = v.degree();

//             format!("{id},{out_d},{in_d},{deg}\n")
//         });

//         let file = File::create("bay_deg.csv").expect("unable to create file bay_deg.csv");
//         let mut file = LineWriter::new(file);

//         for line in iter {
//             file.write(line.as_bytes())
//                 .expect("Unable to write to file");
//         }
//         println!(
//             "Degree output written in {} seconds",
//             now.elapsed().as_secs()
//         )
//     }
// }

enum Msg {
    AddVertex(u64, u64),
    AddEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddOutEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddIntoEdge(u64, u64, Vec<(String, Prop)>, u64),
    Len(futures::channel::oneshot::Sender<usize>),
    Batch(Vec<Msg>),
    Done,
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
            Msg::Batch(msgs) => {
                for m in msgs {
                    self.handle_message(m);
                }
            }
            Msg::AddVertex(v, t) => {
                self.tg.add_vertex(v, t);
            }
            Msg::AddEdge(src, dst, props, t) => {
                self.tg.add_edge_props(src, dst, t, &props);
            }
            Msg::AddOutEdge(src, dst, props, t) => {
                self.tg.add_edge_remote_out(src, dst, t, &props);
            }
            Msg::AddIntoEdge(src, dst, props, t) => {
                self.tg.add_edge_remote_into(src, dst, t, &props);
            }
            Msg::Len(tx) => {
                tx.send(self.tg.len())
                    .expect("Failed to send response to TemporalGraph::len()");
            }
            _ => {}
        };
    }
}

fn run_tgraph_shard(mut actor: TGraphShardActor) {
    loop {
        let msg = actor.queue.recv();
        match msg {
            Ok(Msg::Done) | Err(_) => {
                return;
            }
            Ok(msg) => {
                actor.handle_message(msg);
            }
        }
    }
}

struct TGraphShard {
    sender: Sender<Msg>,
    handle: JoinHandle<()>,
    vertex_buf: Vec<Msg>,
    buf_send_size: usize,
}

impl TGraphShard {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded::<Msg>();
        let actor = TGraphShardActor::new(receiver);

        let handle = thread::spawn(|| {
            println!("STARTED THREAD");
            run_tgraph_shard(actor);
            println!("DONE THREAD");
        });
        // crossbeam_utils::thread::scope(|s|{
        //     s.spawn(|_| run_tgraph_shard(actor))
        // });
        // tokio::spawn(run_tgraph_shard(actor));
        let capacity = 16 * 1024;
        Self {
            sender,
            handle,
            vertex_buf: Vec::with_capacity(capacity),
            buf_send_size: capacity,
        }
    }

    fn send_or_buffer_msg(&mut self, m: Msg, force: bool) {
        if self.vertex_buf.len() < self.buf_send_size && !force {
            self.vertex_buf.push(m)
        } else {
            let send_me_buf = replace_with_or_abort_and_return(self, |_self| match _self {
                TGraphShard {
                    sender,
                    handle,
                    vertex_buf,
                    buf_send_size,
                } => (
                    vertex_buf,
                    TGraphShard {
                        sender,
                        handle,
                        buf_send_size,
                        vertex_buf: Vec::with_capacity(_self.buf_send_size),
                    },
                ),
            });
            self.sender
                .send(Msg::Batch(send_me_buf))
                .expect("Failed to send batch")
        }
    }

    #[inline(always)]
    pub fn add_vertex(&mut self, v: u64, t: u64) {
        self.send_or_buffer_msg(Msg::AddVertex(v, t), false)
    }

    #[inline(always)]
    pub fn add_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.send_or_buffer_msg(Msg::AddEdge(src, dst, props, t), false)
    }

    #[inline(always)]
    pub fn add_remote_out_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.send_or_buffer_msg(Msg::AddOutEdge(src, dst, props, t), false)
    }

    #[inline(always)]
    pub fn add_remote_into_edge(&mut self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.send_or_buffer_msg(Msg::AddIntoEdge(src, dst, props, t), false)
    }

    pub fn done(mut self) {
        // done shutsdown the shard and flushes all messages
        self.send_or_buffer_msg(Msg::Done, true)
    }

    pub fn len(&self) -> usize {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self.sender.send(Msg::Len(tx));
        futures::executor::block_on(rx).unwrap()
    }
}

fn shard_from_id(v_id: u64, n_shards: usize) -> usize {
    let v: usize = v_id.try_into().unwrap();
    v % n_shards
}

fn local_actor_single_threaded_temporal_graph(gs: &mut Vec<TGraphShard>, args: Vec<String>) {
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

                let src_shard = shard_from_id(src, gs.len());
                let dst_shard = shard_from_id(dst, gs.len());

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
        g.done();
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
        let g = GraphDB::new(16);

        let now = Instant::now();

        let _ = CsvLoader::new(input_folder)
            .with_filter(Regex::new(r".+(sent|received)").unwrap())
            .load_into_graph(&g, |sent: Sent, g: &GraphDB| {
                let src = calculate_hash(&sent.addr);
                let dst = calculate_hash(&sent.txn);
                let t = sent.time.timestamp();

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

        let test_v = calculate_hash(&"139eeGkMGR6F9EuJQ3qYoXebfkBbNAsLtV:btc");

        assert!(g.contains(test_v));

        // g.neighbours_window(test_v)


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
