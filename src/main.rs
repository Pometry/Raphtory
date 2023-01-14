#![allow(unused_imports)]
use std::collections::HashMap;
use std::marker::PhantomData;
use std::thread::JoinHandle;
use std::{env, thread};

use crossbeam::channel::{bounded, unbounded, Receiver, SendError, Sender};
use csv::StringRecord;
use docbrown::db::GraphDB;
use docbrown::graph::TemporalGraph;
use docbrown::Prop;
// use flume::{unbounded, Receiver, Sender};
use itertools::Itertools;
use replace_with::{replace_with, replace_with_or_abort, replace_with_or_abort_and_return};
// use std::sync::mpsc;
use std::time::Instant;

use flate2; // 1.0
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{prelude::*, BufReader, LineWriter};

fn parse_record(rec: &StringRecord) -> Option<(u64, u64, u64, u64)> {
    let src = rec.get(3).and_then(|s| s.parse::<u64>().ok())?;
    let dst = rec.get(4).and_then(|s| s.parse::<u64>().ok())?;
    let t = rec.get(5).and_then(|s| s.parse::<u64>().ok())?;
    let amount = rec.get(7).and_then(|s| s.parse::<u64>().ok())?;
    Some((src, dst, t, amount))
}

fn local_single_threaded_temporal_graph(args: Vec<String>) {
    let mut g = TemporalGraph::default();

    let now = Instant::now();

    if let Some(file_name) = args.get(1) {
        let f = File::open(file_name).expect(&format!("Can't open file {file_name}"));
        let mut csv_gz_reader = csv::Reader::from_reader(BufReader::new(GzDecoder::new(f)));

        for rec_res in csv_gz_reader.records() {
            if let Ok(rec) = rec_res {
                if let Some((src, dst, t, amount)) = parse_record(&rec) {
                    g.add_vertex(src, t);
                    g.add_vertex(dst, t);
                    g.add_edge_props(src, dst, t, &vec![("amount".into(), Prop::U64(amount))]);
                }
            }
        }

        println!(
            "Loaded {} vertices, took {} seconds",
            g.len(),
            now.elapsed().as_secs()
        );

        let now = Instant::now();

        let iter = g.iter_vertices().map(|v| {
            let id = v.global_id();
            let out_d = v.outbound_degree();
            let in_d = v.inbound_degree();
            let deg = v.degree();

            format!("{id},{out_d},{in_d},{deg}\n")
        });

        let file = File::create("bay_deg.csv").expect("unable to create file bay_deg.csv");
        let mut file = LineWriter::new(file);

        for line in iter {
            file.write(line.as_bytes())
                .expect("Unable to write to file");
        }
        println!(
            "Degree output written in {} seconds",
            now.elapsed().as_secs()
        )
    }
}

enum Msg {
    AddVertex(u64, u64),
    AddEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddOutEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddIntoEdge(u64, u64, Vec<(String, Prop)>, u64),
    VertexChunk(Vec<Msg>),
    Done,
    Len(tokio::sync::oneshot::Sender<usize>),
    Batch(Vec<Msg>),
}

struct TGraphShardActor {
    queue: Receiver<Msg>,
    tg: TemporalGraph,
    buf: Vec<Msg>,
}

impl TGraphShardActor {
    fn new(receiver: Receiver<Msg>) -> Self {
        TGraphShardActor {
            queue: receiver,
            tg: TemporalGraph::default(),
            buf: vec![],
        }
    }

    fn handle_message(&mut self, msg: Msg) -> () {
        match msg {
            // Msg::AddVertex(v, t) => {
            //     self.tg.add_vertex(v, t);
            // }
            // Msg::AddEdge(src, dst, props, t) => {
            //     self.tg.add_edge_props(src, dst, t, &props);
            // }
            // Msg::AddOutEdge(src, dst, props, t) => {
            //     self.tg.add_edge_remote_out(src, dst, t, &props);
            // }
            // Msg::AddIntoEdge(src, dst, props, t) => {
            //     self.tg.add_edge_remote_into(src, dst, t, &props);
            // }
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
        let capacity = 1024;
        Self {
            sender,
            handle,
            vertex_buf: Vec::with_capacity(capacity),
            buf_send_size: capacity,
        }
    }

    fn send_or_buffer_msg(&mut self, m: Msg, force: bool) {
        if self.vertex_buf.len() < self.buf_send_size && !force{
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

    pub async fn len(&self) -> usize {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.sender.send(Msg::Len(tx));
        rx.await.unwrap()
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
        // for g in gs {
        //     len += g.len().await;
        // }
        println!(
            "Loading {len} vertices, took {} seconds",
            now.elapsed().as_secs()
        );
    }
}

// #[tokio::main]
fn main() {
    // let handle = tokio::spawn(async move {
    //     local_single_threaded_temporal_graph(args);
    // });

    // handle.await.unwrap();
    let args: Vec<String> = env::args().collect();
    let threads = 8;
    let mut shards = vec![];

    for _ in 0..threads {
        shards.push(TGraphShard::new())
    }

    local_actor_single_threaded_temporal_graph(&mut shards, args);

    for g in shards {
        g.done();
    }
}
