#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;

use crossbeam::channel::{unbounded, Receiver, Sender};
use csv::StringRecord;
use docbrown::db::GraphDB;
use docbrown::graph::TemporalGraph;
use docbrown::Prop;
use itertools::Itertools;
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
    Done,
    Len(tokio::sync::oneshot::Sender<usize>),
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

async fn run_tgraph_shard(mut actor: TGraphShardActor) {
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
}

impl TGraphShard {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded::<Msg>();
        let actor = TGraphShardActor::new(receiver);

        tokio::spawn(run_tgraph_shard(actor));
        Self { sender }
    }

    pub fn add_vertex(&self, v: u64, t: u64) {
        self.sender
            .send(Msg::AddVertex(v, t))
            .expect("Failed to add vertex");
    }

    pub fn add_edge(&self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.sender
            .send(Msg::AddEdge(src, dst, props, t))
            .expect("Failed to add Edge");
    }

    pub fn add_remote_out_edge(&self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.sender
            .send(Msg::AddOutEdge(src, dst, props, t))
            .unwrap();
    }

    pub fn add_remote_into_edge(&self, src: u64, dst: u64, props: Vec<(String, Prop)>, t: u64) {
        self.sender
            .send(Msg::AddIntoEdge(src, dst, props, t))
            .unwrap();
    }

    pub fn done(self) {
        // done shutsdown the shard
        self.sender.send(Msg::Done).unwrap();
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

async fn local_actor_single_threaded_temporal_graph(gs: &Vec<TGraphShard>, args: Vec<String>) {
    let now = Instant::now();

    if let Some(file_name) = args.get(1) {
        let f = File::open(file_name).expect(&format!("Can't open file {file_name}"));
        let mut csv_gz_reader = csv::Reader::from_reader(BufReader::new(GzDecoder::new(f)));

        let mut rec = csv::StringRecord::new();
        let mut count = 0;
        while csv_gz_reader.read_record(&mut rec).unwrap() {
            if let Some((src, dst, t, amount)) = parse_record(&rec) {
                count += 1;

                let src_shard = shard_from_id(src, gs.len());
                let dst_shard = shard_from_id(dst, gs.len());

                gs[src_shard].add_vertex(src, t);
                gs[dst_shard].add_vertex(dst, t);

                if src_shard == dst_shard {
                    gs[src_shard].add_edge(src, dst, vec![("amount".into(), Prop::U64(amount))], t);
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
                }
            }
        }

        println!(
            "Shipping {count} lines took {} seconds",
            now.elapsed().as_secs()
        );
        let mut len = 0;
        for g in gs {
            len += g.len().await;
        }
        println!(
            "Loading {len} vertices, took {} seconds",
            now.elapsed().as_secs()
        );
    }
}

#[tokio::main]
async fn main() {
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

    local_actor_single_threaded_temporal_graph(&shards, args).await;

    for g in shards {
        g.done();
    }
}
