#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;

use crossbeam::channel::unbounded;
use csv::StringRecord;
use docbrown::db::GraphDB;
use docbrown::graph::TemporalGraph;
use docbrown::Prop;
use itertools::Itertools;
use rayon::prelude::*;
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
enum Msg {
    AddVertex(u64, u64),
    AddEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddOutEdge(u64, u64, Vec<(String, Prop)>, u64),
    AddIntoEdge(u64, u64, Vec<(String, Prop)>, u64),
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

        let file =
            File::create("bay_deg.csv").expect("unable to create file bay_deg.csv");
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

fn main() {
    let args: Vec<String> = env::args().collect();
    local_single_threaded_temporal_graph(args);

    // // let g = GraphDB::new(32);
    // //
    // // let mut m: HashMap<u64, TCell<u64>> = HashMap::default();

    // // let now = Instant::now();

    // use crossbeam::channel::unbounded;

    // let n: usize = 32;

    // let mut channels = vec![];

    // for _ in 0..n {
    //     channels.push(unbounded::<Msg>())
    // }

    // let senders = channels
    //     .iter()
    //     .map(|(sender, _)| sender.clone())
    //     .collect_vec();
    // let receivers = channels
    //     .iter()
    //     .map(|(_, receiver)| receiver.clone())
    //     .collect_vec();

    // drop(channels);

    // if let Some(file_name) = args.get(1) {
    //     rayon::scope(|s| {
    //         s.spawn(move |_| {
    //             if let Ok(mut reader) = csv::Reader::from_path(file_name) {
    //                 // let senders: Vec<crossbeam::channel::Sender<Msg>> = senders;
    //                 reader.records().for_each(|rec_res| {
    //                     if let Ok(rec) = rec_res {
    //                         if let Some((src, dst, t, amount)) = parse_record(&rec) {
    //                             let src_u: usize = src.try_into().unwrap();
    //                             let dst_u: usize = dst.try_into().unwrap();

    //                             let src_shard: usize = src_u % n;
    //                             let dst_shard: usize = dst_u % n;

    //                             // add vertices
    //                             senders[src_shard]
    //                                 .send(Msg::AddVertex(src, t))
    //                                 .expect("BOOM!");
    //                             senders[dst_shard]
    //                                 .send(Msg::AddVertex(dst, t))
    //                                 .expect("BOOM!");

    //                             if src_shard == dst_shard {
    //                                 senders[src_shard]
    //                                     .send(Msg::AddEdge(
    //                                         src,
    //                                         dst,
    //                                         vec![("amount".into(), Prop::U64(amount))],
    //                                         t,
    //                                     ))
    //                                     .expect("BOOM!");
    //                             } else {
    //                                 senders[src_shard]
    //                                     .send(Msg::AddOutEdge(
    //                                         src,
    //                                         dst,
    //                                         vec![("amount".into(), Prop::U64(amount))],
    //                                         t,
    //                                     ))
    //                                     .expect("BOOM!");
    //                                 senders[dst_shard]
    //                                     .send(Msg::AddIntoEdge(
    //                                         src,
    //                                         dst,
    //                                         vec![("amount".into(), Prop::U64(amount))],
    //                                         t,
    //                                     ))
    //                                     .expect("BOOM!");
    //                             }

    //                             // g.add_vertex(src, t, vec![]).expect("can't add vertex");
    //                             // g.add_vertex(dst, t, vec![]).expect("can't add vertex");
    //                             // g.add_edge(src, dst, t, &vec![("amount".into(), Prop::U64(amount))]).expect("can't add edge");
    //                         }
    //                     }
    //                 });
    //             }
    //         });

    //         receivers.iter().enumerate().for_each(|(i, rec)| {
    //             s.spawn(move |_| {
    //                 let mut g = TemporalGraph::default();
    //                 println!("Started thread {i}");

    //                 let rcv = rec;

    //                 let mut count: usize = 0;
    //                 while let Ok(msg) = rcv.recv() {
    //                     if count % 500000 == 0 {
    //                         println!("GOT {count} {i}");
    //                     }
    //                     match msg {
    //                         Msg::AddVertex(v, t) => {
    //                             g.add_vertex(v, t);
    //                         }
    //                         Msg::AddEdge(src, dst, props, t) => {
    //                             g.add_edge_props(src, dst, t, &props);
    //                         }
    //                         Msg::AddOutEdge(src, dst, props, t) => {
    //                             g.add_edge_remote_out(src, dst, t, &props);
    //                         }
    //                         Msg::AddIntoEdge(src, dst, props, t) => {
    //                             g.add_edge_remote_into(src, dst, t, &props);
    //                         }
    //                     }
    //                     count += 1;
    //                 }

    //                 let now = Instant::now();
    //                 let len = g.len();
    //                 println!("Done {i} {now:?} vs: {len}")
    //             });
    //         });
    //     });

    //     // drop(channels);
    //     // drop(senders);
    //     drop(receivers);

    //     // drop(s1);
    //     // drop(s2);
    //     // println!("SHOULD BE DONE NOW!")

    //     // println!(
    //     //     "Loaded {} vertices, took {} seconds",
    //     //     g.len(),
    //     //     now.elapsed().as_secs()
    //     // );

    //     // println!("VERTEX,DEGREE,OUT_DEGREE,IN_DEGREE");
    //     // g.iter_vertices()
    //     //     .map(|v| {
    //     //         let id = v.global_id();
    //     //         let out_d = v.outbound_degree();
    //     //         let in_d = v.inbound_degree();
    //     //         let d = out_d + in_d;
    //     //         let out_sum: u64 = v
    //     //             .outbound()
    //     //             .flat_map(|e| {
    //     //                 e.props("amount").flat_map(|(t, p)| match p {
    //     //                     Prop::U64(amount) => Some(amount),
    //     //                     _ => None,
    //     //                 })
    //     //             })
    //     //             .sum();

    //     //         (id, d, out_d, out_sum, in_d)
    //     //     })
    //     //     .sorted_by_cached_key(|(_, _, _, _, d)| *d)
    //     //     .into_iter()
    //     //     .for_each(|(v, d, outd, amount, ind)| println!("{},{},{},{},{}", v, ind, outd, d, amount));
    // } else {
    //     panic!("NO FILE ! NO GRAPH!")
    // }
}
