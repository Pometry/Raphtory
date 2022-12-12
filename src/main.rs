use std::env;
use std::fs;

use csv::StringRecord;
use docbrown::graph::TemporalGraph;
use docbrown::TemporalGraphStorage;
use std::time::{Duration, Instant};


fn parse_record(rec: &StringRecord) -> Option<(u64,u64,u64)> {
    let src = rec.get(3).and_then(|s|s.parse::<u64>().ok())?;
    let dst = rec.get(4).and_then(|s|s.parse::<u64>().ok())?;
    let t = rec.get(5).and_then(|s|s.parse::<u64>().ok())?;
    Some((src, dst, t))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut g = TemporalGraph::default();
    let now = Instant::now();

    if let Some(file_name) = args.get(1) {
        if let Ok(mut reader) = csv::Reader::from_path(file_name) {
           for rec_res in reader.records() {
               if let Ok(rec) = rec_res {
                   if let Some((src, dst, t)) = parse_record(&rec) {
                       g.add_vertex(src, t);
                       g.add_vertex(dst, t);
                       g.add_edge(src, dst, t);
                   }
               }
           } 
        }

        println!("Loaded {} vertices, took {} seconds", g.vertex_count(), now.elapsed().as_secs());
    } else {
        panic!("NO FILE ! NO GRAPH!")
    }
}
