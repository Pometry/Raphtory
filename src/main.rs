#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;

use csv::StringRecord;
use docbrown::graph::TemporalGraph;
use docbrown::Prop;
// use docbrown::tcell::TCell;
use itertools::Itertools;
use std::time::Instant;

fn parse_record(rec: &StringRecord) -> Option<(u64, u64, u64, u64)> {
    let src = rec.get(3).and_then(|s| s.parse::<u64>().ok())?;
    let dst = rec.get(4).and_then(|s| s.parse::<u64>().ok())?;
    let t = rec.get(5).and_then(|s| s.parse::<u64>().ok())?;
    let amount = rec.get(7).and_then(|s| s.parse::<u64>().ok())?;
    Some((src, dst, t, amount))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut g = TemporalGraph::default();
    //
    // let mut m: HashMap<u64, TCell<u64>> = HashMap::default();

    let now = Instant::now();

    if let Some(file_name) = args.get(1) {
        if let Ok(mut reader) = csv::Reader::from_path(file_name) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t, amount)) = parse_record(&rec) {
                        g.add_vertex(src, t);
                        g.add_vertex(dst, t);
                        g.add_edge_props(src, dst, t, vec![("amount".into(), Prop::U64(amount))]);
                        //
                        // m.entry(src)
                        //     .and_modify(|cell| cell.set(t, src))
                        //     .or_insert_with(|| TCell::new(t, src));

                        // m.entry(dst)
                        //     .and_modify(|cell| cell.set(t, dst))
                        //     .or_insert_with(|| TCell::new(t, dst));
                    }
                }
            }
        }

        println!(
            "Loaded {} vertices, took {} seconds",
            g.len(),
            now.elapsed().as_secs()
        );

        // println!("VERTEX,DEGREE,OUT_DEGREE,IN_DEGREE");
        // g.iter_vertices()
        //     .map(|v| {
        //         let id = v.global_id();
        //         let out_d = v.outbound_degree();
        //         let in_d = v.inbound_degree();
        //         let d = out_d + in_d;
        //         let out_sum: u64 = v
        //             .outbound()
        //             .flat_map(|e| {
        //                 e.props("amount").flat_map(|(t, p)| match p {
        //                     Prop::U64(amount) => Some(amount),
        //                     _ => None,
        //                 })
        //             })
        //             .sum();

        //         (id, d, out_d, out_sum, in_d)
        //     })
        //     .sorted_by_cached_key(|(_, _, _, _, d)| *d)
        //     .into_iter()
        //     .for_each(|(v, d, outd, amount, ind)| println!("{},{},{},{},{}", v, ind, outd, d, amount));
    } else {
        panic!("NO FILE ! NO GRAPH!")
    }
}
