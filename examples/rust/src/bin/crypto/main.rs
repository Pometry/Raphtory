use chrono::NaiveDateTime;
use itertools::Itertools;
use raphtory::algorithms::generic_taint::generic_taint;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::core::time::TryIntoTime;
use raphtory::db::view_api::internal::BoxableGraphView;
use raphtory::db::view_api::layer::LayerOps;
use raphtory::db::view_api::time::WindowSet;
use raphtory::db::view_api::*;
use raphtory_io::graph_loader::example::stable_coins::stable_coin_graph;
use serde::Deserialize;
use std::env;
use std::time::Instant;

#[derive(Deserialize, std::fmt::Debug)]
pub struct StableCoin {
    block_number: String,
    transaction_index: u32,
    from_address: String,
    to_address: String,
    time_stamp: i64,
    contract_address: String,
    value: f64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let data_dir = if args.len() < 2 {
        None
    } else {
        Some(args.get(1).unwrap().to_string())
    };

    let g = stable_coin_graph(data_dir, true, 1);

    assert_eq!(g.num_vertices(), 1523333);
    assert_eq!(g.num_edges(), 2814155);

    assert_eq!(
        g.get_unique_layers().into_iter().sorted().collect_vec(),
        vec!["Dai", "LUNC", "USD", "USDP", "USDT", "USTC"]
    );

    println!("Pagerank");
    let now = Instant::now();
    let _ = unweighted_page_rank(&g, 20, None, None, true);
    println!("Time taken: {} secs", now.elapsed().as_secs());

    let now = Instant::now();

    let _ = unweighted_page_rank(&g, 20, None, None, true);
    println!("Time taken: {} secs", now.elapsed().as_secs());

    let now = Instant::now();
    let _ = unweighted_page_rank(&g.layer("USDT").unwrap(), 20, None, None, true);
    println!("Time taken: {} secs", now.elapsed().as_secs());

    println!("Generic taint");
    let now = Instant::now();
    let _ = generic_taint(
        &g.layer("USDT").unwrap(),
        None,
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        vec![],
    );
    println!("Time taken: {} secs", now.elapsed().as_secs());
}
