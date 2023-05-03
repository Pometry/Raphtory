use std::env;
use std::path::Path;
use raphtory::algorithms::generic_taint::generic_taint;
use raphtory::db::view_api::*;
use raphtory::graph_loader::example::stable_coins::stable_coin_graph;
use serde::Deserialize;
use std::time::Instant;
use raphtory::algorithms::pagerank::unweighted_page_rank;

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

    let g = stable_coin_graph(data_dir, 1);

    assert_eq!(g.num_vertices(), 1523333);
    assert_eq!(g.num_edges(), 2871269);

    println!("Pagerank");
    let now = Instant::now();
    let _ = unweighted_page_rank(&g, i64::MIN..i64::MAX, 20);
    println!("Time taken: {} secs", now.elapsed().as_secs());

    let now = Instant::now();
    let _ = unweighted_page_rank(&g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap(), i64::MIN..i64::MAX, 20);
    println!("Time taken: {} secs", now.elapsed().as_secs());

    println!("Generic taint");
    let now = Instant::now();
    let _ = generic_taint(
        &g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap(),
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        vec![],
    );
    println!("Time taken: {}", now.elapsed().as_secs());
}
