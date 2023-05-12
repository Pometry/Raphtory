use itertools::Itertools;
use raphtory::algorithms::generic_taint::generic_taint;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::db::view_api::layer::LayerOps;
use raphtory::db::view_api::*;
use raphtory::graph_loader::example::stable_coins::stable_coin_graph;
use serde::Deserialize;
use std::env;
use std::time::Instant;
use raphtory::algorithms::generic_taint_v2::generic_taint_v2;

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
    assert_eq!(g.num_edges(), 2814155);

    assert_eq!(
        g.get_unique_layers().into_iter().sorted().collect_vec(),
        vec![
            "0x6b175474e89094c44da98b954eedeac495271d0f",
            "0x8e870d67f660d95d5be530380d0ec0bd388289e1",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0xa47c8bf37f92abed4a126bda807a7b7498661acd",
            "0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9",
            "0xdac17f958d2ee523a2206206994597c13d831ec7"
        ]
    );

    // println!("Pagerank");
    // let now = Instant::now();
    // let _ = unweighted_page_rank(&g, 20, None, None);
    // println!("Time taken: {} secs", now.elapsed().as_secs());
    //
    // let now = Instant::now();
    // let _ = unweighted_page_rank(
    //     &g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7")
    //         .unwrap(),
    //     20,
    //     None,
    //     None,
    // );
    // println!("Time taken: {} secs", now.elapsed().as_secs());

    // let now = Instant::now();
    // let _ = unweighted_page_rank(
    //     &g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7")
    //         .unwrap(),
    //     20,
    //     None,
    //     None,
    // );
    // println!("Time taken: {} secs", now.elapsed().as_secs());

    println!("Generic taint");
    let now = Instant::now();
    let _ = generic_taint(
        &g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7")
            .unwrap(),
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        vec![],
    );
    println!("Time taken: {} secs", now.elapsed().as_secs());

    println!("Generic taint V2");
    let now = Instant::now();
    let _ = generic_taint_v2(
        &g.layer("0xdac17f958d2ee523a2206206994597c13d831ec7")
            .unwrap(),
        None,
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        vec![],
    );
    println!("Time taken: {} secs", now.elapsed().as_secs());
}
