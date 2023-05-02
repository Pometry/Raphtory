use raphtory::algorithms::generic_taint::generic_taint;
use raphtory::db::view_api::*;
use raphtory::graph_loader::example::stable_coins::stable_coin_graph;
use serde::Deserialize;
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
    let g = stable_coin_graph(None, 1);

    assert_eq!(g.num_vertices(), 1523333);
    assert_eq!(g.num_edges(), 2814155);

    let now = Instant::now();
    let r = generic_taint(
        &g,
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        vec![],
    );
    println!("Time taken: {}", now.elapsed().as_secs());
    println!("{:?}", r);
}
