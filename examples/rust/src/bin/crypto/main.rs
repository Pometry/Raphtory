use itertools::Itertools;
use raphtory::{
    algorithms::{
        centrality::pagerank::unweighted_page_rank,
        pathing::temporal_reachability::temporally_reachable_nodes,
    },
    db::api::view::*,
    graph_loader::example::stable_coins::stable_coin_graph,
};
use std::{env, time::Instant};

fn main() {
    let args: Vec<String> = env::args().collect();

    let data_dir = if args.len() < 2 {
        None
    } else {
        Some(args.get(1).unwrap().to_string())
    };

    let g = stable_coin_graph(data_dir, true);

    assert_eq!(g.count_vertices(), 1523333);
    assert_eq!(g.count_edges(), 2814155);

    assert_eq!(
        g.unique_layers().into_iter().sorted().collect_vec(),
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
    let _ = temporally_reachable_nodes(
        &g.layer("USDT").unwrap(),
        None,
        20,
        1651105815,
        vec!["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"],
        None,
    );
    println!("Time taken: {} secs", now.elapsed().as_secs());
}
