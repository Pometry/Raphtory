// #[cfg(feature = "io")]
use raphtory::{
    db::graph::{graph::Graph, views::layer_graph::LayeredGraph},
    errors::GraphError,
};
// #[cfg(feature = "io")]
use raphtory_core::entities::LayerId;
// #[cfg(feature = "io")]
use storage::api::edges::EdgeRefOps;

// #[cfg(feature = "io")]
fn work(g: &Graph) -> Result<(), GraphError> {
    use std::time::Instant;

    use raphtory::{errors::GraphError, prelude::*};
    use raphtory_core::storage::timeindex::{AsTime, EventTime, TimeIndexOps};
    use raphtory_storage::{
        core_ops::CoreGraphOps, graph::edges::edge_storage_ops::EdgeStorageOps,
        layer_ops::InternalLayerOps,
    };
    use rayon::prelude::*;

    // let locked = g.core_graph().lock();
    // let edges = locked.edges();
    // let start = 1000i64;
    // let end = 1247528959958i64;
    // let layer_ids = g.layer_ids();
    // let num_layers = g.num_layers();
    let now = Instant::now();
    // let c = edges
    //     .par_iter(&raphtory_core::entities::LayerIds::All)
    //     .filter(|edge| {
    //         let e = edge.as_ref();
    //         // (1..num_layers).map(LayerId).any(|l| {
    //         //     e.has_layer_inner(l) && e.additions(l).active(EventTime::range(start..end))
    //         // })
    //         let l = LayerId(4);

    //         e.has_layer_inner(l) && e.additions(l).active(EventTime::range(start..end))
    //     })
    //     .count();
    // println!("Count edges took {:?}, found : {c}", now.elapsed());
    let l_g = g.layers(["HAS_CREATOR"]).unwrap();
    let edge_count = l_g.edges().into_iter().count();
    println!(
        "Counting layer edges [{edge_count}] took {:?}",
        now.elapsed()
    );

    // let w_l_g = l_g.window(1000, 1247528959958);

    // let now = Instant::now();
    // let edge_count = w_l_g.edges().into_iter().count();
    // println!(
    //     "Counting window layer edges [{edge_count}] took {:?}",
    //     now.elapsed()
    // );

    Ok(())
}

fn main() {
    #[cfg(feature = "io")]
    {
        let g = Graph::load(
            "/Volumes/Work/ldbc/social_network-sf100-CsvComposite-LongDateFormatter/graph",
        )
        .unwrap();
        work(&g).unwrap()
    }
}

// Counting layer edges [278083075] took 454.118125ms
// Counting window layer edges [0] took 3.694590875s

// Counting layer edges [278083075] took 460.288666ms
// Counting window layer edges [0] took 29.42822275s
