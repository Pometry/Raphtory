//! Triadic census
//! ...
//! ...
//! # Examples
//!
//! ```rust
//!
//! ```

use ahash::HashMap;
use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::*};
use itertools::Itertools;
use crate::db::graph::node::NodeView;
use rayon::iter::ParallelIterator;

/// Local triangle count - calculates the number of triangles (a cycle of length 3) a node participates in.
///
/// This function returns the number of pairs of neighbours of a given node which are themselves connected.
///
/// # Arguments
/// - `g`: Raphtory graph, this can be directed or undirected but will be treated as undirected
///
/// # Returns
/// Number of triangles associated with node v
///
///
///
///

pub fn tricode<'a,G:StaticGraphViewOps>(
    g: &G,
    v: NodeView<&'a G>,
    u: NodeView<&'a G>,
    w: NodeView<&'a G>,
) -> usize {

    /*
    Returns the integer code of the given triad.

    This is some fancy magic that comes from Batagelj and Mrvar's paper. It
    treats each edge joining a pair of ``v``, ``u``, and ``w`` as a bit in
    the binary representation of an integer.
    */

    let mut code = 0;

    if g.has_edge(v,u){
        code += 1; // bit 0
    }
    if g.has_edge(u,v){
        code += 2; // bit 1
    }
    if g.has_edge(v,w){
        code += 4; // bit 2
    }
    if g.has_edge(w,v){
        code += 8; // bit 3
    }
    if g.has_edge(u,w){
        code += 16; // bit 4
    }
    if g.has_edge(w,u){
        code += 32; // bit 5
    }

    code as usize
}

pub fn triadic_census<G: StaticGraphViewOps>(
    g: &G,
) -> [usize; 16]{

    let tricodes = [
        1, 2, 2, 3, 2, 4, 6, 8, 2, 6, 5, 7, 3, 8, 7, 11, 2, 6, 4, 8, 5, 9,
        9, 13, 6, 10, 9, 14, 7, 14, 12, 15, 2, 5, 6, 7, 6, 9, 10, 14, 4, 9,
        9, 12, 8, 13, 14, 15, 3, 7, 8, 11, 7, 12, 14, 15, 8, 14, 13, 15,
        11, 15, 15, 16
    ];

    // Map from tricode index to position in final result array (0-15)
    let tricode_to_index: HashMap<usize, usize> = tricodes
        .iter()
        .enumerate()
        .map(|(i, &code)| (i, code - 1))
        .collect();

     g.nodes().par_iter().map(|v| {
        let mut local_census = [0usize; 16];

        let vnbrs: Vec<_> = v.neighbours().iter().sorted_by(|a, b| a.id().cmp(&b.id())).collect();

        for u in vnbrs.clone(){
            if u.id() <= v.id() {
                continue;
            }

            let unbrs: Vec<_> = u.neighbours().iter().sorted_by(|a, b| a.id().cmp(&b.id())).collect();

            for w in vnbrs.iter()
                .merge_by(unbrs.iter(), |a, b| a.id() < b.id())
                .dedup_by(|a, b| a.id() == b.id())
                .skip_while(|w| w.id() < v.id()) {

                if ( u.id() < w.id() )  || (v.id() < w.id() && w.id() < u.id() && !w.neighbours().iter().contains(&v)) {
                    let code = tricode(g, v, u, *w);

                    if let Some(&index) = tricode_to_index.get(&code) {
                        local_census[index] += 1;
                    }
                }


            }
        }

        local_census
    })
    .reduce(
        || [0usize; 16],  // Identity value (empty census)
        |mut a, b| {      // Combine two census arrays
            for i in 0..16 {
                a[i] += b[i];
            }
            a
        }
    )
}

pub fn labeled_triadic_census<G: StaticGraphViewOps>(
    g: &G,
    use_new_triad_names: bool,
) -> HashMap<String, usize> {

    let triad_names = [
        "003", "012", "102", "021D", "021U", "021C", "111D", "111U",
        "030T", "030C", "201", "120D", "120U", "120C", "210", "300"
    ];

    let mapping_census_to_baseline = [
        13, 14, 15, 0, 3, 1, 4, 2, 6, 7, 5, 10, 8, 9, 11, 12
    ];

    let census = triadic_census(g);

    if use_new_triad_names {
        mapping_census_to_baseline.iter().enumerate()
            .map(|(i, &code)| (code.to_string(), census[i]))
            .collect()
    } else {
        triad_names.iter().enumerate()
            .map(|(i, &name)| (name.to_string(), census[i]))
            .collect()
    }
}
#[cfg(test)]
mod triadic_census_tests {
    use super::triadic_census;
    use crate::{
        db::{
            api::{mutation::AdditionOps, view::*},
            graph::graph::Graph,
        },
        prelude::NO_PROPS,
    };

    #[test]
    fn test_triadic_census() {
        let graph = Graph::new();
        let vs = vec![
            (1, 1, 2),
          (1, 2, 3),
          (1, 3, 1),
          (2, 3, 4),
          (2, 4, 1),
          (2, 4, 2)
        ];

        for (t, src, dst) in &vs {
            graph.add_edge(*t, *src, *dst, NO_PROPS, None).unwrap();
        }

        let triad_names = [
            "003", "012", "102", "021D", "021U", "021C", "111D", "111U",
            "030T", "030C", "201", "120D", "120U", "120C", "210", "300"
        ];

        let census = triadic_census(&graph);

        let expected_census = [0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 0];

        assert_eq!(census, expected_census, "Triadic census did not match expected values");

    }
}
