use std::sync::Arc;

use arrow::columnar_graph::TemporalColGraphFragment;
use itertools::Itertools;
use raphtory::core::{entities::vertices::vertex_ref::VertexRef, Direction};

mod arrow;
mod ops;

#[derive(Debug)]
struct TemporalColumnarGraph {
    fragments: Vec<Arc<TemporalColGraphFragment>>,
}

impl TemporalColumnarGraph {
    fn new(fragments: Vec<TemporalColGraphFragment>) -> Self {
        Self {
            fragments: fragments.into_iter().map(Arc::new).collect(),
        }
    }

    pub fn neighbours<V: Into<VertexRef>>(
        &self,
        v: V,
        dir: Direction,
    ) -> impl Iterator<Item = VertexRef> {
        if let VertexRef::External(gid) = v.into() {
            self.fragments
                .clone() // likely not ideal but it gets rid of the lifetime
                .into_iter()
                .filter_map(|fragment| {
                    let vid = fragment.resolve_vertex_id(gid)?;
                    let iter = fragment
                        .neighbours(vid, dir)?
                        .filter_map(move |v| fragment.global_id(v))
                        .map(VertexRef::External);
                    Some(iter)
                })
                .kmerge()
        } else {
            panic!("Internal vertices not supported")
        }
    }
}

#[cfg(test)]
mod test {
    use raphtory::{
        core::Direction,
        prelude::{AdditionOps, Graph, NO_PROPS},
    };

    use super::*;
    use crate::arrow::columnar_graph::TemporalColGraphFragment;

    #[test]
    fn two_fragments_same_vertex() {
        let g1 = Graph::new();

        g1.add_edge(1, 1, 2, NO_PROPS, None)
            .expect("Failed to add edge");
        g1.add_edge(2, 2, 3, NO_PROPS, None)
            .expect("Failed to add edge");

        let g2 = Graph::new();

        g2.add_edge(4, 2, 4, NO_PROPS, None)
            .expect("Failed to add edge");
        g2.add_edge(0, 1, 2, NO_PROPS, None)
            .expect("Failed to add edge");

        let frag1 = TemporalColGraphFragment::from_graph(&g1);
        let frag2 = TemporalColGraphFragment::from_graph(&g2);

        let tcg = TemporalColumnarGraph::new(vec![frag1, frag2]);

        let out_neighbours = tcg.neighbours(2, Direction::OUT).collect::<Vec<_>>();
        assert_eq!(
            out_neighbours,
            vec![VertexRef::External(3), VertexRef::External(4)]
        );

        println!("{:?}", tcg);
    }
}
