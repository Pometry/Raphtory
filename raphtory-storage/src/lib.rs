use arrow::columnar_graph::TemporalColGraphFragment;

mod arrow;
mod ops;

#[derive(Debug)]
struct TemporalColumnarGraph {
    fragments: Vec<TemporalColGraphFragment>,
}

impl TemporalColumnarGraph {
    fn new(fragments: Vec<TemporalColGraphFragment>) -> Self {
        Self { fragments }
    }
}

#[cfg(test)]
mod test {
    use raphtory::prelude::{AdditionOps, Graph, NO_PROPS};

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
        println!("{:?}", tcg);
    }
}
