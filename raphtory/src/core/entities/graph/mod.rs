pub mod tgraph;
pub mod tgraph_storage;
pub(crate) mod timer;

#[cfg(test)]
mod test {
    use crate::{
        core::Direction,
        prelude::{AsProp, NO_PROPS},
    };

    use super::{tgraph::InnerTemporalGraph, *};

    #[test]
    fn test_neighbours_multiple_layers() {
        let g: InnerTemporalGraph<2> = InnerTemporalGraph::default();

        g.add_edge_internal(
            1,
            1,
            2,
            vec![("tx_sent".to_string(), 10.as_prop())],
            Some("btc"),
        );
        g.add_edge_internal(
            1,
            1,
            2,
            vec![("tx_sent".to_string(), 20.as_prop())],
            Some("eth"),
        );
        g.add_edge_internal(
            1,
            1,
            2,
            vec![("tx_sent".to_string(), 70.as_prop())],
            Some("tether"),
        );

        let first = g.vertex(0.into());

        let ns = first
            .neighbours(vec!["btc", "eth"], Direction::OUT)
            .map(|v| v.id().0)
            .collect::<Vec<_>>();

        assert_eq!(ns, vec![1]);


        let first =g.vertex_arc(0.into());
        let edges = first.edge_tuples(&[0, 1, 2, 3, 4], Direction::OUT).collect::<Vec<_>>();

        assert_eq!(edges.len(), 1, "should only have one edge {:?}", edges);
    }
}
