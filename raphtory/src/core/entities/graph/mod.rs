pub mod tgraph;
pub mod tgraph_storage;
pub(crate) mod timer;

#[cfg(test)]
mod test {
    use crate::{
        core::Direction,
        db::api::mutation::internal::InternalAdditionOps,
        prelude::{IntoProp, Prop, NO_PROPS},
    };

    use super::{tgraph::InnerTemporalGraph, *};

    #[test]
    fn test_neighbours_multiple_layers() {
        let g: InnerTemporalGraph<2> = InnerTemporalGraph::default();
        let l_btc = g.resolve_layer(Some("btc"));
        let l_eth = g.resolve_layer(Some("eth"));
        let l_tether = g.resolve_layer(Some("tether"));
        g.inner().add_edge_internal(
            1.into(),
            1,
            2,
            vec![("tx_sent".to_string(), 10.into_prop())],
            l_btc,
        );
        g.inner().add_edge_internal(
            1.into(),
            1,
            2,
            vec![("tx_sent".to_string(), 20.into_prop())],
            l_eth,
        );
        g.inner().add_edge_internal(
            1.into(),
            1,
            2,
            vec![("tx_sent".to_string(), 70.into_prop())],
            l_tether,
        );

        let first = g.inner().vertex(0.into());

        let ns = first
            .neighbours(vec!["btc", "eth"], Direction::OUT)
            .map(|v| v.id().0)
            .collect::<Vec<_>>();

        assert_eq!(ns, vec![1]);

        let first = g.inner().vertex_arc(0.into());
        let edges = first
            .edge_tuples([0, 1, 2, 3, 4].into(), Direction::OUT)
            .collect::<Vec<_>>();

        assert_eq!(edges.len(), 1, "should only have one edge {:?}", edges);
    }

    #[test]
    fn simple_triangle() {
        let g: InnerTemporalGraph<2> = InnerTemporalGraph::default();
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        let empty: Vec<(String, Prop)> = vec![];
        for (t, src, dst) in vs {
            g.inner()
                .add_edge_internal(t.into(), src, dst, empty.clone(), 0);
        }

        let v = g.inner().vertex(0.into());

        let ns = v
            .neighbours(vec![], Direction::BOTH)
            .map(|v| v.id().0)
            .collect::<Vec<_>>();
        assert_eq!(ns, vec![1, 2]);
    }
}
