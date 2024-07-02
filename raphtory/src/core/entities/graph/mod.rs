pub mod tgraph;
pub mod tgraph_storage;
pub(crate) mod timer;

#[cfg(test)]
mod test {
    use super::tgraph::InternalGraph;
    use crate::{
        core::{entities::LayerIds, Direction, PropType},
        db::api::mutation::internal::InternalAdditionOps,
        prelude::Prop,
    };

    #[test]
    fn test_neighbours_multiple_layers() {
        let g: InternalGraph = InternalGraph::default();
        let l_btc = g.resolve_layer(Some("btc"));
        let l_eth = g.resolve_layer(Some("eth"));
        let l_tether = g.resolve_layer(Some("tether"));
        let v1 = g.resolve_node(1, None);
        let v2 = g.resolve_node(2, None);
        let tx_sent_id = g
            .resolve_edge_property("tx_sent", PropType::I32, false)
            .unwrap();
        g.inner()
            .add_edge_internal(1.into(), v1, v2, vec![(tx_sent_id, Prop::I32(10))], l_btc)
            .unwrap();
        g.inner()
            .add_edge_internal(1.into(), v1, v2, vec![(tx_sent_id, Prop::I32(20))], l_eth)
            .unwrap();
        g.inner()
            .add_edge_internal(
                1.into(),
                v1,
                v2,
                vec![(tx_sent_id, Prop::I32(70))],
                l_tether,
            )
            .unwrap();

        let first = g.inner().storage.nodes.entry(v1);

        let ns = first
            .neighbours(&vec![l_btc, l_eth].into(), Direction::OUT)
            .collect::<Vec<_>>();

        assert_eq!(ns, vec![v2]);

        let first = g.inner().storage.nodes.entry_arc(v1);
        let edges = first
            .edge_tuples(&[0, 1, 2, 3, 4].into(), Direction::OUT)
            .collect::<Vec<_>>();

        assert_eq!(edges.len(), 1, "should only have one edge {:?}", edges);
    }

    #[test]
    fn simple_triangle() {
        let g: InternalGraph = InternalGraph::new(2);
        let v1 = g.resolve_node(1, None);
        let v2 = g.resolve_node(2, None);
        let v3 = g.resolve_node(3, None);
        let vs = vec![(1, v1, v2), (2, v1, v3), (3, v2, v1), (4, v3, v2)];

        let empty: Vec<(usize, Prop)> = vec![];
        for (t, src, dst) in vs {
            g.inner()
                .add_edge_internal(t.into(), src, dst, empty.clone(), 0)
                .unwrap();
        }

        let v = g.inner().storage.get_node(v1);

        let ns = v
            .neighbours(&LayerIds::All, Direction::BOTH)
            .collect::<Vec<_>>();
        assert_eq!(ns, [v2, v3]);
    }
}
