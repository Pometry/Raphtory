mod logical_to_physical;
pub mod tgraph;
pub mod tgraph_storage;
pub(crate) mod timer;

#[cfg(test)]
mod test {
    use crate::{core::PropType, db::api::mutation::internal::InternalAdditionOps, prelude::*};

    #[test]
    fn test_neighbours_multiple_layers() {
        let g = Graph::default();
        let l_btc = g.resolve_layer(Some("btc")).unwrap().inner();
        let l_eth = g.resolve_layer(Some("eth")).unwrap().inner();
        let l_tether = g.resolve_layer(Some("tether")).unwrap().inner();
        let v1 = g.resolve_node(1).unwrap().inner();
        let v2 = g.resolve_node(2).unwrap().inner();
        let tx_sent_id = g
            .resolve_edge_property("tx_sent", PropType::I32, false)
            .unwrap()
            .inner();
        g.internal_add_edge(1.into(), v1, v2, &[(tx_sent_id, Prop::I32(10))], l_btc)
            .unwrap();
        g.internal_add_edge(1.into(), v1, v2, &[(tx_sent_id, Prop::I32(20))], l_eth)
            .unwrap();
        g.internal_add_edge(1.into(), v1, v2, &[(tx_sent_id, Prop::I32(70))], l_tether)
            .unwrap();

        let first = g
            .node(v1)
            .and_then(|n| n.layers(["btc", "eth"]).ok())
            .unwrap();

        let ns = first
            .out_neighbours()
            .into_iter()
            .map(|n| n.node)
            .collect::<Vec<_>>();

        assert_eq!(ns, vec![v2]);

        let first = g.node(v1).and_then(|n| n.layers(Layer::All).ok()).unwrap();
        let edges = first.out_edges().into_iter().collect::<Vec<_>>();

        assert_eq!(edges.len(), 1, "should only have one edge {:?}", edges);
    }

    #[test]
    fn simple_triangle() {
        let g = Graph::default();
        let v1 = 1u64;
        let v2 = 2;
        let v3 = 3;
        let vs = vec![(1i64, v1, v2), (2, v1, v3), (3, v2, v1), (4, v3, v2)];

        for (t, src, dst) in vs {
            g.add_edge(t, src, dst, NO_PROPS, None).unwrap();
        }

        let v = g.node(v1).and_then(|n| n.layers(Layer::All).ok()).unwrap();

        let ns = v
            .neighbours()
            .into_iter()
            .filter_map(|n| n.id().as_u64())
            .collect::<Vec<_>>();
        assert_eq!(ns, [v2, v3]);
    }
}
