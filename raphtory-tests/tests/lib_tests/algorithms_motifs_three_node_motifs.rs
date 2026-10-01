#[cfg(test)]
mod three_node_motifs_test {
    use raphtory::algorithms::motifs::three_node_motifs::{
        init_tri_count, map2d, TriangleEdge, TwoNodeCounter, TwoNodeEvent, INCOMING, OUTGOING,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::info;

    #[test]
    fn map_test() {
        assert_eq!(map2d(1, 1), 3);
    }

    #[test]
    fn two_node_test() {
        global_info_logger();
        let events = vec![
            TwoNodeEvent {
                dir: OUTGOING,
                time: 1,
            },
            TwoNodeEvent {
                dir: INCOMING,
                time: 2,
            },
            TwoNodeEvent {
                dir: INCOMING,
                time: 3,
            },
        ];
        let mut twonc = TwoNodeCounter {
            count1d: [0; 2],
            count2d: [0; 4],
            count3d: [0; 8],
        };
        twonc.execute(&events, 5);
        info!("motifs are {:?}", twonc.count3d);
    }

    #[test]
    fn triad_test() {
        global_info_logger();
        let events = [(true, 0, 1, 1, 1), (false, 1, 0, 1, 2), (false, 0, 0, 0, 3)]
            .iter()
            .map(|x| TriangleEdge {
                uv_edge: x.0,
                uorv: x.1,
                nb: x.2,
                dir: x.3,
                time: x.4,
            })
            .collect::<Vec<_>>();
        let mut triangle_count = init_tri_count(3);
        triangle_count.execute(&events, 5);
        info!("triangle motifs are {:?}", triangle_count.final_counts);
    }
}
