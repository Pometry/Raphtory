#[cfg(test)]
mod test {
    use raphtory::{
        db::api::view::{internal::GraphTimeSemanticsOps, InternalTimeOps},
        prelude::*,
    };
    use raphtory_api::core::storage::timeindex::AsTime;

    #[test]
    fn test_view_start_end() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.start(), None);
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.end(), None);
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(1));
        e.delete(2, None).unwrap();
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(3));
        let w = g.window(g.timeline_start().unwrap(), g.timeline_end().unwrap());
        assert!(g.has_edge(1, 2));
        assert!(w.has_edge(1, 2));
        assert_eq!(w.start().map(|t| t.t()), Some(0));
        assert_eq!(w.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(w.end().map(|t| t.t()), Some(3));
        assert_eq!(w.timeline_end().map(|t| t.t()), Some(3));

        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(5));
    }
    #[test]
    fn test_materialize_window_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let ltg = g.latest_time_global();
        assert_eq!(ltg, Some(10));

        let wg = g.window(3, 5);

        let e = wg.edge(1, 2).unwrap();
        assert_eq!(e.earliest_time().map(|t| t.t()), Some(3));
        assert_eq!(e.latest_time().map(|t| t.t()), Some(3));
        let n1 = wg.node(1).unwrap();
        assert_eq!(n1.earliest_time().unwrap().t(), 3);
        assert_eq!(n1.latest_time().unwrap().t(), 3);
        let n2 = wg.node(2).unwrap();
        assert_eq!(n2.earliest_time().unwrap().t(), 3);
        assert_eq!(n2.latest_time().unwrap().t(), 3);

        let actual_lt = wg.latest_time();
        assert_eq!(actual_lt.unwrap().t(), 3);

        let actual_et = wg.earliest_time();
        assert_eq!(actual_et.unwrap().t(), 3);

        let gm = g
            .window(3, 5)
            .materialize()
            .unwrap()
            .into_persistent()
            .unwrap();

        let expected_et = gm.earliest_time();
        assert_eq!(actual_et, expected_et);
    }
}
