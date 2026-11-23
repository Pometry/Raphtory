use db4_storage::{
    Extension, Layer, LocalPOS,
    api::nodes::NodeSegmentOps,
    persist::{config::BaseConfig, strategy::PersistenceStrategy},
};
use raphtory_api::core::entities::GID;
use std::sync::atomic::Ordering;

fn new_graph() -> Layer<Extension> {
    Layer::new(None, Extension::new(None, BaseConfig::new(10, 10)).unwrap()).unwrap()
}

#[test]
fn growth_while_unlocked_is_counted_once() {
    let graph = new_graph();
    let nodes = graph.nodes();
    let mut a = nodes.writer(0);
    let tracker = a.writer.memory_tracker().clone();
    let tracked_before = tracker.load(Ordering::Relaxed);
    let size_before = a.writer.est_size();

    a.unlocked(|| {
        let mut b = nodes.writer(0);
        let pos = LocalPOS(nodes.reserve_segment_row(b.segment).unwrap());
        b.store_node_id(pos, GID::U64(1));
    });
    let growth = a.writer.est_size() - size_before;
    drop(a);

    assert!(growth > 0);
    assert_eq!(tracker.load(Ordering::Relaxed) - tracked_before, growth);
}

#[test]
fn a_flush_while_unlocked_does_not_underflow() {
    let graph = new_graph();
    let nodes = graph.nodes();
    {
        let mut w = nodes.writer(0);
        let pos = LocalPOS(nodes.reserve_segment_row(w.segment).unwrap());
        w.store_node_id(pos, GID::U64(1));
    }
    let mut a = nodes.writer(0);
    assert!(a.writer.est_size() > 0);
    let segment = a.segment;

    // a flush empties the in-memory segment and resets its size to 0
    a.unlocked(|| {
        segment.head_mut_arc().take();
    });
    drop(a);
}
