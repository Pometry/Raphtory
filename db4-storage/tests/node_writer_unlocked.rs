use db4_storage::{
    Extension, Layer, LocalPOS,
    api::nodes::NodeSegmentOps,
    persist::{config::BaseConfig, strategy::PersistenceStrategy},
};
use raphtory_api::core::entities::GID;
use std::sync::atomic::Ordering;

fn new_graph() -> Layer<Extension> {
    let config = BaseConfig::default().with_max_node_page_len(10).with_max_edge_page_len(10);
    Layer::new(None, Extension::new(None, config).unwrap()).unwrap()
}

#[test]
fn growth_while_unlocked_is_counted_once() {
    let graph = new_graph();
    let nodes = graph.nodes();
    let mut writer = nodes.writer(0);
    let tracker = writer.head.global_memory_tracker().clone();
    let tracked_before = tracker.load(Ordering::Relaxed);
    let size_before = writer.head.est_size();

    writer.unlocked(|| {
        let mut b = nodes.writer(0);
        let pos = LocalPOS(nodes.reserve_segment_row(b.segment).unwrap());
        b.store_node_id(pos, GID::U64(1));
    });

    let growth = writer.head.est_size() - size_before;
    drop(writer);

    assert!(growth > 0);
    assert_eq!(tracker.load(Ordering::Relaxed) - tracked_before, growth);
}

#[test]
fn a_flush_while_unlocked_does_not_underflow() {
    let graph = new_graph();
    let nodes = graph.nodes();

    {
        let mut writer = nodes.writer(0);
        let pos = LocalPOS(nodes.reserve_segment_row(writer.segment).unwrap());
        writer.store_node_id(pos, GID::U64(1));
    }

    let mut writer = nodes.writer(0);
    assert!(writer.head.est_size() > 0);
    let segment = writer.segment;

    // a flush empties the in-memory segment and resets its size to 0
    writer.unlocked(|| {
        segment.head_mut_arc().take();
    });

    drop(writer);
}
