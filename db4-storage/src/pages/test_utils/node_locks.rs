use crate::{
    api::nodes::NodeSegmentOps, pages::node_store::NodeStorageInner,
    persist::strategy::PersistenceStrategy,
};
use std::{sync::mpsc, thread, time::Duration};

/// A writer that holds a full node segment gets a new segment without waiting for the segments
/// the free slots name, even while every one of those is write-locked by someone else.
pub fn check_reserve_with_full_segment_does_not_wait<
    NS: NodeSegmentOps<Extension = EXT>,
    EXT: PersistenceStrategy<NS = NS>,
>(
    nodes: &NodeStorageInner<NS, EXT>,
) {
    let num_slot_segments = nodes.num_segments();
    let held: Vec<_> = (0..num_slot_segments).map(|id| nodes.writer(id)).collect();
    let (sender, receiver) = mpsc::channel();

    let reserved = thread::scope(|scope| {
        scope.spawn(move || {
            // a segment no slot names, so the call cannot return early by finding its own segment
            let guard = nodes.grow_locked(num_slot_segments + 1).remove(0);
            let full = nodes.arc_writer(guard);
            while nodes.reserve_segment_row(full.segment).is_some() {}

            let (_, writer) = nodes.reserve_and_lock_segment(1, Some(&full));
            let _ = sender.send(writer.segment.segment_id());
        });
        let reserved = receiver.recv_timeout(Duration::from_secs(5));
        // lets a call that did wait finish, so the scope can join it
        drop(held);
        reserved
    });

    assert_eq!(
        reserved.expect("waited for a segment that another writer holds"),
        num_slot_segments + 1
    );
}
