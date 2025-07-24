use std::{sync::Arc, thread::JoinHandle};

use itertools::Itertools;

use crate::{
    NS,
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::node_store::NodeStorageInner,
    persist::strategy::PersistentStrategy,
};

// This should be a rayon thread with a reference to Arc<NodeStorageInner> that will flush the data to disk periodically.
#[derive(Debug)]
pub(crate) struct FlushThread {
    handler: JoinHandle<()>,
}

impl FlushThread {
    pub fn new<
        NS: NodeSegmentOps<Extension = EXT> + Send + Sync + 'static,
        ES: EdgeSegmentOps<Extension = EXT> + Send + Sync + 'static,
        EXT: Clone + Default + Send + Sync + 'static,
    >(
        nodes: Arc<NodeStorageInner<NS, EXT>>,
    ) -> Self {
        let handler = std::thread::spawn({
            let nodes = Arc::clone(&nodes);
            move || {
                // Implement the logic to periodically flush the nodes to disk.
                loop {
                    // Flush logic here
                    std::thread::sleep(std::time::Duration::from_millis(100)); // Example sleep duration

                    // let's do some stats, run over all the segments and decrement the event id then get the max and the min
                    // the event_id functions are atomic so no need to lock the segments

                    let min = Iterator::min(
                        nodes
                            .segments()
                            .iter()
                            .map(|(i, ns)| (i, ns.decrement_event_id())),
                    );

                    if let Some((segment_id, event_id)) = min {
                        if event_id <= 0 {
                            // If the event id is 0, we can flush this segment
                            let segment = nodes.segments().get(segment_id).unwrap();
                            segment.flush();
                            segment.increment_event_id(1000); // ignore this segment for the next 1000 events
                            // eprintln!("Triggered flush for {segment_id}")
                        }
                    }

                    // println!("Min Max event ids for segments {min_max:?}");
                }
            }
        });

        Self { handler }
    }
}
