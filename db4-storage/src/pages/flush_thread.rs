use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};

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
    stop: Arc<AtomicBool>,
    handler: Option<JoinHandle<()>>,
}

impl FlushThread {
    pub fn new<
        NS: NodeSegmentOps<Extension = EXT> + Send + Sync + 'static,
        ES: EdgeSegmentOps<Extension = EXT> + Send + Sync + 'static,
        EXT: Clone + Default + Send + Sync + 'static,
    >(
        nodes: Arc<NodeStorageInner<NS, EXT>>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handler = std::thread::spawn({
            let nodes = Arc::clone(&nodes);
            let stop = Arc::clone(&stop);
            move || {
                // Implement the logic to periodically flush the nodes to disk.
                loop {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    // Flush logic here
                    std::thread::sleep(std::time::Duration::from_millis(50)); // Example sleep duration

                    // let's do some stats, run over all the segments and decrement the event id then get the max and the min
                    // the event_id functions are atomic so no need to lock the segments

                    for (segment_id, event_id) in nodes
                        .segments()
                        .iter()
                        .map(|(i, ns)| (i, ns.decrement_event_id()))
                    {
                        if event_id <= 0 {
                            // If the event id is 0, we can flush this segment
                            let segment = nodes.segments().get(segment_id).unwrap();
                            // println!("Flushing from the flusher thread {segment_id}");
                            segment.flush();
                            segment.increment_event_id(500); // ignore this segment for the next 1000 events
                            // eprintln!("Triggered flush for {segment_id}")
                        }
                    }
                }
            }
        });

        Self {
            stop,
            handler: Some(handler),
        }
    }
}

impl Drop for FlushThread {
    fn drop(&mut self) {
        // Wait for the thread to finish
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(handler) = self.handler.take() {
            if let Err(e) = handler.join() {
                eprintln!("Flush thread join error: {e:?}");
            }
        }
    }
}
