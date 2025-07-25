use std::{
    collections::BinaryHeap,
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

                    let mut total_est_size = 0;
                    let mut smallest_event_id_segments = BinaryHeap::new();
                    // top 10 segments with the smallest event id
                    for (segment_id, event_id) in nodes
                        .segments()
                        .iter()
                        .map(|(i, ns)| (i, ns.decrement_event_id()))
                    {
                        let segment = nodes.segments().get(segment_id).unwrap();
                        if event_id <= 0 {
                            // If the event id is 0, we can flush this segment
                            // println!("Flushing from the flusher thread {segment_id}");
                            segment.flush();
                            segment.increment_event_id(500); // ignore this segment for the next 1000 events
                            // eprintln!("Triggered flush for {segment_id}")
                        }
                        if smallest_event_id_segments.len() < 10 {
                            smallest_event_id_segments.push((event_id, segment_id));
                        } else if let Some((top_event_id, _)) =
                            smallest_event_id_segments.peek().cloned()
                        {
                            if event_id < top_event_id {
                                smallest_event_id_segments.pop();
                                smallest_event_id_segments.push((event_id, segment_id));
                            }
                        }

                        total_est_size += segment.est_size();

                        println!(
                            "TOTAL EST SIZE {}GB",
                            total_est_size as f64 / (1024f64 * 1024f64)
                        )
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
