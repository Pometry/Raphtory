use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, RefreshKind};

use crate::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    pages::{edge_store::EdgeStorageInner, node_store::NodeStorageInner},
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
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
        edges: Arc<EdgeStorageInner<ES, EXT>>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        // let handler = std::thread::spawn({
        //     let nodes = Arc::clone(&nodes);
        //     let edges = Arc::clone(&edges);
        //     let stop = Arc::clone(&stop);
        //     move || {
        //         // Implement the logic to periodically flush the nodes to disk.
        //         let mem_refresh = MemoryRefreshKind::nothing().with_ram();
        //         let mut system = sysinfo::System::new_with_specifics(
        //             RefreshKind::nothing()
        //                 .with_memory(mem_refresh)
        //                 .with_processes(ProcessRefreshKind::nothing().with_memory()),
        //         );

        //         let pid = sysinfo::get_current_pid().unwrap();
        //         loop {
        //             if stop.load(std::sync::atomic::Ordering::Relaxed) {
        //                 break;
        //             }
        //             // Flush logic here
        //             std::thread::sleep(std::time::Duration::from_secs(5)); // Example sleep duration
        //             system.refresh_memory_specifics(mem_refresh);
        //             system.refresh_processes_specifics(
        //                 sysinfo::ProcessesToUpdate::Some(&[pid]),
        //                 false,
        //                 ProcessRefreshKind::nothing().with_memory(),
        //             );
        //             let available_memory = system.available_memory() / 1024 / 1024; // in MB
        //             let process = system.process(pid).unwrap();
        //             let process_memory = process.memory() / 1024 / 1024; // in MB
        //             println!(
        //                 "Current system available memory {available_memory} MB, process memory {process_memory} MB"
        //             );
        //             // TODO: flush based on memory usage
        //         }
        //     }
        // });

        Self {
            stop,
            handler: None,
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
