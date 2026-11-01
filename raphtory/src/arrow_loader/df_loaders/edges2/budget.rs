use crossbeam_channel::{bounded, Receiver, Sender};

/// Caps how many chunks are between being read and being fully written, so the input is never
/// read much faster than the owners can write it.
pub(super) struct InFlightBudget {
    returned: Sender<()>,
    available: Receiver<()>,
    size: usize,
}

/// A chunk's place in the budget, given back when the last message about the chunk is dropped.
pub(super) struct Slot(Sender<()>);

impl InFlightBudget {
    pub fn new(size: usize) -> Self {
        let (returned, available) = bounded(size);
        for _ in 0..size {
            let _ = returned.send(());
        }
        Self {
            returned,
            available,
            size,
        }
    }

    pub fn take_slot(&self) -> Slot {
        self.available
            .recv()
            .expect("the budget holds a sender, so its channel stays open");
        Slot(self.returned.clone())
    }

    /// Returns once every chunk taken has been fully written.
    pub fn wait_until_all_returned(&self) {
        for _ in 0..self.size {
            let _ = self.available.recv();
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
