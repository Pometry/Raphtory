use parking_lot::RwLock;
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};

pub static N: LazyLock<usize> = LazyLock::new(rayon::current_num_threads);

#[derive(Debug)]
pub struct FreeSegmentSlots {
    slots: Box<[RwLock<usize>]>,
    round_robin: AtomicUsize,
}

impl Default for FreeSegmentSlots {
    fn default() -> Self {
        Self::new(*N)
    }
}

impl From<Vec<RwLock<usize>>> for FreeSegmentSlots {
    fn from(value: Vec<RwLock<usize>>) -> Self {
        Self {
            slots: value.into(),
            round_robin: AtomicUsize::new(0),
        }
    }
}

impl FreeSegmentSlots {
    fn slot_idx(&self) -> usize {
        self.round_robin.fetch_add(1, Ordering::Relaxed) % self.slots.len()
    }
    /// get the next slot
    pub fn slot(&self) -> &RwLock<usize> {
        let idx = self.slot_idx();
        &self.slots[idx]
    }

    pub fn new(size: usize) -> Self {
        let slots = (0..size).map(RwLock::new).collect::<Box<[_]>>();
        Self {
            slots,
            round_robin: AtomicUsize::new(0),
        }
    }

    pub fn num_slots(&self) -> usize {
        self.slots.len()
    }
}
