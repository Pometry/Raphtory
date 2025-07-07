use std::sync::atomic::AtomicUsize;

use raphtory_core::entities::graph::timer::{MaxCounter, MinCounter, TimeCounterTrait};

#[derive(Debug)]
pub struct GraphStats {
    layers: boxcar::Vec<AtomicUsize>,
    earliest: MinCounter,
    latest: MaxCounter,
}

impl<I: IntoIterator<Item = usize>> From<I> for GraphStats {
    fn from(iter: I) -> Self {
        let layers = iter.into_iter().map(AtomicUsize::new).collect();
        Self {
            layers,
            earliest: MinCounter::new(),
            latest: MaxCounter::new(),
        }
    }
}

impl Default for GraphStats {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStats {
    pub fn new() -> Self {
        let layers = boxcar::Vec::new();
        layers.push_with(|_| AtomicUsize::new(0));
        Self {
            layers,
            earliest: MinCounter::new(),
            latest: MaxCounter::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.layers.count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn update_time(&self, t: i64) {
        self.earliest.update(t);
        self.latest.update(t);
    }

    pub fn earliest(&self) -> i64 {
        self.earliest.get()
    }

    pub fn latest(&self) -> i64 {
        self.latest.get()
    }

    pub fn increment(&self, layer_id: usize) -> usize {
        let counter = self.get_or_create_layer(layer_id);
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get(&self, layer_id: usize) -> usize {
        let counter = self.get_or_create_layer(layer_id);
        counter.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn get_or_create_layer(&self, layer_id: usize) -> &AtomicUsize {
        if let Some(counter) = self.layers.get(layer_id) {
            return counter;
        }

        if self.layers.count() > layer_id {
            // something has allocated the layer, wait for it to be added
            loop {
                if let Some(counter) = self.layers.get(layer_id) {
                    return counter;
                } else {
                    // wait for the layer to be created
                    std::thread::yield_now();
                }
            }
        } else {
            // we need to create the layer
            self.layers.reserve(layer_id + 1 - self.layers.count());

            loop {
                let new_layer_id = self.layers.push_with(|_| AtomicUsize::new(0));
                if new_layer_id >= layer_id {
                    loop {
                        if let Some(counter) = self.layers.get(layer_id) {
                            return counter;
                        } else {
                            // wait for the layer to be created
                            std::thread::yield_now();
                        }
                    }
                }
            }
        }
    }
}
