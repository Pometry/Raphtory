use raphtory_core::entities::graph::timer::{MaxCounter, MinCounter, TimeCounterTrait};
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub struct GraphStats {
    layers: boxcar::Vec<AtomicUsize>,
    earliest: MinCounter,
    latest: MaxCounter,
    num_nodes: AtomicUsize,
}

impl<I: IntoIterator<Item = usize>> From<I> for GraphStats {
    fn from(iter: I) -> Self {
        let layers = iter
            .into_iter()
            .map(AtomicUsize::new)
            .collect::<boxcar::Vec<_>>();

        let num_nodes = layers
            .get(0)
            .map_or(0, |n| n.load(std::sync::atomic::Ordering::Acquire));
        Self {
            layers,
            earliest: MinCounter::new(),
            latest: MaxCounter::new(),
            num_nodes: AtomicUsize::new(num_nodes),
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
        layers.push_with(|_| Default::default());
        Self {
            layers,
            earliest: MinCounter::new(),
            latest: MaxCounter::new(),
            num_nodes: AtomicUsize::new(0),
        }
    }

    pub fn load(counts: impl IntoIterator<Item = usize>, earliest: i64, latest: i64) -> Self {
        let mut stats: Self = counts.into();
        stats.earliest = MinCounter::from(earliest);
        stats.latest = MaxCounter::from(latest);
        stats
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
        self.num_nodes.fetch_add(
            (layer_id == 0) as usize,
            std::sync::atomic::Ordering::Release,
        );
        counter.fetch_add(1, std::sync::atomic::Ordering::Release)
    }

    #[inline(always)]
    pub fn num_nodes(&self) -> usize {
        self.num_nodes.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn get(&self, layer_id: usize) -> usize {
        let counter = self.get_or_create_layer(layer_id);
        counter.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn get_counter(&self, layer_id: usize) -> &AtomicUsize {
        self.get_or_create_layer(layer_id)
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
            loop {
                let new_layer_id = self.layers.push_with(|_| Default::default());
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
