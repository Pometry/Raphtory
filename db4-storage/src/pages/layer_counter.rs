use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
pub struct LayerCounter {
    layers: boxcar::Vec<AtomicUsize>,
}

impl <I: IntoIterator<Item = usize>> From<I> for LayerCounter {
    fn from(iter: I) -> Self {
        let counts = iter.into_iter().map(|c| AtomicUsize::new(c)).collect();
        let layers = boxcar::Vec::from(counts);
        Self { layers }
    }
}

impl LayerCounter {
    pub fn new() -> Self {
        let layers = boxcar::Vec::new();
        for _ in 0..16 {
            let id = layers.push_with(|_| AtomicUsize::new(0));
            while layers.get(id).is_none() {
                // wait for the layer to be created
                std::thread::yield_now();
            }
        }
        Self { layers }
    }
    
    pub fn len(&self) -> usize {
        self.layers.count()
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

        if self.layers.count() >= layer_id + 1 {
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
