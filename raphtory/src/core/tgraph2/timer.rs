use std::sync::atomic::{AtomicI64, Ordering};

use serde::{Deserialize, Serialize};

pub(crate) trait TimeCounterTrait {
    fn cmp(a: i64, b: i64) -> bool;
    fn counter(&self) -> &AtomicI64;

    fn update(&self, new_value: i64) {
        let mut current_value = self.get();
        while Self::cmp(new_value, current_value) {
            match self.counter().compare_exchange_weak(
                current_value,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(value) => current_value = value,
            }
        }
    }
    fn get(&self) -> i64;
}

#[derive(Serialize, Deserialize)]
pub(crate) struct MinCounter {
    counter: AtomicI64,
}

impl MinCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicI64::new(i64::MAX),
        }
    }
}

impl TimeCounterTrait for MinCounter {
    fn get(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn cmp(new_value: i64, current_value: i64) -> bool {
        new_value < current_value
    }

    fn counter(&self) -> &AtomicI64 {
        &self.counter
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct MaxCounter {
    counter: AtomicI64,
}

impl MaxCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicI64::new(i64::MIN),
        }
    }
}

impl TimeCounterTrait for MaxCounter {
    fn cmp(a: i64, b: i64) -> bool {
        a > b
    }
    fn get(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    fn counter(&self) -> &AtomicI64 {
        &self.counter
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn min_counter() {
        let counter = MinCounter::new();
        counter.update(0);
        assert_eq!(counter.get(), 0);
        counter.update(1);
        assert_eq!(counter.get(), 0);
        counter.update(0);
        assert_eq!(counter.get(), 0);
        counter.update(-1);
        assert_eq!(counter.get(), -1);
    }

    #[test]
    fn max_counter() {
        let counter = MaxCounter::new();
        counter.update(0);
        assert_eq!(counter.get(), 0);
        counter.update(-1);
        assert_eq!(counter.get(), 0);
        counter.update(0);
        assert_eq!(counter.get(), 0);
        counter.update(1);
        assert_eq!(counter.get(), 1);
    }
}
