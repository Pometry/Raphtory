use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};

pub trait TimeCounterTrait {
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
    #[inline(always)]
    fn get(&self) -> i64 {
        self.counter().load(Ordering::Relaxed)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MinCounter {
    counter: AtomicI64,
}

impl Default for MinCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl MinCounter {
    pub fn new() -> Self {
        Self {
            counter: AtomicI64::new(i64::MAX),
        }
    }
}

impl TimeCounterTrait for MinCounter {
    fn cmp(new_value: i64, current_value: i64) -> bool {
        new_value < current_value
    }

    #[inline(always)]
    fn counter(&self) -> &AtomicI64 {
        &self.counter
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MaxCounter {
    counter: AtomicI64,
}

impl Default for MaxCounter {
    fn default() -> Self {
        Self::new()
    }
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
    #[inline(always)]
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
