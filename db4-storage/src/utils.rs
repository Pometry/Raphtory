use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};

#[derive(
    Clone,
    Debug,
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter2<I1, I2> {
    I1(I1),
    I2(I2),
}

#[derive(
    Copy,
    Clone,
    Iterator,
    ExactSizeIterator,
    DoubleEndedIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter3<I, J, K> {
    I(I),
    J(J),
    K(K),
}

#[derive(
    Copy,
    Clone,
    Iterator,
    ExactSizeIterator,
    DoubleEndedIterator,
    ParallelIterator,
    IndexedParallelIterator,
    FusedIterator,
)]
pub enum Iter4<I, J, K, L> {
    I(I),
    J(J),
    K(K),
    L(L),
}

/// Maps `items` on plain threads, in any order. Unlike a rayon worker, a thread waiting here
/// runs no other job, so the caller may hold a lock that other jobs take.
pub fn map_on_threads<T: Send, R: Send>(items: Vec<T>, map: impl Fn(T) -> R + Sync) -> Vec<R> {
    if items.len() < 2 {
        return items.into_iter().map(map).collect();
    }
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let threads = threads.min(items.len());
    let queue = std::sync::Mutex::new(items.into_iter());
    let next = || {
        queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next()
    };
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| scope.spawn(|| std::iter::from_fn(next).map(&map).collect::<Vec<R>>()))
            .collect();
        let joined = workers.into_iter().flat_map(|worker| match worker.join() {
            Ok(mapped) => mapped,
            Err(panic) => std::panic::resume_unwind(panic),
        });
        joined.collect()
    })
}

#[cfg(test)]
mod test {
    use super::map_on_threads;

    #[test]
    fn map_on_threads_maps_every_item() {
        for len in [0usize, 1, 2, 1000] {
            let mut mapped = map_on_threads((0..len).collect(), |i| i * 2);
            mapped.sort();
            assert_eq!(mapped, (0..len).map(|i| i * 2).collect::<Vec<_>>());
        }
    }
}
