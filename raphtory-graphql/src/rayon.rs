use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;
use tokio::sync::oneshot;

static WRITE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .build()
        .expect("failed to build threadpool")
});

/// Use the rayon threadpool to execute a task
///
/// Use this for long-running, compute-heavy work
pub async fn blocking_compute<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> R {
    let (send, recv) = oneshot::channel();
    rayon::spawn(move || {
        let _ = send.send(closure()); // this only errors if no-one is listening anymore
    });

    recv.await.expect("Function panicked in rayon::spawn")
}

/// Use a separate rayon threadpool to execute write tasks to avoid potential deadlocks
pub async fn blocking_write<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(closure: F) -> R {
    let (send, recv) = oneshot::channel();
    WRITE_POOL.spawn(move || {
        let _ = send.send(closure()); // this only errors if no-one is listening anymore
    });
    recv.await.expect("Function panicked in rayon::spawn")
}
