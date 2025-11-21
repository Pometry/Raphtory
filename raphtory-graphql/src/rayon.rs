use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;
use tokio::sync::oneshot;

static WRITE_POOL: LazyLock<ThreadPool> =
    LazyLock::new(|| ThreadPoolBuilder::new().build().unwrap());

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

#[cfg(test)]
mod deadlock_tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use reqwest::Client;

    use crate::{rayon::WRITE_POOL, GraphServer};

    #[tokio::test]
    async fn test_deadlock_in_read_pool() {
        test_pool_lock(43871, |lock| {
            rayon::spawn_broadcast(move |_| {
                let _guard = lock.lock().unwrap();
            });
        })
        .await;
    }

    #[tokio::test]
    async fn test_deadlock_in_write_pool() {
        test_pool_lock(43872, |lock| {
            WRITE_POOL.spawn_broadcast(move |_| {
                let _guard = lock.lock().unwrap();
            });
        })
        .await;
    }

    async fn test_pool_lock(port: u16, pool_lock: impl FnOnce(Arc<Mutex<()>>)) {
        let server = GraphServer::new(PathBuf::from("/tmp/health-check-test"), None, None).unwrap();
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await; // this is to wait for the server to be up
        let lock = Arc::new(Mutex::new(()));
        let _guard = lock.lock();
        let lock_clone = lock.clone();
        pool_lock(lock_clone);
        let client = Client::new();
        let result = client
            .get(format!("http://localhost:{port}/health"))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        let error =
            result.expect_err("The request should timeout since the thread pool should be locked");
        dbg!(&error);
        assert!(error.is_timeout());
    }
}
