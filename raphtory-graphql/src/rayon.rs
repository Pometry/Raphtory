use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;
use tokio::sync::oneshot;

static WRITE_POOL: LazyLock<ThreadPool> =
    LazyLock::new(|| ThreadPoolBuilder::new().build().unwrap());

static COMPUTE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .stack_size(16 * 1024 * 1024)
        .build()
        .unwrap()
});

/// Use the rayon threadpool to execute a task
///
/// Use this for long-running, compute-heavy work
pub async fn blocking_compute<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> R {
    let (send, recv) = oneshot::channel();
    COMPUTE_POOL.spawn_fifo(move || {
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
    use crate::{
        rayon::{COMPUTE_POOL, WRITE_POOL},
        routes::Health,
        GraphServer,
    };
    use raphtory::db::api::storage::storage::Config;
    use reqwest::{Client, StatusCode};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_deadlock_in_read_pool() {
        test_pool_lock(43871, |lock| {
            COMPUTE_POOL.spawn_broadcast(move |_| {
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
        let tempdir = TempDir::new().unwrap();
        let server =
            GraphServer::new(tempdir.path().to_path_buf(), None, None, Config::default()).unwrap();
        let _running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await; // this is to wait for the server to be up
        let lock = Arc::new(Mutex::new(()));
        let _guard = lock.lock();
        let lock_clone = lock.clone();
        pool_lock(lock_clone);
        let client = Client::new();

        let req = client.get(format!("http://localhost:{port}/health"));
        let response = req.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health: Health = response.json().await.unwrap();
        assert_eq!(health.healthy, false);

        // with default timeout (10s) a 8s timeout causes the http client to finish first
        let req = client.get(format!("http://localhost:{port}/health"));
        let result = req.timeout(Duration::from_secs(8)).send().await.err();
        assert!(result.unwrap().is_timeout());

        // However, with a custom timeout of 5s, now the health check finishes first
        let req = client.get(format!("http://localhost:{port}/health?timeout=5"));
        let response = req.timeout(Duration::from_secs(8)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health: Health = response.json().await.unwrap();
        assert_eq!(health.healthy, false);
    }
}
