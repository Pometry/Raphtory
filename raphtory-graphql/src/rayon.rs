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
    use std::{path::PathBuf, time::Duration};

    use reqwest::Client;

    use crate::{rayon::WRITE_POOL, server::RunningGraphServer, GraphServer};

    async fn assert_healthcheck_timeout(port: u16) {
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

    async fn start_server(port: u16) -> RunningGraphServer {
        let server = GraphServer::new(PathBuf::from("/tmp/health-check-test"), None, None).unwrap();
        let running = server.start_with_port(port).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        running
    }

    #[tokio::test]
    async fn test_deadlock_in_read_pool() {
        let port = 43871;
        let _running = start_server(port).await;
        for _ in 0..rayon::current_num_threads() {
            rayon::spawn(move || loop {
                std::hint::spin_loop();
            });
        }
        assert_healthcheck_timeout(port).await;
    }

    #[tokio::test]
    async fn test_deadlock_in_write_pool() {
        let port = 43872;
        let _running = start_server(port).await;
        for _ in 0..WRITE_POOL.current_num_threads() {
            WRITE_POOL.spawn(move || loop {
                std::hint::spin_loop();
            });
        }
        assert_healthcheck_timeout(port).await;
    }
}
