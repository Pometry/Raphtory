use rayon::{ThreadPool, ThreadPoolBuilder};
#[cfg(test)]
use std::sync::Mutex;
use std::{
    any::Any,
    panic::{self, AssertUnwindSafe},
    sync::{LazyLock, OnceLock},
};
use tokio::sync::{oneshot, Semaphore};
use tracing::{error, warn};

/// Process-global: set once by the first server, shared by every later one.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PoolSettings {
    /// Max graph loads decoding at once; bounds peak memory. `None` = cores / 4, at least 2.
    pub max_concurrent_loads: Option<usize>,
    /// Threads caching filtered graph views. `None` = 4 with more than 6 cores, else cores / 2.
    pub caching_threads: Option<usize>,
    /// Max compute jobs handed to the compute pool at once; bounds how deep jobs can nest on a
    /// worker's stack. `None` = 8 times the compute threads.
    pub max_concurrent_compute: Option<usize>,
    /// Stack size in bytes of each compute thread. `None` = [`STACK_PER_COMPUTE_JOB`] per
    /// compute permit, at least [`MIN_COMPUTE_STACK_SIZE`].
    pub compute_stack_size: Option<usize>,
}

static SETTINGS: OnceLock<PoolSettings> = OnceLock::new();

/// First caller wins; a later, different configuration is ignored with a warning.
pub fn configure_pools(settings: PoolSettings) {
    let applied = SETTINGS.get_or_init(|| settings.clone());
    if *applied != settings {
        warn!(?applied, ignored = ?settings, "rayon pools already configured; keeping the first configuration");
    }
}

fn settings() -> &'static PoolSettings {
    SETTINGS.get_or_init(PoolSettings::default)
}

fn cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

pub static WRITE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .thread_name(|t| format!("RAP-write-{t}"))
        .build()
        .unwrap()
});

fn compute_threads() -> usize {
    cores().max(2)
}

/// Stack budgeted per compute job that can nest on a compute thread (see [`COMPUTE_PERMITS`]).
/// The overflow seen in CI took roughly 80KB a request; this leaves headroom for heavier ones.
const STACK_PER_COMPUTE_JOB: usize = 256 * 1024;

/// The default compute stack is never smaller than this, the fixed size it had before it scaled
/// with the permits.
const MIN_COMPUTE_STACK_SIZE: usize = 64 * 1024 * 1024;

/// Stack of a compute thread, sized to fit [`compute_permits`] nested jobs unless configured. It is
/// address space reserved per thread; only the pages a thread actually touches use memory.
fn compute_stack_size() -> usize {
    settings().compute_stack_size.unwrap_or_else(|| {
        compute_permits()
            .saturating_mul(STACK_PER_COMPUTE_JOB)
            .max(MIN_COMPUTE_STACK_SIZE)
    })
}

pub static COMPUTE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .stack_size(compute_stack_size())
        .num_threads(compute_threads())
        .thread_name(|t| format!("RAP-compute-{t}"))
        .build()
        .unwrap()
});

pub static EVICT_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .stack_size(16 * 1024 * 1024)
        .num_threads(1)
        .thread_name(|t| format!("RAP-evict-{t}"))
        .build()
        .unwrap()
});

/// How many compute jobs may be on the compute pool at once. Generous, so short queries rarely
/// wait behind long ones, while still bounding the nesting (see [`COMPUTE_PERMITS`]). The stack
/// size follows it (see [`compute_stack_size`]), so it scales with the cores without a cap.
fn compute_permits() -> usize {
    match settings().max_concurrent_compute {
        Some(0) => {
            warn!("max_concurrent_compute = 0 would run no compute job; using 1");
            1
        }
        Some(permits) => permits,
        None => 8 * compute_threads(),
    }
}

/// A worker that blocks in `join` (any parallel iterator does) runs whatever job is queued on
/// the pool, including a whole other request, on top of its own stack, and that one can block
/// and take another in turn. With an unbounded backlog the nesting is unbounded too: under
/// load, 215 requests nested on one 16MB `RAP-compute` stack and overflowed it, which kills the
/// process with a bare SIGSEGV (no Rust overflow message, as Python owns the signal handlers).
/// Only jobs holding a permit are ever on the pool, so a worker can nest at most that many.
static COMPUTE_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(compute_permits()));

/// Why a compute job gave no result. What a client sees is only this text: the panic message
/// is logged where it is caught, as it can name server internals.
#[derive(Debug, thiserror::Error)]
pub enum ComputeError {
    #[error("the computation failed")]
    Panicked(String),
    #[error("the compute job was dropped before it ran")]
    Dropped,
    #[error("the compute queue is closed")]
    Closed,
}

/// Query work on the compute pool. Waits for a compute permit first, see [`COMPUTE_PERMITS`].
/// A panic in `closure` comes back as [`ComputeError::Panicked`], and is logged.
pub async fn blocking_compute<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> Result<R, ComputeError> {
    let permit = COMPUTE_PERMITS
        .acquire()
        .await
        .map_err(|_| ComputeError::Closed)?;
    spawn_compute(move || {
        let _permit = permit;
        closure()
    })
    .await
}

/// A no-op round trip through the compute pool, for the health check. It skips the permit, as
/// it is meant to show whether the pool itself is still running jobs, and an empty job cannot
/// nest anything on the worker that picks it up.
pub async fn probe_compute() -> Result<(), ComputeError> {
    spawn_compute(|| {}).await
}

async fn spawn_compute<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> Result<R, ComputeError> {
    let (send, recv) = oneshot::channel();
    COMPUTE_POOL.spawn(move || {
        // a panic in a rayon job aborts the process, so it is handed back to the caller instead
        let _ = send.send(panic::catch_unwind(AssertUnwindSafe(closure)));
    });
    recv.await
        .map_err(|_| ComputeError::Dropped)?
        .map_err(|panic| {
            let message = panic_message(panic.as_ref());
            error!(panic = %message, "a compute job panicked");
            ComputeError::Panicked(message)
        })
}

static LOAD_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| {
    let permits = settings()
        .max_concurrent_loads
        .unwrap_or_else(|| (cores() / 4).max(2));
    Semaphore::new(permits.max(1))
});

/// Graph loads: off the query pools, bounded because each in-flight load holds a whole graph.
pub async fn blocking_load<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(closure: F) -> R {
    let _permit = LOAD_PERMITS
        .acquire()
        .await
        .expect("load semaphore is never closed");
    tokio::task::spawn_blocking(closure)
        .await
        .expect("graph load panicked")
}

/// Background graph view caching operations.
static CACHING_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    // using 4 cores was found to be optimal
    let num_threads = settings()
        .caching_threads
        .unwrap_or_else(|| if cores() > 6 { 4 } else { cores() / 2 })
        .max(1);
    ThreadPoolBuilder::new()
        .stack_size(16 * 1024 * 1024)
        .num_threads(num_threads)
        .thread_name(|t| format!("RAP-cache-{t}"))
        .build()
        .unwrap()
});

/// One caching operation at a time, as each already runs in parallel.
static CACHING_PERMITS: Semaphore = Semaphore::const_new(1);

/// Why a caching job gave no result.
#[derive(Debug, thiserror::Error)]
pub enum CachingError {
    #[error("caching panicked: {0}")]
    Panicked(String),
    #[error("the caching job was dropped before it ran")]
    Dropped,
    #[error("the caching queue is closed")]
    Closed,
}

/// Queue `closure` behind any other caching operation and run it on the caching pool.
pub async fn blocking_cache<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> Result<R, CachingError> {
    let permit = CACHING_PERMITS
        .acquire()
        .await
        .map_err(|_| CachingError::Closed)?;
    let (send, recv) = oneshot::channel();
    CACHING_POOL.spawn(move || {
        let _permit = permit;
        // a panic in a rayon job aborts the process, so it is handed back to the caller instead
        let _ = send.send(panic::catch_unwind(AssertUnwindSafe(closure)));
    });
    recv.await
        .map_err(|_| CachingError::Dropped)?
        .map_err(|panic| CachingError::Panicked(panic_message(panic.as_ref())))
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message.to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "a non-string panic payload".to_string()
    }
}

/// Use a separate rayon threadpool to execute write tasks to avoid potential deadlocks
pub async fn blocking_write<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(closure: F) -> R {
    let (send, recv) = oneshot::channel();
    WRITE_POOL.spawn(move || {
        let _ = send.send(closure()); // this only errors if no-one is listening anymore
    });
    recv.await.expect("Function panicked in rayon::spawn")
}

/// The pools are process-global, so tests that jam or saturate them must not overlap.
#[cfg(test)]
static TEST_SERIAL: Mutex<()> = Mutex::new(());

#[cfg(test)]
mod deadlock_tests {
    use crate::{
        rayon::{COMPUTE_POOL, WRITE_POOL},
        routes::Health,
        GraphServer,
    };
    use parking_lot::Mutex;
    use raphtory::db::api::storage::storage::Args;
    use reqwest::{Client, StatusCode};
    use std::{sync::Arc, time::Duration};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_deadlock_in_read_pool() {
        test_pool_lock(|lock| {
            COMPUTE_POOL.spawn_broadcast(move |_| {
                let _guard = lock.lock();
            });
        })
        .await;
    }

    #[tokio::test]
    async fn test_deadlock_in_write_pool() {
        test_pool_lock(|lock| {
            WRITE_POOL.spawn_broadcast(move |_| {
                let _guard = lock.lock();
            });
        })
        .await;
    }

    // Holding TEST_SERIAL (and the pool-jamming lock) across awaits is deliberate: it
    // serialises tests that block the process-global pools.
    #[allow(clippy::await_holding_lock)]
    async fn test_pool_lock(pool_lock: impl FnOnce(Arc<Mutex<()>>)) {
        let _serial = super::TEST_SERIAL.lock();
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let port = running.port();
        tokio::time::sleep(Duration::from_secs(1)).await; // this is to wait for the server to be up
        let lock = Arc::new(Mutex::new(()));
        let _guard = lock.lock();
        let lock_clone = lock.clone();
        pool_lock(lock_clone);
        let client = Client::new();

        let req = client.get(format!("http://localhost:{port}/health"));
        let response = req.timeout(Duration::from_secs(100)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health: Health = response.json().await.unwrap();
        assert!(!health.healthy);

        let req = client.get(format!("http://localhost:{port}/health?timeout=5"));
        let response = req.timeout(Duration::from_secs(100)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health: Health = response.json().await.unwrap();
        assert!(!health.healthy);
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn a_compute_job_that_panics_returns_an_error_and_the_pool_keeps_going() {
        let failed = blocking_compute(|| -> usize { panic!("boom") }).await;
        assert!(matches!(failed, Err(ComputeError::Panicked(message)) if message == "boom"));
        assert_eq!(blocking_compute(|| 1).await.unwrap(), 1);
    }

    #[test]
    fn a_failed_compute_job_reaches_the_client_as_an_internal_error() {
        let error = async_graphql::Error::from(ComputeError::Panicked("secret detail".into()));
        assert_eq!(error.message, "Internal server error");
        let error = async_graphql::Error::from(crate::GQLError::from(ComputeError::Dropped));
        assert_eq!(error.message, "Internal server error");
    }

    /// A worker blocked in `join` runs whatever job is queued, including a whole other request,
    /// on top of its own stack. Unbounded, enough concurrent requests overflow the stack (seen in
    /// the GraphQL bench: 215 `degree_centrality` requests nested on one `RAP-compute` thread).
    #[tokio::test(flavor = "multi_thread")]
    async fn compute_jobs_do_not_nest_beyond_the_cap() {
        use rayon::prelude::*;
        use std::{
            cell::Cell,
            sync::atomic::{AtomicUsize, Ordering},
        };
        let _serial = TEST_SERIAL.lock();
        thread_local!(static DEPTH: Cell<usize> = const { Cell::new(0) });
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        let cap = compute_permits();
        // nesting grows with the backlog, so flood the pool well past the cap
        let jobs: Vec<_> = (0..32 * cap)
            .map(|_| {
                tokio::spawn(blocking_compute(|| {
                    let depth = DEPTH.with(|d| {
                        d.set(d.get() + 1);
                        d.get()
                    });
                    PEAK.fetch_max(depth, Ordering::SeqCst);
                    // blocks this worker in `join`, where it can pick up other queued jobs
                    (0..64)
                        .into_par_iter()
                        .for_each(|_| std::thread::sleep(Duration::from_millis(1)));
                    DEPTH.with(|d| d.set(d.get() - 1));
                }))
            })
            .collect();
        for job in jobs {
            job.await.unwrap().unwrap();
        }
        let peak = PEAK.load(Ordering::SeqCst);
        assert!(
            peak <= cap,
            "{peak} compute jobs nested on one worker, more than the {cap} allowed at once"
        );
    }

    #[tokio::test]
    async fn a_caching_job_that_panics_returns_an_error_and_frees_its_permit() {
        let _serial = TEST_SERIAL.lock();
        let failed = blocking_cache(|| -> usize { panic!("boom") }).await;
        assert!(matches!(failed, Err(CachingError::Panicked(message)) if message == "boom"));
        // there is a single caching permit, so this only runs if the panic released it
        let next = tokio::time::timeout(Duration::from_secs(10), blocking_cache(|| 1));
        assert_eq!(next.await.expect("the permit was not released").unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_loads_are_bounded() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let _serial = TEST_SERIAL.lock();
        static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        let loads: Vec<_> = (0..16)
            .map(|_| {
                tokio::spawn(blocking_load(|| {
                    let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
                    PEAK.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
                }))
            })
            .collect();
        for l in loads {
            let _ = l.await;
        }
        let cap = settings()
            .max_concurrent_loads
            .unwrap_or_else(|| (cores() / 4).max(2));
        assert!(
            PEAK.load(Ordering::SeqCst) <= cap,
            "peak {} loads exceeded the cap {cap}",
            PEAK.load(Ordering::SeqCst)
        );
    }
}
