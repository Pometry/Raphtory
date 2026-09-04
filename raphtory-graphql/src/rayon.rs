use rayon::{ThreadPool, ThreadPoolBuilder};
use std::{
    collections::VecDeque,
    sync::{LazyLock, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, Semaphore};
use tracing::warn;

/// How the compute pool schedules query work. The pools are process-global statics, so this is
/// set once from the first server's `ConcurrencyConfig`; every later server in the process shares it.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolSettings {
    /// Threads taken out of the compute pool and reserved for [`EXPRESS_POOL`].
    pub express_threads: usize,
    /// Maximum number of [`blocking_compute`] closures executing at once; further submissions
    /// queue in the [`SCHEDULER`]. `None` = half the compute pool's threads.
    pub max_concurrent_queries: Option<usize>,
    /// When true, queued submissions are dispatched newest-first (with rationed promotion of old
    /// waiters — see [`pump`]); when false, strictly first-in first-out.
    pub newest_first: bool,
    /// Maximum graph loads decoding at once via [`blocking_load`]. Each in-flight load holds a
    /// whole graph in memory, so this bounds peak memory during a load stampede.
    /// `None` = cores / 4, at least 2.
    pub max_concurrent_loads: Option<usize>,
}

impl Default for PoolSettings {
    fn default() -> Self {
        PoolSettings {
            express_threads: default_express_threads(),
            max_concurrent_queries: None,
            newest_first: true,
            max_concurrent_loads: None,
        }
    }
}

/// Two reserved threads on an 8+-core machine; one below that, so a small machine does not give
/// up a large share of its compute pool for the reservation.
pub fn default_express_threads() -> usize {
    if cores() >= 8 {
        2
    } else {
        1
    }
}
const PROMOTE_AFTER: Duration = Duration::from_secs(1);

static SETTINGS: OnceLock<PoolSettings> = OnceLock::new();

/// First caller wins (the pools are process-global); a later, different configuration is ignored
/// with a warning.
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

pub static COMPUTE_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .stack_size(16 * 1024 * 1024)
        .num_threads(cores().saturating_sub(settings().express_threads).max(2))
        .thread_name(|t| format!("RAP-compute-{t}"))
        .build()
        .unwrap()
});

/// Threads reserved for work that must stay responsive while the compute pool is saturated:
/// the `/health` round-trip and cheap store/metadata resolvers. Nothing submitted here may do
/// graph work — one scan on this pool removes the reservation for everything else.
pub static EXPRESS_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .num_threads(settings().express_threads.max(1))
        .thread_name(|t| format!("RAP-express-{t}"))
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

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Scheduler {
    queue: VecDeque<(Instant, Job)>,
    running: usize,
    dispatched: u64,
}

static SCHEDULER: LazyLock<Mutex<Scheduler>> = LazyLock::new(|| {
    Mutex::new(Scheduler {
        queue: VecDeque::new(),
        running: 0,
        dispatched: 0,
    })
});

/// One dispatch in this many takes the oldest waiter (if older than [`PROMOTE_AFTER`]) instead of
/// the newest. Promotion must be rationed: under a sustained backlog the front of the queue is
/// always old, so promoting it on every dispatch is exactly first-in first-out and newest-first
/// never happens.
const PROMOTE_EVERY: u64 = 4;

fn max_concurrent() -> usize {
    settings()
        .max_concurrent_queries
        .unwrap_or_else(|| (COMPUTE_POOL.current_num_threads() / 2).max(1))
}

/// Dispatch queued jobs until every admission slot is in use, then return; runs again after each
/// job completes and on every submission. Admission (at most [`max_concurrent`] jobs running)
/// stops queries time-sharing the whole pool, so a slot frees as soon as one query finishes;
/// newest-first dispatch then hands that slot to the most recently submitted query, so a short
/// query arriving into a backlog waits for one slot, not for the whole backlog to drain.
fn pump() {
    loop {
        let job = {
            let mut s = SCHEDULER.lock().expect("scheduler lock");
            if s.running >= max_concurrent() {
                return;
            }
            s.dispatched += 1;
            let promote_old = s.dispatched % PROMOTE_EVERY == 0;
            let job = if settings().newest_first {
                match s.queue.front() {
                    Some((queued, _)) if promote_old && queued.elapsed() > PROMOTE_AFTER => {
                        s.queue.pop_front()
                    }
                    _ => s.queue.pop_back(),
                }
            } else {
                s.queue.pop_front()
            };
            let Some((_, job)) = job else { return };
            s.running += 1;
            job
        };
        COMPUTE_POOL.spawn(move || {
            job();
            SCHEDULER.lock().expect("scheduler lock").running -= 1;
            pump();
        });
    }
}

/// Run `closure` on the compute pool, scheduled: the job queues in the [`SCHEDULER`] and starts
/// when an admission slot is free, dispatched newest-first (see [`pump`]). Use for query work,
/// which may be arbitrarily heavy.
pub async fn blocking_compute<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> R {
    let (send, recv) = oneshot::channel();
    {
        let mut s = SCHEDULER.lock().expect("scheduler lock");
        s.queue.push_back((
            Instant::now(),
            Box::new(move || {
                let _ = send.send(closure()); // this only errors if no-one is listening anymore
            }),
        ));
    }
    pump();
    recv.await.expect("Function panicked in rayon::spawn")
}

static LOAD_PERMITS: LazyLock<Semaphore> = LazyLock::new(|| {
    let permits = settings()
        .max_concurrent_loads
        .unwrap_or_else(|| (cores() / 4).max(2));
    Semaphore::new(permits.max(1))
});

/// Run a graph load: on tokio's blocking pool (so it never holds a query admission slot), bounded
/// by [`LOAD_PERMITS`] (each in-flight load holds a decoded graph in memory, so unbounded
/// concurrency is a memory blow-up under a load stampede). Internal parallelism uses rayon's
/// global pool, which the query path never touches.
pub async fn blocking_load<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(closure: F) -> R {
    let _permit = LOAD_PERMITS
        .acquire()
        .await
        .expect("load semaphore is never closed");
    tokio::task::spawn_blocking(closure)
        .await
        .expect("graph load panicked")
}

/// Run `closure` immediately on the reserved [`EXPRESS_POOL`], bypassing the scheduler. Only for
/// work that is always cheap (health checks, store/metadata reads) — never graph work.
pub async fn blocking_express<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(
    closure: F,
) -> R {
    let (send, recv) = oneshot::channel();
    EXPRESS_POOL.spawn(move || {
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
    use raphtory::db::api::storage::storage::Config;
    use reqwest::{Client, StatusCode};
    use std::{sync::Arc, time::Duration};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_deadlock_in_read_pool() {
        test_pool_lock(43871, |lock| {
            COMPUTE_POOL.spawn_broadcast(move |_| {
                let _guard = lock.lock();
            });
        })
        .await;
    }

    #[tokio::test]
    async fn test_deadlock_in_write_pool() {
        test_pool_lock(43872, |lock| {
            WRITE_POOL.spawn_broadcast(move |_| {
                let _guard = lock.lock();
            });
        })
        .await;
    }

    async fn test_pool_lock(port: u16, pool_lock: impl FnOnce(Arc<Mutex<()>>)) {
        let _serial = super::TEST_SERIAL.lock();
        let tempdir = TempDir::new().unwrap();
        let server = GraphServer::new(tempdir.path().to_path_buf(), None, Config::default())
            .await
            .unwrap();
        let _running = server.start_with_port(port).await.unwrap();
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
        assert_eq!(health.healthy, false);

        let req = client.get(format!("http://localhost:{port}/health?timeout=5"));
        let response = req.timeout(Duration::from_secs(100)).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health: Health = response.json().await.unwrap();
        assert_eq!(health.healthy, false);
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;

    /// A short task submitted behind a backlog of long ones runs long before the backlog drains.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_new_short_task_jumps_a_heavy_backlog() {
        let _serial = TEST_SERIAL.lock();
        let heavies: Vec<_> = (0..COMPUTE_POOL.current_num_threads() * 4)
            .map(|_| {
                tokio::spawn(blocking_compute(|| {
                    std::thread::sleep(Duration::from_millis(200))
                }))
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let t0 = Instant::now();
        blocking_compute(|| {}).await;
        let waited = t0.elapsed();
        // Full FIFO drain would be seconds; one admission slot freeing is a few hundred ms.
        assert!(
            waited < Duration::from_millis(1500),
            "short task waited {waited:?} behind the heavy backlog"
        );
        for h in heavies {
            let _ = h.await;
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn express_is_immediate_while_compute_is_saturated() {
        let _serial = TEST_SERIAL.lock();
        let heavies: Vec<_> = (0..COMPUTE_POOL.current_num_threads() * 4)
            .map(|_| {
                tokio::spawn(blocking_compute(|| {
                    std::thread::sleep(Duration::from_millis(200))
                }))
            })
            .collect();
        let t0 = Instant::now();
        blocking_express(|| {}).await;
        let waited = t0.elapsed();
        // Drain the backlog before releasing the serial guard, so no jobs leak into other tests.
        for h in heavies {
            let _ = h.await;
        }
        assert!(
            waited < Duration::from_millis(100),
            "express waited {waited:?}"
        );
    }
}
