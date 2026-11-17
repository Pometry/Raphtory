use crate::config::concurrency_config::ConcurrencyConfig;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The job slots of SPARQL queries and SHACL validations (see `heavy_query_limit`).
#[derive(Clone)]
pub(crate) struct RdfJobs(Option<Arc<Semaphore>>);

impl RdfJobs {
    pub(crate) fn new(config: &ConcurrencyConfig) -> Self {
        let limit = config.heavy_query_limit;
        Self(limit.map(|limit| Arc::new(Semaphore::new(limit))))
    }

    /// Waits for a free slot; `None` when nothing limits the jobs.
    pub(crate) async fn permit(jobs: Option<&Self>) -> Option<OwnedSemaphorePermit> {
        jobs?.0.clone()?.acquire_owned().await.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::RdfJobs;
    use crate::{config::concurrency_config::ConcurrencyConfig, sparql::run_limited};
    use futures_util::future::join_all;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread::sleep,
        time::Duration,
    };
    use tokio::time::timeout;

    #[tokio::test]
    async fn jobs_beyond_the_limit_wait() {
        static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        let config = ConcurrencyConfig {
            heavy_query_limit: Some(1),
            ..Default::default()
        };
        let jobs = RdfJobs::new(&config);
        let job = || {
            run_limited(Some(&config), Some(&jobs), |_| {
                let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
                PEAK.fetch_max(now, Ordering::SeqCst);
                sleep(Duration::from_millis(50));
                IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        };
        let results = join_all([job(), job(), job()]).await;
        assert!(results.iter().all(|result| matches!(result, Ok(Some(())))));
        assert_eq!(PEAK.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_dropped_call_keeps_its_slot_until_its_job_ends() {
        static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
        static PEAK: AtomicUsize = AtomicUsize::new(0);
        let config = ConcurrencyConfig {
            heavy_query_limit: Some(1),
            ..Default::default()
        };
        let jobs = RdfJobs::new(&config);
        let job = |millis| {
            run_limited(Some(&config), Some(&jobs), move |_| {
                let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
                PEAK.fetch_max(now, Ordering::SeqCst);
                sleep(Duration::from_millis(millis));
                IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        };
        assert!(timeout(Duration::from_millis(10), job(200)).await.is_err());
        assert!(matches!(job(0).await, Ok(Some(()))));
        assert_eq!(PEAK.load(Ordering::SeqCst), 1);
    }
}
