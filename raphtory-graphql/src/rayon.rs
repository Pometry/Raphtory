use tokio::sync::oneshot;

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
