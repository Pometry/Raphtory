use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::Scope,
    time::{Duration, Instant},
};

use super::{config::Config, owner::OwnerMsg};

pub(super) trait Phase: Copy + 'static {
    const ALL: &'static [Self];
    fn index(self) -> usize;
    fn label(self) -> &'static str;
}

/// Time spent in each phase of a thread's work.
pub(super) struct PhaseTimes<P> {
    totals: Vec<Duration>,
    _phases: PhantomData<P>,
}

#[derive(Clone, Copy)]
pub(super) enum ReaderPhase {
    WaitingForBudget,
    TakingInput,
    Decoding,
    Preparing,
    Resolving,
    Routing,
}

#[derive(Clone, Copy)]
pub(super) enum OwnerPhase {
    Busy,
    Idle,
    Finishing,
}

#[derive(Default)]
pub(super) struct ReaderStats {
    chunks: u64,
    rows: u64,
    time: PhaseTimes<ReaderPhase>,
}

#[derive(Default)]
pub(super) struct OwnerStats {
    outbound_rows: u64,
    inbound_rows: u64,
    time: PhaseTimes<OwnerPhase>,
}

pub(super) struct LoadStats {
    pub readers: Vec<ReaderStats>,
    pub owners: Vec<OwnerStats>,
}

/// While alive, prints the memory budget's estimate next to the process's RSS every second.
pub(super) struct MemorySampler {
    _stop: Sender<()>,
}

impl<P: Phase> Default for PhaseTimes<P> {
    fn default() -> Self {
        Self {
            totals: vec![Duration::ZERO; P::ALL.len()],
            _phases: PhantomData,
        }
    }
}

impl<P: Phase> PhaseTimes<P> {
    pub fn time<T>(&mut self, phase: P, work: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let result = work();
        self.totals[phase.index()] += started.elapsed();
        result
    }

    fn describe(&self) -> String {
        P::ALL
            .iter()
            .map(|&phase| {
                format!(
                    "{} {:.1}s",
                    phase.label(),
                    self.totals[phase.index()].as_secs_f64()
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Phase for ReaderPhase {
    const ALL: &'static [Self] = &[
        Self::WaitingForBudget,
        Self::TakingInput,
        Self::Decoding,
        Self::Preparing,
        Self::Resolving,
        Self::Routing,
    ];

    fn index(self) -> usize {
        self as usize
    }

    fn label(self) -> &'static str {
        match self {
            Self::WaitingForBudget => "budget wait",
            Self::TakingInput => "taking input",
            Self::Decoding => "decode",
            Self::Preparing => "prepare",
            Self::Resolving => "resolve",
            Self::Routing => "route",
        }
    }
}

impl Phase for OwnerPhase {
    const ALL: &'static [Self] = &[Self::Busy, Self::Idle, Self::Finishing];

    fn index(self) -> usize {
        self as usize
    }

    fn label(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Idle => "idle",
            Self::Finishing => "finish",
        }
    }
}

impl ReaderStats {
    pub fn time<T>(&mut self, phase: ReaderPhase, work: impl FnOnce() -> T) -> T {
        self.time.time(phase, work)
    }

    pub fn count_chunk(&mut self, rows: usize) {
        self.chunks += 1;
        self.rows += rows as u64;
    }
}

impl OwnerStats {
    pub fn time<T>(&mut self, phase: OwnerPhase, work: impl FnOnce() -> T) -> T {
        self.time.time(phase, work)
    }

    pub fn count(&mut self, message: &OwnerMsg) {
        match message {
            OwnerMsg::WriteOutbound(_, rows) => self.outbound_rows += rows.len() as u64,
            OwnerMsg::WriteInbound(_, edges) => self.inbound_rows += edges.len() as u64,
            _ => {}
        }
    }
}

impl LoadStats {
    pub fn print(&self, config: &Config, wall: Duration) {
        let rows: u64 = self.readers.iter().map(|reader| reader.rows).sum();
        eprintln!(
            "LOAD2 {rows} rows in {:.1}s ({:.2}M rows/s), {} readers, {} owners, {} chunks in flight",
            wall.as_secs_f64(),
            rows as f64 / wall.as_secs_f64() / 1e6,
            config.readers,
            config.owners,
            config.chunks_in_flight
        );
        for (i, reader) in self.readers.iter().enumerate() {
            eprintln!(
                "LOAD2 reader {i}: {} chunks, {}",
                reader.chunks,
                reader.time.describe()
            );
        }
        for (i, owner) in self.owners.iter().enumerate() {
            eprintln!(
                "LOAD2 owner {i}: {}, {} outbound rows, {} inbound rows",
                owner.time.describe(),
                owner.outbound_rows,
                owner.inbound_rows
            );
        }
    }
}

impl MemorySampler {
    pub fn start<'scope>(
        scope: &'scope Scope<'scope, '_>,
        started: Instant,
        estimate: Arc<AtomicUsize>,
    ) -> Self {
        let (stop, stopped) = bounded(0);
        scope.spawn(move || sample_every_second(started, &estimate, &stopped));
        Self { _stop: stop }
    }
}

fn sample_every_second(started: Instant, estimate: &AtomicUsize, stopped: &Receiver<()>) {
    loop {
        eprintln!(
            "LOAD2 mem {:.0}s estimate {:.1}GB rss {:.1}GB",
            started.elapsed().as_secs_f64(),
            gigabytes(estimate.load(Ordering::Relaxed)),
            gigabytes(resident_bytes())
        );
        if stopped.recv_timeout(Duration::from_secs(1)) != Err(RecvTimeoutError::Timeout) {
            return;
        }
    }
}

/// Linux only, 0 elsewhere: the second field of `/proc/self/statm` is resident pages.
fn resident_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map_or(0, |pages| pages * 4096)
}

fn gigabytes(bytes: usize) -> f64 {
    bytes as f64 / 1e9
}
