pub(super) struct Config {
    pub readers: usize,
    pub owners: usize,
    pub chunks_in_flight: usize,
    pub print_stats: bool,
}

impl Config {
    pub fn from_env() -> Self {
        let cores = std::thread::available_parallelism().map_or(8, |n| n.get());
        let readers = env_usize("RAPHTORY_LOAD2_READERS").unwrap_or(4).max(1);
        let owners = env_usize("RAPHTORY_LOAD2_OWNERS")
            .unwrap_or(cores.saturating_sub(readers))
            .max(1);
        Self {
            readers,
            owners,
            chunks_in_flight: env_usize("RAPHTORY_LOAD2_IN_FLIGHT")
                .unwrap_or(2 * owners)
                .max(1),
            print_stats: std::env::var_os("RAPHTORY_LOAD2_STATS").is_some(),
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}
