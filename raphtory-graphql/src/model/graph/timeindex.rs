use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory_api::core::storage::timeindex::{AsTime, TimeIndexEntry};

/// Represents a time index entry with timestamp and secondary index for ordering
#[derive(ResolvedObject, Clone)]
#[graphql(name = "TimeIndexEntry")]
pub struct GqlTimeIndexEntry {
    pub(crate) entry: TimeIndexEntry,
}

impl From<TimeIndexEntry> for GqlTimeIndexEntry {
    fn from(entry: TimeIndexEntry) -> Self {
        Self { entry }
    }
}

#[ResolvedObjectFields]
impl GqlTimeIndexEntry {
    /// The timestamp as milliseconds since Unix epoch
    async fn timestamp(&self) -> i64 {
        self.entry.t()
    }

    /// The secondary index of the TimeIndexEntry
    async fn secondary_index(&self) -> u64 {
        self.entry.i() as u64
    }

    /// Returns a datetime string representation if timestamp is valid, or else returns an error message string.
    /// Uses RFC 3339 format by default (e.g., "2023-12-25T10:30:45.123Z").
    /// Optionally takes a format string as argument to format the output of datetimes.
    /// Refer to chrono::format::strftime for formatting specifiers and escape sequences.
    async fn datetime(&self, format_string: Option<String>) -> String {
        let fmt_string = format_string.as_deref().unwrap_or("%+"); // %+ is RFC 3339
        match self.entry.dt() {
            Ok(dt) => dt.format(fmt_string).to_string(),
            Err(e) => format!("Time conversion error: {}", e.to_string()),
        }
    }
}
