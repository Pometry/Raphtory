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
        self.entry.i() as u64 // GraphQL doesn't have usize, use u64
    }

    /// ISO 8601 datetime string representation if timestamp is valid, or else returns an error message
    async fn datetime(&self) -> String {
        match self.entry.dt() {
            Ok(dt) => dt.to_rfc3339(),
            Err(e) => e.to_string(),
        }
    }

    /// String representation showing both timestamp and secondary index
    async fn display(&self) -> String {
        format!("TimeIndexEntry[{}, {}]", self.entry.0, self.entry.1)
    }
}
