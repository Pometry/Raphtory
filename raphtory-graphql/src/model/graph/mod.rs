use dynamic_graphql::{Enum, OneOfInput};
use raphtory::core::utils::time::{AlignmentUnit, Interval, TryIntoInterval};
use raphtory_api::core::utils::time::ParseTimeError;

pub mod collection;
pub mod edge;
pub mod edges;
pub mod filtering;
pub mod graph;
pub mod history;
pub mod meta_graph;
pub mod mutable_graph;
pub mod namespace;
pub mod namespaced_item;
pub mod nested_edges;
pub mod node;
pub mod node_id;
pub mod node_state;
pub mod nodes;
pub mod path_from_graph;
pub mod path_from_node;
pub mod property;
pub mod timeindex;
pub mod windowset;

#[cfg(feature = "vectors")]
pub mod vector_selection;

#[cfg(feature = "vectors")]
pub mod vectorised_graph;

#[cfg(feature = "vectors")]
pub mod document;

#[derive(OneOfInput, Clone)]
pub enum WindowDuration {
    /// Duration of window period.
    ///
    /// Choose from:
    Duration(String),
    /// Time.
    Epoch(u64),
}

impl TryFrom<WindowDuration> for Interval {
    type Error = ParseTimeError;

    fn try_from(value: WindowDuration) -> Result<Self, Self::Error> {
        match value {
            WindowDuration::Duration(temporal) => temporal.try_into_interval(),
            WindowDuration::Epoch(discrete) => discrete.try_into_interval(),
        }
    }
}

/// Alignment unit used to align window boundaries.
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "AlignmentUnit")]
pub enum GqlAlignmentUnit {
    Unaligned, // note that there is no functional difference between millisecond and unaligned for the time being
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl From<GqlAlignmentUnit> for AlignmentUnit {
    fn from(unit: GqlAlignmentUnit) -> Self {
        match unit {
            GqlAlignmentUnit::Unaligned => AlignmentUnit::Unaligned,
            GqlAlignmentUnit::Millisecond => AlignmentUnit::Millisecond,
            GqlAlignmentUnit::Second => AlignmentUnit::Second,
            GqlAlignmentUnit::Minute => AlignmentUnit::Minute,
            GqlAlignmentUnit::Hour => AlignmentUnit::Hour,
            GqlAlignmentUnit::Day => AlignmentUnit::Day,
            GqlAlignmentUnit::Week => AlignmentUnit::Week,
            GqlAlignmentUnit::Month => AlignmentUnit::Month,
            GqlAlignmentUnit::Year => AlignmentUnit::Year,
        }
    }
}
