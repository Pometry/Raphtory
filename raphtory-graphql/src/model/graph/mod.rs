use dynamic_graphql::{Enum, OneOfInput};
use raphtory::core::utils::time::{AlignmentUnit, Interval, TryIntoInterval};
use raphtory_api::core::utils::time::ParseTimeError;

pub(crate) mod collection;
mod document;
pub(crate) mod edge;
mod edges;
pub(crate) mod filtering;
pub(crate) mod graph;
pub(crate) mod history;
pub(crate) mod index;
pub(crate) mod meta_graph;
pub(crate) mod mutable_graph;
pub(crate) mod namespace;
pub(crate) mod namespaced_item;
pub(crate) mod node;
mod nodes;
mod path_from_node;
pub(crate) mod property;
pub(crate) mod timeindex;
pub(crate) mod vector_selection;
pub(crate) mod vectorised_graph;
mod windowset;

#[derive(OneOfInput, Clone)]
pub(crate) enum WindowDuration {
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
pub(crate) enum GqlAlignmentUnit {
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
