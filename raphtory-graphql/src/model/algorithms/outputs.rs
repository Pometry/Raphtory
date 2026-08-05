use dynamic_graphql::SimpleObject;

/// The motif counts for a single delta. Wraps the counts in an object because
/// the schema builder does not support nested lists of scalars.
#[derive(SimpleObject)]
#[graphql(name = "MotifCounts")]
pub(crate) struct GqlMotifCounts {
    /// The delta these counts were computed for.
    delta: i64,
    /// The 40 motif counts, positionally ordered (see the core docs).
    counts: Vec<usize>,
}
