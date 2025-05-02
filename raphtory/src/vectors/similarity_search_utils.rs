use crate::db::{api::view::StaticGraphViewOps, graph::views::window_graph::WindowedGraph};
use itertools::Itertools;

/// Returns the top k docs in descending order
pub(crate) fn find_top_k<'a, I, T>(elements: I, k: usize) -> impl Iterator<Item = (T, f32)> + 'a
where
    I: Iterator<Item = (T, f32)> + 'a,
    T: 'static,
{
    elements.sorted_by_key(|(_, score)| -score).take(k)
}

pub(super) fn apply_window<G: StaticGraphViewOps>(
    graph: &G,
    window: Option<(i64, i64)>,
) -> Option<WindowedGraph<G>> {
    window.map(|(start, end)| graph.window(start, end))
}
