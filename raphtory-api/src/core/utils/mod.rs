pub mod hashing;
pub mod logging;
pub mod time;

pub fn generalised_reduce<V>(
    data: impl IntoIterator<Item = V>,
    op: impl Fn(V, V) -> Option<V>,
    check: impl Fn(&V) -> bool,
) -> Option<V> {
    let mut iter = data.into_iter();
    let first = iter.next()?;
    if !check(&first) {
        return None;
    }
    iter.try_fold(first, op)
}
