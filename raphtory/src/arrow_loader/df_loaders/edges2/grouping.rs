/// Groups `items` by a key with few distinct values, keeping their order within each group.
pub(super) fn group_by<T>(
    items: impl IntoIterator<Item = T>,
    key: impl Fn(&T) -> usize,
) -> Vec<(usize, Vec<T>)> {
    let mut groups: Vec<(usize, Vec<T>)> = Vec::new();
    for item in items {
        let key = key(&item);
        match groups.iter_mut().find(|(group, _)| *group == key) {
            Some((_, group)) => group.push(item),
            None => groups.push((key, vec![item])),
        }
    }
    groups
}

/// Sorts `items` into `buckets` buckets by index, keeping their order within each bucket.
pub(super) fn bucket_by<T>(
    items: impl IntoIterator<Item = T>,
    buckets: usize,
    bucket: impl Fn(&T) -> usize,
) -> Vec<Vec<T>> {
    let mut sorted: Vec<Vec<T>> = (0..buckets).map(|_| Vec::new()).collect();
    for item in items {
        sorted[bucket(&item)].push(item);
    }
    sorted
}
