/// Per-segment / per-layer summary of which property ids have values in a
/// given (layer, segment) pair.
///
/// `temporal_props[i] == true` means a value has been written for global
/// temporal-prop-id `i` in this segment for this layer. Same for `metadata`
/// with the global metadata-prop-id space.
///
/// Bitsets are merged across segments and layers by `union_with`, giving the
/// per-layer property schema without scanning entities.
///
/// The representation is intentionally simple — a `Vec<bool>` rather than a
/// packed bitset — because the schema is small (number of properties, not
/// number of rows) and needs to round-trip through disk via rkyv. We can swap
/// in a denser representation later if profiling justifies it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LayerPropSchema {
    temporal_props: Vec<bool>,
    metadata: Vec<bool>,
}

impl LayerPropSchema {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a schema directly from two presence vectors. Used when
    /// reconstructing a schema from persisted on-disk state.
    pub fn from_bools(temporal_props: Vec<bool>, metadata: Vec<bool>) -> Self {
        Self {
            temporal_props,
            metadata,
        }
    }

    /// Mark a temporal property id as present.
    #[inline]
    pub fn insert_temporal(&mut self, prop_id: usize) {
        if prop_id >= self.temporal_props.len() {
            self.temporal_props.resize(prop_id + 1, false);
        }
        self.temporal_props[prop_id] = true;
    }

    /// Mark a metadata (const) property id as present.
    #[inline]
    pub fn insert_metadata(&mut self, prop_id: usize) {
        if prop_id >= self.metadata.len() {
            self.metadata.resize(prop_id + 1, false);
        }
        self.metadata[prop_id] = true;
    }

    #[inline]
    pub fn contains_temporal(&self, prop_id: usize) -> bool {
        self.temporal_props.get(prop_id).copied().unwrap_or(false)
    }

    #[inline]
    pub fn contains_metadata(&self, prop_id: usize) -> bool {
        self.metadata.get(prop_id).copied().unwrap_or(false)
    }

    pub fn temporal_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.temporal_props
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| b.then_some(i))
    }

    pub fn metadata_prop_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.metadata
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| b.then_some(i))
    }

    pub fn temporal_bits(&self) -> &[bool] {
        &self.temporal_props
    }

    pub fn metadata_bits(&self) -> &[bool] {
        &self.metadata
    }

    /// Union another schema into this one.
    pub fn union_with(&mut self, other: &LayerPropSchema) {
        union_into(&mut self.temporal_props, &other.temporal_props);
        union_into(&mut self.metadata, &other.metadata);
    }

    pub fn is_empty(&self) -> bool {
        !self.temporal_props.iter().any(|&b| b) && !self.metadata.iter().any(|&b| b)
    }
}

fn union_into(dst: &mut Vec<bool>, src: &[bool]) {
    if src.len() > dst.len() {
        dst.resize(src.len(), false);
    }
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        *d |= *s;
    }
}

impl FromIterator<LayerPropSchema> for LayerPropSchema {
    fn from_iter<I: IntoIterator<Item = LayerPropSchema>>(iter: I) -> Self {
        let mut acc = LayerPropSchema::new();
        for s in iter {
            acc.union_with(&s);
        }
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_query() {
        let mut s = LayerPropSchema::new();
        s.insert_temporal(0);
        s.insert_temporal(5);
        s.insert_metadata(2);

        assert!(s.contains_temporal(0));
        assert!(s.contains_temporal(5));
        assert!(!s.contains_temporal(1));
        assert!(s.contains_metadata(2));
        assert!(!s.contains_metadata(0));

        let t: Vec<usize> = s.temporal_prop_ids().collect();
        assert_eq!(t, vec![0, 5]);
        let m: Vec<usize> = s.metadata_prop_ids().collect();
        assert_eq!(m, vec![2]);
    }

    #[test]
    fn union() {
        let mut a = LayerPropSchema::new();
        a.insert_temporal(1);
        a.insert_metadata(3);

        let mut b = LayerPropSchema::new();
        b.insert_temporal(10);
        b.insert_metadata(3);
        b.insert_metadata(7);

        a.union_with(&b);
        assert_eq!(a.temporal_prop_ids().collect::<Vec<_>>(), vec![1, 10]);
        assert_eq!(a.metadata_prop_ids().collect::<Vec<_>>(), vec![3, 7]);
    }

    #[test]
    fn from_bools_roundtrip() {
        let mut s = LayerPropSchema::new();
        s.insert_temporal(2);
        s.insert_metadata(0);
        let rebuilt =
            LayerPropSchema::from_bools(s.temporal_bits().to_vec(), s.metadata_bits().to_vec());
        assert_eq!(s, rebuilt);
    }
}
