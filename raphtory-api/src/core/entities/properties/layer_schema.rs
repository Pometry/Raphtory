/// Summary of which property ids have values across one or more layers.
///
/// `temporal_props[i] == true` means a value has been written for global
/// temporal-prop-id `i`. Same for `metadata` with the global metadata-prop-id
/// space. Built and consumed by the per-layer property presence bitset on
/// `PropMapper` (see `raphtory-api::core::entities::properties::meta`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LayerPropSchema {
    temporal_props: Vec<bool>,
    metadata: Vec<bool>,
}

impl LayerPropSchema {
    pub fn new() -> Self {
        Self::default()
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

    /// Union temporal property presence bits into this schema (position-wise
    /// OR). Bits beyond the current length grow the backing vec.
    pub fn union_temporal_with(&mut self, bits: &[bool]) {
        union_into(&mut self.temporal_props, bits);
    }

    /// Union metadata presence bits into this schema (position-wise
    /// OR). Bits beyond the current length grow the backing vec.
    pub fn union_metadata_with(&mut self, bits: &[bool]) {
        union_into(&mut self.metadata, bits);
    }

    /// Position-wise AND of the temporal-prop bits with the supplied visibility mask.
    /// Careful: Bits at positions beyond `mask.len()` are treated as not visible.
    pub fn intersect_temporal_with(&mut self, mask: &[bool]) {
        intersect_into(&mut self.temporal_props, mask);
    }

    /// Position-wise AND of the metadata bits with the supplied visibility mask.
    /// Careful: Bits at positions beyond `mask.len()` are treated as not visible.
    pub fn intersect_metadata_with(&mut self, mask: &[bool]) {
        intersect_into(&mut self.metadata, mask);
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

fn intersect_into(dst: &mut Vec<bool>, mask: &[bool]) {
    for (i, d) in dst.iter_mut().enumerate() {
        *d &= mask.get(i).copied().unwrap_or(false);
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
    fn union_bits() {
        let mut s = LayerPropSchema::new();
        s.insert_temporal(1);
        s.insert_metadata(3);

        s.union_temporal_with(&[
            false, false, false, false, false, false, false, false, false, false, true,
        ]);
        s.union_metadata_with(&[false, false, false, false, false, false, false, true]);

        assert_eq!(s.temporal_prop_ids().collect::<Vec<_>>(), vec![1, 10]);
        assert_eq!(s.metadata_prop_ids().collect::<Vec<_>>(), vec![3, 7]);
    }

    #[test]
    fn intersect() {
        let mut s = LayerPropSchema::new();
        s.insert_temporal(0);
        s.insert_temporal(2);
        s.insert_temporal(5);
        s.insert_metadata(1);
        s.insert_metadata(3);

        // Mask shorter than schema — trailing bits clear.
        s.intersect_temporal_with(&[true, false, true]);
        assert_eq!(s.temporal_prop_ids().collect::<Vec<_>>(), vec![0, 2]);

        // Mask covers schema — keep only marked bits.
        s.intersect_metadata_with(&[false, false, false, true, true]);
        assert_eq!(s.metadata_prop_ids().collect::<Vec<_>>(), vec![3]);
    }
}
