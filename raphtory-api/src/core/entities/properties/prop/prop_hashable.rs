use crate::core::entities::properties::prop::Prop;
use bigdecimal::BigDecimal;
use derive_more::From;
use num_bigint::BigInt;
use num_rational::Ratio;
use num_traits::{float::FloatCore, Float, ToPrimitive};
use ordered_float::OrderedFloat;
use std::{
    fmt::{Display, Formatter},
    hash::{DefaultHasher, Hash, Hasher},
    num::Wrapping,
    ops::Deref,
};

/// A wrapper around `Prop` that implements relaxed PartialEq and Eq that is consistent
/// with hashing, used for defining `HashSet`s of `Prop` values.
///
/// `F32` and `F64` are equivalent as are all integer variants but floats and integers are always
/// considered distinct, even if they represent the same numeric value.
#[derive(Debug, Clone, From)]
#[repr(transparent)]
pub struct HashableProp(pub Prop);

impl Deref for HashableProp {
    type Target = Prop;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for HashableProp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<HashableProp> for Prop {
    fn from(value: HashableProp) -> Self {
        value.0
    }
}

impl PartialEq for HashableProp {
    fn eq(&self, other: &Self) -> bool {
        self.0.equals(&other.0)
    }
}

impl Eq for HashableProp {}

fn hash_integer<T: Into<BigInt>, H: Hasher>(v: T, state: &mut H) {
    let ratio = Ratio::from_integer(v.into());
    ratio.hash(state)
}

fn hash_float<T: Into<f64>, H: Hasher>(v: T, state: &mut H) {
    let v_f64 = v.into();
    match Ratio::from_float(v_f64) {
        None => OrderedFloat(v_f64).hash(state), // handle NaN and infinity
        Some(ratio) => ratio.hash(state),
    }
}

fn hash_decimal<H: Hasher>(v: &BigDecimal, state: &mut H) {
    // probably not the best hash but it is consistent
    match v.to_f64() {
        None => v.hash(state),
        Some(f) => hash_float(f, state),
    }
}

impl Hash for HashableProp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Prop::Str(s) => s.hash(state),
            Prop::U8(u) => hash_integer(*u, state),
            Prop::U16(u) => hash_integer(*u, state),
            Prop::I32(i) => hash_integer(*i, state),
            Prop::I64(i) => hash_integer(*i, state),
            Prop::U32(u) => hash_integer(*u, state),
            Prop::U64(u) => hash_integer(*u, state),
            Prop::F32(f) => hash_float(*f, state),
            Prop::F64(f) => hash_float(*f, state),
            Prop::Bool(b) => b.hash(state),
            Prop::NDTime(dt) => dt.hash(state),
            Prop::DTime(dt) => dt.hash(state),
            Prop::List(v) => {
                for prop in v.iter() {
                    prop.hash(state);
                }
            }
            Prop::Map(m) => {
                // Based on python set hash
                let mut hash = Wrapping(1927868237u64);
                hash *= (m.len() as u64).wrapping_add(1);
                for v in m.iter() {
                    let mut inner_hasher = DefaultHasher::new();
                    v.hash(&mut inner_hasher);
                    let inner_hash = Wrapping(inner_hasher.finish());
                    hash ^= (inner_hash ^ (inner_hash << 16) ^ Wrapping(89869747u64))
                        * Wrapping(3644798167u64);
                }
                hash ^= (hash >> 11) ^ (hash >> 25);
                hash *= 69069;
                hash += 907133923;
                state.write_u64(hash.0);
            }
            Prop::Decimal(d) => hash_decimal(d, state),
        }
    }
}

impl<'a> From<&'a Prop> for &'a HashableProp {
    fn from(value: &'a Prop) -> Self {
        // Safety: HashableProp is #[repr(transparent)] and has no invalid values, there is no physical
        // difference between HashableProp and Prop
        unsafe { &*(value as *const Prop as *const HashableProp) }
    }
}

impl<'a> From<&'a HashableProp> for &'a Prop {
    fn from(value: &'a HashableProp) -> Self {
        // Safety: HashableProp is #[repr(transparent)] and has no invalid values, there is no physical
        // difference between HashableProp and Prop
        unsafe { &*(value as *const HashableProp as *const Prop) }
    }
}

impl AsRef<HashableProp> for Prop {
    fn as_ref(&self) -> &HashableProp {
        self.into()
    }
}

#[cfg(test)]
mod tests {
    use crate::core::entities::properties::prop::{prop_hashable::HashableProp, Prop};
    use proptest::{arbitrary::any, proptest};
    use std::collections::HashSet;

    #[test]
    fn test_prop_float_hashing() {
        let members = [
            Prop::F64(0.5),
            Prop::F32(0.5),
            Prop::F64(1.0),
            Prop::F32(1.0),
        ];

        let set: HashSet<HashableProp> = members.iter().cloned().map(HashableProp::from).collect();

        // deduplicated same values for different float types
        assert_eq!(set.len(), 2);

        // membership check still passes for all input values
        for member in &members {
            assert!(set.contains(member.into()))
        }

        // random other float is not in the set
        assert!(!set.contains(Prop::F64(0.4).as_ref()));
        assert!(!set.contains(Prop::F32(0.4).as_ref()));

        // integers are equivalent to floats
        assert!(set.contains(Prop::U8(1).as_ref()));
        assert!(set.contains(Prop::U16(1).as_ref()));
        assert!(set.contains(Prop::U32(1).as_ref()));
        assert!(set.contains(Prop::U64(1).as_ref()));
        assert!(set.contains(Prop::I32(1).as_ref()));
        assert!(set.contains(Prop::I64(1).as_ref()));

        // decimal is equivalent to floats
        assert!(set.contains(Prop::Decimal(1.into()).as_ref()));
    }

    #[test]
    fn test_prop_int_hashing() {
        let members = [
            Prop::U8(1),
            Prop::U16(1),
            Prop::U32(1),
            Prop::U64(1),
            Prop::I32(1),
            Prop::I64(1),
        ];

        let set: HashSet<_> = members.iter().cloned().map(HashableProp::from).collect();

        // deduplicated same values for different int types
        assert_eq!(set.len(), 1);

        // membership check still passes for all input values
        for member in &members {
            assert!(set.contains(member.into()))
        }

        // random other int is not in the set
        assert!(!set.contains(Prop::I32(2).as_ref()));
        assert!(!set.contains(Prop::U32(3).as_ref()));

        // integers are equivalent to floats
        assert!(set.contains(Prop::F32(1.0).as_ref()));
        assert!(set.contains(Prop::F64(1.0).as_ref()));

        // decimal is equivalent to int
        assert!(set.contains(Prop::Decimal(1.into()).as_ref()));
    }
}
