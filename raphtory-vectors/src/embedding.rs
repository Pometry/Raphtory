//! The [`Embedding`] type is the boundary between the vector DB (lancedb, which brings its own
//! arrow and datafusion versions) and the rest of raphtory. It only depends on [`bytes`], so
//! either side can convert to and from its own arrow buffers without copying and without the
//! two arrow versions having to match.

use bytes::Bytes;
use serde::{ser::SerializeSeq, Deserialize, Serialize, Serializer};
use std::{fmt, ops::Deref};

#[cfg(feature = "python")]
use raphtory_api::python::repr::Repr;

/// A dense `f32` vector.
///
/// Invariant: the bytes are a native-endian `[f32]`, so the pointer is aligned to
/// `align_of::<f32>()` and the length is a multiple of `size_of::<f32>()`. Arrow asserts the
/// alignment when the bytes are turned back into a `ScalarBuffer<f32>`, so every constructor
/// has to uphold it.
#[derive(Clone)]
pub struct Embedding(Bytes);

/// Owns a `Vec<f32>` for [`Bytes::from_owner`]; going through `Vec<u8>` would lose the alignment.
struct F32Owner(Vec<f32>);

impl AsRef<[u8]> for F32Owner {
    fn as_ref(&self) -> &[u8] {
        bytemuck::cast_slice(&self.0)
    }
}

impl Embedding {
    /// Wraps native-endian `f32` bytes without copying them, or copies them if they are not
    /// aligned for `f32`. Returns `None` if the length is not a multiple of 4.
    pub fn from_bytes(bytes: Bytes) -> Option<Self> {
        if bytes.len() % size_of::<f32>() != 0 {
            return None;
        }
        let aligned = bytes.as_ptr().align_offset(align_of::<f32>()) == 0;
        if aligned && !bytes.is_empty() {
            Some(Self(bytes))
        } else {
            // `Bytes::new()` and empty slices may point anywhere, so empty embeddings are
            // rebuilt as well to keep the pointer aligned
            Some(
                bytes
                    .chunks_exact(size_of::<f32>())
                    .map(|chunk| f32::from_ne_bytes(chunk.try_into().unwrap()))
                    .collect(),
            )
        }
    }

    /// The native-endian `f32` bytes, aligned for `f32`.
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    /// The native-endian `f32` bytes, aligned for `f32`.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl Deref for Embedding {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        bytemuck::cast_slice(&self.0)
    }
}

impl AsRef<[f32]> for Embedding {
    fn as_ref(&self) -> &[f32] {
        self
    }
}

impl fmt::Debug for Embedding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Embedding").field(&self.deref()).finish()
    }
}

impl PartialEq for Embedding {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Serialize for Embedding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for value in self.iter() {
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

impl<'a> Deserialize<'a> for Embedding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let vec: Vec<f32> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into())
    }
}

impl From<Vec<f32>> for Embedding {
    fn from(vec: Vec<f32>) -> Self {
        Embedding(Bytes::from_owner(F32Owner(vec)))
    }
}

impl From<&[f32]> for Embedding {
    fn from(slice: &[f32]) -> Self {
        slice.to_vec().into()
    }
}

impl<const N: usize> From<[f32; N]> for Embedding {
    fn from(array: [f32; N]) -> Self {
        array.to_vec().into()
    }
}

impl FromIterator<f32> for Embedding {
    fn from_iter<T: IntoIterator<Item = f32>>(iter: T) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

#[cfg(feature = "python")]
impl Repr for Embedding {
    fn repr(&self) -> String {
        format!("{:?}", self.deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_bytes() {
        let embedding = Embedding::from(vec![1.0, -2.5, 3.25]);
        let bytes = embedding.clone().into_bytes();
        assert_eq!(bytes.as_ptr().align_offset(align_of::<f32>()), 0);
        assert_eq!(Embedding::from_bytes(bytes).unwrap(), embedding);
    }

    #[test]
    fn copies_misaligned_bytes() {
        let raw: Vec<u8> = [0u8]
            .into_iter()
            .chain([1.5f32, 2.5].iter().flat_map(|v| v.to_ne_bytes()))
            .collect();
        let bytes = Bytes::from(raw);
        let misaligned = bytes.slice(1..);
        let embedding = Embedding::from_bytes(misaligned).unwrap();
        assert_eq!(&*embedding, &[1.5, 2.5]);
        assert_eq!(
            embedding.as_bytes().as_ptr().align_offset(align_of::<f32>()),
            0
        );
    }

    #[test]
    fn rejects_partial_floats() {
        assert!(Embedding::from_bytes(Bytes::from_static(&[0, 0, 0])).is_none());
    }

    #[test]
    fn empty_is_aligned() {
        for embedding in [
            Embedding::from(Vec::new()),
            Embedding::from_bytes(Bytes::new()).unwrap(),
        ] {
            assert!(embedding.is_empty());
            assert_eq!(
                embedding.as_bytes().as_ptr().align_offset(align_of::<f32>()),
                0
            );
        }
    }
}
