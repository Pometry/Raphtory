use crate::core::storage::arc_str::ArcStr;
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelExtend, ParallelIterator,
};
use rayon::prelude::*;
use std::{iter::Copied, sync::Arc};

#[derive(Debug, Clone)]
pub enum Layer {
    All,
    None,
    Default,
    One(ArcStr),
    Multiple(Arc<[ArcStr]>),
}

impl Layer {
    pub fn contains(&self, name: &str) -> bool {
        match self {
            Layer::All => true,
            Layer::None => false,
            Layer::Default => name == "_default",
            Layer::One(layer) => layer == name,
            Layer::Multiple(layers) => layers.iter().any(|l| l == name),
        }
    }
}

pub trait SingleLayer {
    fn name(self) -> ArcStr;
}

impl<T: SingleLayer> From<T> for Layer {
    fn from(value: T) -> Self {
        Layer::One(value.name())
    }
}

impl SingleLayer for ArcStr {
    fn name(self) -> ArcStr {
        self
    }
}

impl SingleLayer for String {
    fn name(self) -> ArcStr {
        self.into()
    }
}
impl SingleLayer for &str {
    fn name(self) -> ArcStr {
        self.into()
    }
}

impl SingleLayer for &String {
    fn name(self) -> ArcStr {
        self.as_str().into()
    }
}

impl SingleLayer for &ArcStr {
    fn name(self) -> ArcStr {
        self.clone()
    }
}

impl<T: SingleLayer> SingleLayer for Option<T> {
    fn name(self) -> ArcStr {
        match self {
            None => ArcStr::from("_default"),
            Some(s) => s.name(),
        }
    }
}

impl<T: SingleLayer> From<Vec<T>> for Layer {
    fn from(names: Vec<T>) -> Self {
        match names.len() {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

impl<T: SingleLayer, const N: usize> From<[T; N]> for Layer {
    fn from(names: [T; N]) -> Self {
        match N {
            0 => Layer::None,
            1 => Layer::One(names.into_iter().next().unwrap().name()),
            _ => Layer::Multiple(
                names
                    .into_iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .into(),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LayerIds {
    None,
    All,
    One(usize),
    Multiple(Multiple),
}

#[derive(
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    FusedIterator,
    ParallelIterator,
    ParallelExtend,
    IndexedParallelIterator,
)]
pub enum LayerVariants<None, All, One, Multiple> {
    None(None),
    All(All),
    One(One),
    Multiple(Multiple),
}

#[derive(Clone, Debug, Default)]
pub struct Multiple(pub Arc<[usize]>);

impl<'a> IntoIterator for &'a Multiple {
    type Item = usize;
    type IntoIter = Copied<std::slice::Iter<'a, usize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().copied()
    }
}

impl Multiple {
    #[inline]
    pub fn binary_search(&self, pos: &usize) -> Option<usize> {
        self.0.binary_search(pos).ok()
    }

    #[inline]
    pub fn into_iter(&self) -> impl Iterator<Item = usize> {
        let ids = self.0.clone();
        (0..ids.len()).map(move |i| ids[i])
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.0.iter().copied()
    }

    #[inline]
    pub fn find(&self, id: usize) -> Option<usize> {
        self.0.get(id).copied()
    }

    #[inline]
    pub fn par_iter(&self) -> impl rayon::iter::ParallelIterator<Item = usize> {
        let bit_vec = self.0.clone();
        (0..bit_vec.len()).into_par_iter().map(move |i| bit_vec[i])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<usize> for Multiple {
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        Multiple(iter.into_iter().collect())
    }
}

impl From<Vec<usize>> for Multiple {
    fn from(v: Vec<usize>) -> Self {
        v.into_iter().collect()
    }
}
