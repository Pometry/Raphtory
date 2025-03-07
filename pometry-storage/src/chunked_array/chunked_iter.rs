use crate::prelude::Chunked;
use rayon::prelude::IndexedParallelIterator;
use std::ops::Range;

pub trait IntoChunkedIter<'a>: Chunked
where
    Self::Chunk: Send + 'a,
{
    fn into_par_iter_chunks(self) -> impl IndexedParallelIterator<Item = &'a Self::Chunk>;

    fn into_par_iter_chunks_range(
        self,
        range: Range<usize>,
    ) -> impl IndexedParallelIterator<Item = &'a Self::Chunk>;
}

pub struct RangeChunkedIter<C, CI> {
    first_chunk: Option<C>,
    last_chunk: Option<C>,
    middle_chunks: CI,
    first_done: bool,
    last_done: bool,
}

impl<C, CI> RangeChunkedIter<C, CI> {
    pub fn new(first_chunk: Option<C>, last_chunk: Option<C>, middle_chunks: CI) -> Self {
        Self {
            first_chunk,
            last_chunk,
            middle_chunks,
            first_done: false,
            last_done: false,
        }
    }
}

impl<'a, C, CI> Iterator for RangeChunkedIter<C, CI>
where
    CI: Iterator<Item = &'a C>,
    C: 'a,
    Self: 'a,
{
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        let next = if !self.first_done && self.first_chunk.is_some() {
            self.first_chunk
                .as_ref()
                .map(|chunk| unsafe { change_lifetime_const(chunk) }) // safety: First chunk lives as long as self and is not modified
        } else if let Some(chunk) = self.middle_chunks.next() {
            Some(chunk)
        } else if !self.last_done {
            self.last_done = true;
            self.last_chunk
                .as_ref()
                .map(|v| unsafe { change_lifetime_const(v) }) // safety: Last chunk lives as long as self and is not modified
        } else {
            None
        };
        self.first_done = true;
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (mut min, mut max) = self.middle_chunks.size_hint();
        if !self.first_done {
            min = min.saturating_add(1);
            max = max.map(|max| max.saturating_add(1));
        }
        if !self.last_done && self.last_chunk.is_some() {
            min = min.saturating_add(1);
            max = max.map(|max| max.saturating_add(1));
        }
        (min, max)
    }
}

impl<'a, C, CI> DoubleEndedIterator for RangeChunkedIter<C, CI>
where
    CI: DoubleEndedIterator<Item = &'a C> + 'a,
    C: 'a,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let next_back = if !self.last_done && self.last_chunk.is_some() {
            self.last_chunk
                .as_ref()
                .map(|v| unsafe { change_lifetime_const(v) })
        } else if let Some(chunk) = self.middle_chunks.next_back() {
            Some(chunk)
        } else if !self.first_done {
            self.first_done = true;
            self.first_chunk
                .as_ref()
                .map(|chunk| unsafe { change_lifetime_const(chunk) })
        } else {
            None
        };
        self.last_done = true;
        next_back
    }
}

impl<'a, C, CI> ExactSizeIterator for RangeChunkedIter<C, CI>
where
    CI: ExactSizeIterator<Item = &'a C> + 'a,
    C: 'a,
{
}

// impl<'a, C, CI> ParallelIterator for RangeChunkedIter<C, CI>
// where
//     CI: IndexedParallelIterator<Item = &'a C>,
//     C: Send + Sync + 'a,
//     Self: 'a,
// {
//     type Item = &'a C;

//     fn drive_unindexed<C1>(self, consumer: C1) -> C1::Result
//     where
//         C1: UnindexedConsumer<Self::Item>,
//     {
//         bridge(self, consumer)
//     }

//     fn opt_len(&self) -> Option<usize> {
//         Some(self.len())
//     }
// }

// impl<'a, C, CI> IndexedParallelIterator for RangeChunkedIter<C, CI>
// where
//     CI: IndexedParallelIterator<Item = &'a C>,
//     C: Send + Sync + 'a,
//     Self: 'a,
// {
//     fn len(&self) -> usize {
//         let mut len = self.middle_chunks.len();
//         if !self.first_done && self.first_chunk.is_some() {
//             len += 1;
//         }

//         if !self.last_done && self.last_chunk.is_some() {
//             len += 1;
//         }
//         len
//     }

//     fn drive<C1: Consumer<Self::Item>>(self, consumer: C1) -> C1::Result {
//         bridge(self, consumer)
//     }

//     fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
//         self.middle_chunks.with_producer(PCB{cb: callback})
//     }
// }
// struct PCB<CB>{cb: CB}

// impl <C, CB: ProducerCallback<C>> ProducerCallback<C> for PCB<CB>{
//     type Output = CB::Output;

//     fn callback<P>(self, producer: P) -> Self::Output
//     where
//         P: Producer<Item = C> {
//         todo!()
//     }
// }

// pub trait AllIter: Iterator + DoubleEndedIterator + ExactSizeIterator {}

// impl<I> AllIter for I where I: Iterator + DoubleEndedIterator + ExactSizeIterator {}

// impl<'a, C, CI> Producer for RangeChunkedIter<C, CI>
// where
//     CI: IndexedParallelIterator<Item = &'a C>,
//     C: Send + Sync + 'a,
//     Self: 'a,
// {
//     type Item = &'a C;

//     type IntoIter = Box<dyn AllIter<Item = Self::Item> + 'a>;

//     fn into_iter(self) -> Self::IntoIter {
//         Box::new(std::iter::empty())
//     }

//     fn split_at(self, index: usize) -> (Self, Self) {
//         self.middle_chunks
//     }
// }

/// # Safety
///
/// Requires that you ensure the reference does not become invalid.
/// The object has to outlive the reference.
pub unsafe fn change_lifetime_const<'b, T>(x: &T) -> &'b T {
    &*(x as *const T)
}
