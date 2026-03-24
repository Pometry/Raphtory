use crate::storage::lazy_vec::IllegalSet;
use arrow_array::{
    builder::{ArrayBuilder, StringLikeArrayBuilder, StringViewBuilder},
    types::StringViewType,
    GenericByteViewArray,
};
use arrow_buffer::{
    bit_util::{get_bit, set_bit},
    Buffer, NullBufferBuilder,
};
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
use arrow_schema::ArrowError;

#[derive(Copy, Clone, Debug)]
struct BlockSizeGrowthStrategy {
    current_size: u32,
}
const STARTING_BLOCK_SIZE: BlockSizeGrowthStrategy = BlockSizeGrowthStrategy {
    current_size: 8 * 1024,
}; // 8KiB
const MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024; // 2MiB

impl BlockSizeGrowthStrategy {
    fn next_size(&mut self) -> u32 {
        if self.current_size < MAX_BLOCK_SIZE {
            // we have fixed start/end block sizes, so we can't overflow
            self.current_size = self.current_size.saturating_mul(2);
            self.current_size
        } else {
            MAX_BLOCK_SIZE
        }
    }
}

#[inline]
fn inline_view(bytes: &[u8]) -> Option<u128> {
    let len = bytes.len();
    if len <= MAX_INLINE_VIEW_LEN as usize {
        let mut view_buffer = [0; 16];
        view_buffer[0..4].copy_from_slice(&(len as u32).to_le_bytes());
        view_buffer[4..4 + len].copy_from_slice(bytes);
        Some(u128::from_le_bytes(view_buffer))
    } else {
        None
    }
}

#[derive(Debug)]
pub struct StringColBuilder {
    views_buffer: Vec<u128>,
    null_buffer_builder: NullBufferBuilder,
    completed: Vec<Buffer>,
    in_progress: Vec<u8>,
    block_size: BlockSizeGrowthStrategy,
}

impl StringColBuilder {
    pub fn len(&self) -> usize {
        self.views_buffer.len()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            views_buffer: Vec::with_capacity(capacity),
            null_buffer_builder: NullBufferBuilder::new(capacity),
            completed: vec![],
            in_progress: vec![],
            block_size: STARTING_BLOCK_SIZE,
        }
    }

    pub fn get_value(&self, index: usize) -> Option<&str> {
        let view = self.views_buffer.get(index)?;
        if self.null_buffer_builder.is_valid(index) {
            let len = *view as u32;
            let bytes = if len <= MAX_INLINE_VIEW_LEN {
                // # Safety
                // The view is valid from the builder
                unsafe { GenericByteViewArray::<StringViewType>::inline_value(view, len as usize) }
            } else {
                let view = ByteView::from(*view);
                if view.buffer_index < self.completed.len() as u32 {
                    let block = &self.completed[view.buffer_index as usize];
                    &block[view.offset as usize..view.offset as usize + view.length as usize]
                } else {
                    &self.in_progress
                        [view.offset as usize..view.offset as usize + view.length as usize]
                }
            };
            // # Safety
            // Strings in the builder are always valid
            Some(unsafe { str::from_utf8_unchecked(bytes) })
        } else {
            None
        }
    }

    /// Append a null value into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.null_buffer_builder.append_null();
        self.views_buffer.push(0);
    }

    #[inline]
    fn append_value_inner(&mut self, bytes: &[u8]) -> Result<ByteView, ArrowError> {
        let required_cap = self.in_progress.len() + bytes.len();
        if self.in_progress.capacity() < required_cap {
            self.flush_in_progress();
            let to_reserve = bytes.len().max(self.block_size.next_size() as usize);
            self.in_progress.reserve(to_reserve);
        };

        let offset = self.in_progress.len() as u32;
        self.in_progress.extend_from_slice(bytes);

        let buffer_index: u32 = self.completed.len().try_into().map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "Buffer count {} exceeds u32::MAX",
                self.completed.len()
            ))
        })?;

        let length: u32 = bytes.len().try_into().map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "String length {} exceeds u32::MAX",
                bytes.len()
            ))
        })?;

        let view = ByteView {
            length,
            // This won't panic as we checked the length of prefix earlier.
            prefix: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            buffer_index,
            offset,
        };
        Ok(view)
    }

    #[inline]
    fn update_value_inner(&mut self, index: usize, bytes: &[u8]) -> Result<(), ArrowError> {
        if let Some(inline_view) = inline_view(bytes) {
            // inline, only need to update the view
            self.views_buffer[index] = inline_view;
            return Ok(());
        }
        let new_len: u32 = bytes.len().try_into().map_err(|_| {
            ArrowError::InvalidArgumentError(format!(
                "String length {} exceeds u32::MAX",
                bytes.len()
            ))
        })?;
        let old_view = self.views_buffer[index];
        let old_len = old_view as u32;
        if old_len >= new_len {
            // can maybe reuse old allocation
            let mut view = ByteView::from(old_view);
            if view.buffer_index >= self.completed.len() as u32 {
                self.in_progress[view.offset as usize..view.offset as usize + bytes.len()]
                    .copy_from_slice(bytes);
                view.length = new_len;
                view.prefix = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
                self.views_buffer[index] = view.into();
                return Ok(());
            }
        }
        let view = self.append_value_inner(bytes)?;
        self.views_buffer[index] = view.into();
        Ok(())
    }

    #[inline]
    pub fn try_append_value(&mut self, value: &str) -> Result<(), ArrowError> {
        let v: &[u8] = value.as_ref();
        let length: u32 = v.len().try_into().map_err(|_| {
            ArrowError::InvalidArgumentError(format!("String length {} exceeds u32::MAX", v.len()))
        })?;

        if let Some(view) = inline_view(v) {
            self.views_buffer.push(view);
            self.null_buffer_builder.append_non_null();
            return Ok(());
        }

        let view = self.append_value_inner(v)?;
        self.views_buffer.push(view.into());
        self.null_buffer_builder.append_non_null();

        Ok(())
    }

    #[inline]
    pub fn append_value(&mut self, value: &str) {
        self.try_append_value(value).unwrap();
    }

    pub fn upsert_value(&mut self, index: usize, value: &str) -> Result<(), ArrowError> {
        if index >= self.len() {
            for _ in self.len()..index {
                self.append_null();
            }
            self.try_append_value(value)
        } else {
            let bytes = value.as_bytes();
            if let Some(inline_view) = inline_view(bytes) {
                // inline, only need to update the view
                self.views_buffer[index] = inline_view;
            } else {
                self.update_value_inner(index, bytes)?;
            }
            // set new entry as valid
            if !self.null_buffer_builder.is_valid(index) {
                let nulls = self
                    .null_buffer_builder
                    .as_slice_mut()
                    .expect("NullBufferBuilder with nulls should be materialized");
                set_bit(nulls, index);
            }
            Ok(())
        }
    }

    /// Flushes the in progress block if any
    #[inline]
    fn flush_in_progress(&mut self) {
        if !self.in_progress.is_empty() {
            let f = Buffer::from_vec(std::mem::take(&mut self.in_progress));
            self.push_completed(f)
        }
    }

    /// Append a block to `self.completed`, checking for overflow
    #[inline]
    fn push_completed(&mut self, block: Buffer) {
        assert!(block.len() < u32::MAX as usize, "Block too large");
        assert!(self.completed.len() < u32::MAX as usize, "Too many blocks");
        self.completed.push(block);
    }
}

#[derive(Debug)]
pub enum StringCol {
    Empty {
        len: usize,
    },
    One {
        len: usize,
        index: usize,
        value: String,
    },
    Many {
        values: StringColBuilder,
    },
}

impl StringCol {
    pub fn with_len(len: usize) -> Self {
        StringCol::Empty { len }
    }

    pub fn len(&self) -> usize {
        match self {
            StringCol::Empty { len } | StringCol::One { len, .. } => *len,
            StringCol::Many { values } => values.len(),
        }
    }

    pub fn get_opt(&self, i: usize) -> Option<&str> {
        match self {
            StringCol::Empty { .. } => None,
            StringCol::One { index, value, .. } => {
                if i == *index {
                    Some(value)
                } else {
                    None
                }
            }
            StringCol::Many { values } => values.get_value(i),
        }
    }

    pub fn upsert(&mut self, new_index: usize, new_value: &str) -> Result<(), ArrowError> {
        match self {
            StringCol::Empty { len } => {
                let len = (*len).max(new_index + 1);
                *self = StringCol::One {
                    len,
                    index: new_index,
                    value: new_value.to_string(),
                };
            }
            StringCol::One { len, index, value } => {
                if *index == new_index {
                    *value = new_value.to_string();
                } else {
                    let len = (*len).max(new_index + 1);
                    let (first_index, first_value, second_index, second_value) =
                        if *index < new_index {
                            (*index, value.as_str(), new_index, new_value)
                        } else {
                            (new_index, new_value, *index, value.as_str())
                        };
                    let mut values = StringViewBuilder::with_capacity(len);
                    for _ in 0..first_index {
                        values.append_null();
                    }
                    values.append_value(first_value);
                    for _ in first_index + 1..second_index {
                        values.append_null();
                    }
                    values.append_value(second_value);
                    for _ in second_index + 1..len {
                        values.append_null();
                    }
                }
            }
            StringCol::Many { values } => values.upsert_value(new_index, new_value)?,
        }
        Ok(())
    }

    pub fn check(&self, new_index: usize, new_value: &str) -> Result<(), IllegalSet<String>> {
        if let Some(old_value) = self.get_opt(new_index) {
            if old_value != new_value {
                return Err(IllegalSet::new(
                    new_index,
                    old_value.to_owned(),
                    new_value.to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub fn push_value(&mut self, new_value: &str) -> Result<(), ArrowError> {
        match self {
            StringCol::Empty { len } => {
                let index = self.len();
                let len = index + 1;
                let value = new_value.to_owned();
                *self = StringCol::One { len, index, value }
            }
            StringCol::One { index, value, len } => {
                let mut values = StringColBuilder::with_capacity(*len + 1);
                for _ in 0..*index {
                    values.append_null();
                }
                values.try_append_value(value)?;
                for _ in *index + 1..*len {
                    values.append_null();
                }
                values.try_append_value(new_value)?;
                *self = StringCol::Many { values };
            }
            StringCol::Many { values } => values.try_append_value(new_value)?,
        }
        Ok(())
    }

    pub fn push_null(&mut self) {
        match self {
            StringCol::Empty { len } => *len += 1,
            StringCol::One { len, .. } => *len += 1,
            StringCol::Many { values } => values.append_null(),
        }
    }
}
