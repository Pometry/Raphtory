//! Standalone, lock-free gid -> VID resolver for bulk loading.
//! See `gid_resolver.md` next to this file for the design.
//!
//! Per chunk:
//!   1. hash every src/dst key once, in parallel
//!   2. radix-partition the row indices by shard (parallel histogram + scatter)
//!   3. each shard is owned by exactly one rayon task and only touches its own rows:
//!      a. probe: hits write their VID, misses get a provisional (pending) id
//!      b. allocate: reserve exactly as many VIDs as the shard has new nodes
//!      c. patch: rewrite pending entries / rows with the real VIDs

use crate::errors::{GraphError, LoadError};
use arrow::array::{Array, AsArray, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Int32Type, Int64Type, UInt32Type, UInt64Type};
use hashbrown::{hash_table::Entry, HashTable};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};

const SHARD_BITS: u32 = 8;
const NUM_SHARDS: usize = 1 << SHARD_BITS;
// hashbrown uses the low bits for the bucket and the top 7 bits for the tag,
// so take the shard from the bits in between.
const SHARD_SHIFT: u32 = 48;
const BLOCK: usize = 1 << 16;
// VIDs never get near 2^63, so the top bit marks a provisional id
const PENDING: u64 = 1 << 63;

/// Hands out VIDs for new nodes.
pub trait VidAllocator: Sync {
    /// Reserve up to `count` (> 0) consecutive VIDs, `hint` picks a slot to spread contention.
    /// Returns the first VID and how many were reserved (>= 1).
    fn reserve(&self, hint: usize, count: u32) -> (u64, u32);
}

impl<F: Fn(usize, u32) -> (u64, u32) + Sync> VidAllocator for F {
    fn reserve(&self, hint: usize, count: u32) -> (u64, u32) {
        self(hint, count)
    }
}

/// Dense `0..n` VIDs from a single counter.
#[derive(Default)]
pub struct DenseAllocator(AtomicU64);

impl VidAllocator for DenseAllocator {
    fn reserve(&self, _hint: usize, count: u32) -> (u64, u32) {
        (self.0.fetch_add(count as u64, Ordering::Relaxed), count)
    }
}

#[inline(always)]
fn shard_of(hash: u64) -> usize {
    ((hash >> SHARD_SHIFT) as usize) & (NUM_SHARDS - 1)
}

#[inline(always)]
fn hash_u64(x: u64) -> u64 {
    let m = (x ^ 0x2d35_8dcc_aa6c_78a5) as u128 * 0x9e37_79b9_7f4a_7c15u128;
    (m as u64) ^ ((m >> 64) as u64)
}

#[inline(always)]
fn hash_bytes(b: &[u8]) -> u64 {
    // fixed seeds so the resolver is deterministic across runs
    ahash::RandomState::with_seeds(1, 2, 3, 4).hash_one(b)
}

#[derive(Copy, Clone)]
struct StrEntry {
    off: usize,
    len: usize,
    hash: u64,
    id: u64,
}

/// A key first seen in the current chunk, kept so its table entry can be patched.
#[derive(Copy, Clone)]
enum NewKey {
    Int { hash: u64, key: u64 },
    Str { hash: u64, off: usize },
}

#[derive(Default)]
struct Shard {
    ints: HashTable<(u64, u64)>,
    strs: HashTable<StrEntry>,
    arena: Vec<u8>,
    num_nodes: usize,
    // per-chunk scratch
    new_keys: Vec<NewKey>,
    pending_rows: Vec<(u32, u32)>,
    new_vids: Vec<u64>,
    /// (row, vid) of the first row of every node created by the last chunk
    new_nodes: Vec<(u32, u64)>,
}

impl Shard {
    #[inline(always)]
    fn pending(&mut self, new_key: NewKey) -> u64 {
        let id = PENDING | self.new_keys.len() as u64;
        self.new_keys.push(new_key);
        id
    }

    #[inline(always)]
    fn probe_int(&mut self, hash: u64, key: u64) -> u64 {
        match self.ints.entry(hash, |e| e.0 == key, |e| hash_u64(e.0)) {
            Entry::Occupied(e) => e.get().1,
            Entry::Vacant(v) => {
                let id = PENDING | self.new_keys.len() as u64;
                self.new_keys.push(NewKey::Int { hash, key });
                v.insert((key, id));
                id
            }
        }
    }

    #[inline(always)]
    fn probe_str(&mut self, hash: u64, key: &[u8]) -> u64 {
        let arena = &self.arena;
        if let Some(e) = self.strs.find(hash, |e| {
            e.hash == hash && &arena[e.off..e.off + e.len] == key
        }) {
            return e.id;
        }
        let off = self.arena.len();
        self.arena.extend_from_slice(key);
        let id = self.pending(NewKey::Str { hash, off });
        self.strs.insert_unique(
            hash,
            StrEntry {
                off,
                len: key.len(),
                hash,
                id,
            },
            |e| e.hash,
        );
        id
    }

    /// Reserves exactly one VID per new key and patches the table entries.
    fn allocate(&mut self, shard_id: usize, alloc: &impl VidAllocator) {
        self.new_vids.clear();
        let mut remaining = self.new_keys.len();
        while remaining > 0 {
            let want = remaining.min(u32::MAX as usize) as u32;
            let (start, got) = alloc.reserve(shard_id, want);
            self.new_vids.extend(start..start + got as u64);
            remaining -= got as usize;
        }
        for (key, &vid) in self.new_keys.iter().zip(&self.new_vids) {
            match *key {
                NewKey::Int { hash, key } => {
                    self.ints.find_mut(hash, |e| e.0 == key).unwrap().1 = vid;
                }
                NewKey::Str { hash, off } => {
                    self.strs.find_mut(hash, |e| e.off == off).unwrap().id = vid;
                }
            }
        }
        self.num_nodes += self.new_keys.len();
        self.new_keys.clear();
    }
}

#[derive(Copy, Clone)]
struct SyncPtr<T>(*mut T);
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}
impl<T> SyncPtr<T> {
    #[inline(always)]
    unsafe fn write(self, i: usize, v: T) {
        self.0.add(i).write(v)
    }
}

enum Col<'a> {
    U64(&'a [u64]),
    I64(&'a [i64]),
    U32(&'a [u32]),
    I32(&'a [i32]),
    Str(&'a StringArray),
    LargeStr(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> Col<'a> {
    fn new(arr: &'a dyn Array) -> Result<Self, GraphError> {
        Ok(match arr.data_type() {
            DataType::UInt64 => Col::U64(arr.as_primitive::<UInt64Type>().values()),
            DataType::Int64 => Col::I64(arr.as_primitive::<Int64Type>().values()),
            DataType::UInt32 => Col::U32(arr.as_primitive::<UInt32Type>().values()),
            DataType::Int32 => Col::I32(arr.as_primitive::<Int32Type>().values()),
            DataType::Utf8 => Col::Str(arr.as_string::<i32>()),
            DataType::LargeUtf8 => Col::LargeStr(arr.as_string::<i64>()),
            DataType::Utf8View => Col::View(arr.as_string_view()),
            dt => return Err(LoadError::InvalidNodeIdType(dt.clone()).into()),
        })
    }

    fn is_str(&self) -> bool {
        matches!(self, Col::Str(_) | Col::LargeStr(_) | Col::View(_))
    }

    #[inline(always)]
    fn int(&self, i: usize) -> u64 {
        unsafe {
            match self {
                Col::U64(v) => *v.get_unchecked(i),
                Col::I64(v) => *v.get_unchecked(i) as u64,
                Col::U32(v) => *v.get_unchecked(i) as u64,
                Col::I32(v) => *v.get_unchecked(i) as i64 as u64,
                _ => unreachable!(),
            }
        }
    }

    #[inline(always)]
    fn bytes(&self, i: usize) -> &'a [u8] {
        unsafe {
            match self {
                Col::Str(a) => a.value_unchecked(i).as_bytes(),
                Col::LargeStr(a) => a.value_unchecked(i).as_bytes(),
                Col::View(a) => a.value_unchecked(i).as_bytes(),
                _ => unreachable!(),
            }
        }
    }
}

pub struct GidResolver {
    shards: Vec<Shard>,
    // scratch, reused across chunks
    hashes: Vec<u64>,
    part_idx: Vec<u32>,
    part_hash: Vec<u64>,
    hist: Vec<[u32; NUM_SHARDS]>,
}

impl Default for GidResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl GidResolver {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// `expected_nodes` pre-sizes the shard tables so they don't rehash while loading.
    pub fn with_capacity(expected_nodes: usize) -> Self {
        let per_shard = expected_nodes.div_ceil(NUM_SHARDS);
        let shards = (0..NUM_SHARDS)
            .map(|_| Shard {
                ints: HashTable::with_capacity(per_shard),
                ..Default::default()
            })
            .collect();
        Self {
            shards,
            hashes: vec![],
            part_idx: vec![],
            part_hash: vec![],
            hist: vec![],
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.shards.iter().map(|s| s.num_nodes).sum()
    }

    /// `(row, vid)` for every node created by the last `resolve` call, where `row`
    /// indexes the concatenated `[src, dst]` columns (`row >= len` is `dst[row - len]`).
    /// Grouped by resolver shard, not sorted.
    pub fn new_nodes(&self) -> impl ParallelIterator<Item = (usize, u64)> + '_ {
        self.shards
            .par_iter()
            .flat_map_iter(|s| s.new_nodes.iter().map(|&(row, vid)| (row as usize, vid)))
    }

    /// Resolves `src` and `dst`, `out` is resized to `2 * len` with the src VIDs
    /// followed by the dst VIDs. New nodes get VIDs from `alloc`.
    pub fn resolve<'o>(
        &mut self,
        src: &dyn Array,
        dst: &dyn Array,
        alloc: &impl VidAllocator,
        out: &'o mut Vec<u64>,
    ) -> Result<(&'o [u64], &'o [u64]), GraphError> {
        let n = src.len();
        assert_eq!(n, dst.len());
        assert!(2 * n <= u32::MAX as usize);
        let src = Col::new(src)?;
        let dst = Col::new(dst)?;
        if src.is_str() != dst.is_str() {
            return Err(LoadError::InvalidNodeIdType(DataType::Utf8View).into());
        }

        out.resize(2 * n, 0);
        if src.is_str() {
            let get = |i: usize| {
                if i < n {
                    src.bytes(i)
                } else {
                    dst.bytes(i - n)
                }
            };
            self.run(
                2 * n,
                |i| hash_bytes(get(i)),
                |shard, h, i| shard.probe_str(h, get(i)),
                alloc,
                out,
            );
        } else {
            let get = |i: usize| if i < n { src.int(i) } else { dst.int(i - n) };
            self.run(
                2 * n,
                |i| hash_u64(get(i)),
                |shard, h, i| shard.probe_int(h, get(i)),
                alloc,
                out,
            );
        }
        let (s, d) = out.split_at(n);
        Ok((s, d))
    }

    fn run(
        &mut self,
        len: usize,
        hash: impl Fn(usize) -> u64 + Sync,
        probe: impl Fn(&mut Shard, u64, usize) -> u64 + Sync,
        alloc: &impl VidAllocator,
        out: &mut [u64],
    ) {
        self.shards.par_iter_mut().for_each(|s| s.new_nodes.clear());
        if len == 0 {
            return;
        }
        let num_blocks = len.div_ceil(BLOCK);

        // 1. hash + per-block histogram
        self.hashes.resize(len, 0);
        self.hist.resize(num_blocks, [0; NUM_SHARDS]);
        self.hashes
            .par_chunks_mut(BLOCK)
            .zip(self.hist.par_iter_mut())
            .enumerate()
            .for_each(|(b, (hs, hist))| {
                *hist = [0; NUM_SHARDS];
                let base = b * BLOCK;
                for (j, h) in hs.iter_mut().enumerate() {
                    *h = hash(base + j);
                    hist[shard_of(*h)] += 1;
                }
            });

        // 2. exclusive prefix sum: shard-major, block-minor
        let mut shard_start = [0usize; NUM_SHARDS + 1];
        let mut acc = 0u32;
        for s in 0..NUM_SHARDS {
            shard_start[s] = acc as usize;
            for hist in self.hist.iter_mut() {
                let c = hist[s];
                hist[s] = acc;
                acc += c;
            }
        }
        shard_start[NUM_SHARDS] = acc as usize;

        // 3. scatter (each block writes to disjoint ranges)
        self.part_idx.resize(len, 0);
        self.part_hash.resize(len, 0);
        let idx_ptr = SyncPtr(self.part_idx.as_mut_ptr());
        let hash_ptr = SyncPtr(self.part_hash.as_mut_ptr());
        self.hashes
            .par_chunks(BLOCK)
            .zip(self.hist.par_iter_mut())
            .enumerate()
            .for_each(|(b, (hs, offsets))| {
                let base = b * BLOCK;
                for (j, &h) in hs.iter().enumerate() {
                    let s = shard_of(h);
                    let pos = offsets[s] as usize;
                    offsets[s] += 1;
                    unsafe {
                        idx_ptr.write(pos, (base + j) as u32);
                        hash_ptr.write(pos, h);
                    }
                }
            });

        // 4. resolve, one task per shard, rows in each shard are disjoint
        let out_ptr = SyncPtr(out.as_mut_ptr());
        let part_idx = &self.part_idx;
        let part_hash = &self.part_hash;
        self.shards
            .par_iter_mut()
            .enumerate()
            .with_max_len(1)
            .for_each(|(sid, shard)| {
                let range = shard_start[sid]..shard_start[sid + 1];
                shard.pending_rows.clear();

                // a. probe
                for (&i, &h) in part_idx[range.clone()].iter().zip(&part_hash[range]) {
                    let id = probe(shard, h, i as usize);
                    if id & PENDING == 0 {
                        unsafe { out_ptr.write(i as usize, id) };
                    } else {
                        shard.pending_rows.push((i, (id & !PENDING) as u32));
                    }
                }

                // b. allocate exactly what this shard needs
                let num_new = shard.new_keys.len();
                shard.allocate(sid, alloc);

                // c. patch; the first pending row of every new key is where it was created
                let mut seen = 0;
                for &(i, k) in &shard.pending_rows {
                    let vid = shard.new_vids[k as usize];
                    unsafe { out_ptr.write(i as usize, vid) };
                    if k as usize == seen {
                        shard.new_nodes.push((i, vid));
                        seen += 1;
                    }
                }
                debug_assert_eq!(seen, num_new);
            });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{StringViewArray, UInt64Array};
    use std::{
        collections::{HashMap, HashSet},
        sync::Mutex,
    };

    fn check(
        ids: &[u64],
        seen: &mut HashMap<u64, usize>,
        map: &mut HashMap<usize, u64>,
        key_ix: impl Fn(usize) -> usize,
    ) {
        for (row, &id) in ids.iter().enumerate() {
            let k = key_ix(row);
            if let Some(&prev) = map.get(&k) {
                assert_eq!(prev, id);
            } else {
                assert!(seen.insert(id, k).is_none(), "id reused for different keys");
                map.insert(k, id);
            }
        }
    }

    #[test]
    fn ints_are_unique_and_stable() {
        let mut r = GidResolver::new();
        let alloc = DenseAllocator::default();
        let mut out = vec![];
        let mut seen = HashMap::new();
        let mut map = HashMap::new();
        for chunk in 0..3u64 {
            let src: Vec<u64> = (0..100_000u64).map(|i| (i * 7 + chunk) % 50_000).collect();
            let dst: Vec<u64> = (0..100_000u64)
                .map(|i| (i * 13 + chunk * 3) % 70_000)
                .collect();
            let (s, d) = r
                .resolve(
                    &UInt64Array::from(src.clone()),
                    &UInt64Array::from(dst.clone()),
                    &alloc,
                    &mut out,
                )
                .unwrap();
            check(s, &mut seen, &mut map, |row| src[row] as usize);
            check(d, &mut seen, &mut map, |row| dst[row] as usize);
        }
        assert_eq!(r.num_nodes(), map.len());
        // dense allocator: ids are exactly 0..n
        assert_eq!(*seen.keys().max().unwrap() as usize, map.len() - 1);
    }

    #[test]
    fn strings_are_unique_and_stable() {
        let mut r = GidResolver::new();
        let alloc = DenseAllocator::default();
        let mut out = vec![];
        let mut seen = HashMap::new();
        let mut map = HashMap::new();
        for chunk in 0..3usize {
            let src: Vec<String> = (0..50_000)
                .map(|i| format!("node_{}", (i * 7 + chunk) % 20_000))
                .collect();
            let dst: Vec<String> = (0..50_000)
                .map(|i| format!("node_{}", (i * 13 + chunk) % 30_000))
                .collect();
            let (s, d) = r
                .resolve(
                    &StringViewArray::from_iter_values(&src),
                    &StringViewArray::from_iter_values(&dst),
                    &alloc,
                    &mut out,
                )
                .unwrap();
            let num = |s: &String| s[5..].parse::<usize>().unwrap();
            check(s, &mut seen, &mut map, |row| num(&src[row]));
            check(d, &mut seen, &mut map, |row| num(&dst[row]));
        }
        assert_eq!(r.num_nodes(), map.len());
    }

    /// Mimics `NodeStorageInner::reserve_free_range`: `slots` free segments, each fills up to
    /// `max_len` before a new segment is pushed.
    struct SegmentAllocator {
        max_len: u32,
        // (free segment per slot, row counter per segment)
        state: Mutex<(Vec<usize>, Vec<u32>)>,
    }

    impl SegmentAllocator {
        fn new(slots: usize, max_len: u32) -> Self {
            Self {
                max_len,
                state: Mutex::new(((0..slots).collect(), vec![0; slots])),
            }
        }

        fn counters(&self) -> Vec<u32> {
            self.state.lock().unwrap().1.clone()
        }
    }

    impl VidAllocator for SegmentAllocator {
        fn reserve(&self, hint: usize, count: u32) -> (u64, u32) {
            let mut guard = self.state.lock().unwrap();
            let (slots, counters) = &mut *guard;
            let slot = hint % slots.len();
            loop {
                let seg = slots[slot];
                let cur = counters[seg];
                let got = count.min(self.max_len - cur);
                if got > 0 {
                    counters[seg] += got;
                    return (seg as u64 * self.max_len as u64 + cur as u64, got);
                }
                counters.push(0);
                slots[slot] = counters.len() - 1;
            }
        }
    }

    #[test]
    fn segment_allocation_has_no_holes() {
        let max_len = 1000;
        let alloc = SegmentAllocator::new(8, max_len);
        let mut r = GidResolver::new();
        let mut out = vec![];
        let mut new_vids = HashSet::new();
        for chunk in 0..4u64 {
            let src = UInt64Array::from_iter_values((0..20_000).map(|i| (i * 7 + chunk) % 9_000));
            let dst = UInt64Array::from_iter_values((0..20_000).map(|i| (i * 13 + chunk) % 11_000));
            r.resolve(&src, &dst, &alloc, &mut out).unwrap();
            let created: Vec<_> = r.new_nodes().collect();
            for (row, vid) in created {
                assert_eq!(out[row], vid);
                assert!(new_vids.insert(vid));
            }
        }
        assert_eq!(new_vids.len(), r.num_nodes());
        // every reserved row is used exactly once
        let counters = alloc.counters();
        let reserved: usize = counters.iter().map(|&c| c as usize).sum();
        assert_eq!(reserved, r.num_nodes());
        for vid in new_vids {
            let (seg, pos) = (vid / max_len as u64, (vid % max_len as u64) as u32);
            assert!(pos < counters[seg as usize]);
        }
    }

    /// cargo test --release -p raphtory gid_resolver::test::bench -- --ignored --nocapture
    #[test]
    #[ignore]
    fn bench() {
        let rows = 10_000_000u64;
        let nodes = 5_000_000u64;
        let mut r = GidResolver::new();
        let alloc = SegmentAllocator::new(rayon::current_num_threads(), 1 << 20);
        let mut out = vec![];
        let mut total = 0;
        let start = std::time::Instant::now();
        for chunk in 0..10u64 {
            let src = UInt64Array::from_iter_values(
                (0..rows).map(|i| hash_u64(i + chunk * rows) % nodes),
            );
            let dst = UInt64Array::from_iter_values(
                (0..rows).map(|i| hash_u64(!(i + chunk * rows)) % nodes),
            );
            let t = std::time::Instant::now();
            r.resolve(&src, &dst, &alloc, &mut out).unwrap();
            let e = t.elapsed();
            total += rows;
            println!(
                "chunk {chunk}: {:.1}M rows/s",
                rows as f64 / e.as_secs_f64() / 1e6
            );
        }
        println!(
            "nodes {} rows {total} wall {:?}",
            r.num_nodes(),
            start.elapsed()
        );
    }
}
