use crate::core::entities::VID;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Construct atomic slice from mut slice (reimplementation of currently unstable feature)
#[inline]
pub fn atomic_usize_from_mut_slice(v: &mut [usize]) -> &mut [AtomicUsize] {
    use std::mem::align_of;
    let [] = [(); align_of::<AtomicUsize>() - align_of::<usize>()];
    // SAFETY:
    //  - the mutable reference guarantees unique ownership.
    //  - the alignment of `usize` and `AtomicUsize` is the
    //    same, as verified above.
    unsafe { &mut *(v as *mut [usize] as *mut [AtomicUsize]) }
}

#[inline]
pub fn atomic_vid_from_mut_slice(v: &mut [VID]) -> &mut [AtomicUsize] {
    atomic_usize_from_mut_slice(bytemuck::cast_slice_mut(v))
}

#[inline]
pub fn atomic_u64_from_mut_slice(v: &mut [u64]) -> &mut [AtomicU64] {
    use std::mem::align_of;
    let [] = [(); align_of::<AtomicU64>() - align_of::<u64>()];
    // SAFETY:
    //  - the mutable reference guarantees unique ownership.
    //  - the alignment of `u64` and `AtomicU64` is the
    //    same, as verified above.
    unsafe { &mut *(v as *mut [u64] as *mut [AtomicU64]) }
}
