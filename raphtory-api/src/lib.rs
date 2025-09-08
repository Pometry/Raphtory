pub mod atomic_extra;
pub mod compute;
pub mod core;
#[cfg(feature = "python")]
pub mod python;

pub mod inherit;
pub mod iter;

#[derive(PartialOrd, PartialEq, Debug)]
pub enum GraphType {
    EventGraph,
    PersistentGraph,
}

///
/// A macro that boxes iterators in debug builds for better debugging experience.
/// In release builds, the function remains unchanged for optimal performance.
///
/// This macro transforms iterator-returning functions to return boxed iterators
/// only when debug assertions are enabled (`cfg(debug_assertions)`). This provides
/// better debugging experience by making iterator types concrete while maintaining
/// optimal performance in release builds.
///
/// # Usage
///
/// The macro supports functions with various combinations of:
/// - Generic parameters and lifetimes
/// - Where clauses
/// - Lifetime bounds on return types
///
/// ## Examples
///
/// ### Simple function without lifetimes:
/// ```rust
/// use raphtory_api::box_on_debug;
///
/// box_on_debug! {
///     pub fn simple_iter(count: usize) -> impl Iterator<Item = i32> {
///         (0..count as i32).filter(|x| x % 2 == 0)
///     }
/// }
/// ```
///
///
#[macro_export]
macro_rules! box_on_debug {
    // Function with at least one parameter and no where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident $(<$($generics:tt)*>)? ($($param:ident: $param_ty:ty),+ $(,)?) -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name $(<$($generics)*>)? ($($param: $param_ty),+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name $(<$($generics)*>)? ($($param: $param_ty),+) -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        {
            $($body)*
        }
    };
}
