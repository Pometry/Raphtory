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
/// ### Method with `&self` parameter:
/// ```rust
/// use raphtory_api::box_on_debug;
///
/// struct Graph {
///     node_count: usize,
/// }
///
/// impl Graph {
///     box_on_debug! {
///         pub fn iter_node_ids(&self) -> impl Iterator<Item = usize> {
///             0..self.node_count
///         }
///     }
/// }
/// ```
///
/// ### Function with explicit lifetime parameter:
/// ```rust
/// use raphtory_api::box_on_debug;
///
/// box_on_debug! {
///     pub fn generate_range<'a>(start: i32, count: &'a usize) -> impl Iterator<Item = i32> {
///         (start..start + (*count as i32))
///     }
/// }
/// ```
///
#[macro_export]
macro_rules! box_on_debug {
    // Function with explicit lifetime parameters - must come first for specificity
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($lifetime:lifetime),+ > ($($params:tt)+) -> impl Iterator<Item = $item:ty>
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($params)+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($params)+) -> impl Iterator<Item = $item>
        {
            $($body)*
        }
    };

    // Function with other generics (type parameters, const generics)
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($generics:ident),+ > ($($params:tt)+) -> impl Iterator<Item = $item:ty>
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($generics),+ > ($($params)+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($generics),+ > ($($params)+) -> impl Iterator<Item = $item>
        {
            $($body)*
        }
    };

    // Function with no generics
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident ($($params:tt)+) -> impl Iterator<Item = $item:ty>
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name ($($params)+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name ($($params)+) -> impl Iterator<Item = $item>
        {
            $($body)*
        }
    };
}
