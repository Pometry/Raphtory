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
///     pub fn simple_iter() -> impl Iterator<Item = i32> {
///         (0..10).filter(|x| x % 2 == 0)
///     }
/// }
/// ```
///
/// ### Function with lifetimes:
/// ```rust
/// use raphtory_api::box_on_debug;
///
/// struct EdgeStorageEntry<'a>(&'a str);
///
/// box_on_debug! {
///     pub fn iter_with_lifetime<'a>(data: &'a str) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
///         std::iter::once(EdgeStorageEntry(data))
///     }
/// }
/// ```
///
/// ### Function with where clause:
/// ```rust
/// use raphtory_api::box_on_debug;
///
/// box_on_debug! {
///     pub fn iter_with_where_clause<T>() -> impl Iterator<Item = T>
///     where
///         T: Clone + Send,
///     {
///         std::iter::empty()
///     }
/// }
/// ```
#[macro_export]
macro_rules! box_on_debug {
    // Function with type parameters, empty params, and where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($generic:ident $(: $($bounds:tt)*)? ),+ > () -> impl Iterator<Item = $item:ty>
        where $($where_clause:tt)*
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($generic $(: $($bounds)*)?),+ > () -> Box<dyn Iterator<Item = $item> + Send + Sync>
        where $($where_clause)*
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($generic $(: $($bounds)*)?),+ > () -> impl Iterator<Item = $item>
        where $($where_clause)*
        {
            $($body)*
        }
    };

    // Function with type parameters and where clause (matches T: Clone + Send pattern)
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($generic:ident $(: $($bounds:tt)*)? ),+ > ($($param:ident: $param_ty:ty),+ $(,)?) -> impl Iterator<Item = $item:ty>
        where $($where_clause:tt)*
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($generic $(: $($bounds)*)?),+ > ($($param: $param_ty),+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        where $($where_clause)*
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($generic $(: $($bounds)*)?),+ > ($($param: $param_ty),+) -> impl Iterator<Item = $item>
        where $($where_clause)*
        {
            $($body)*
        }
    };

    // Function with lifetime parameters and where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($lifetime:lifetime),+ > ($($param:ident: $param_ty:ty),* $(,)?) -> impl Iterator<Item = $item:ty> + $output_lifetime:lifetime
        where $($where_clause:tt)*
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($param: $param_ty),*) -> Box<dyn Iterator<Item = $item> + Send + Sync + $output_lifetime>
        where $($where_clause)*
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($param: $param_ty),*) -> impl Iterator<Item = $item> + $output_lifetime
        where $($where_clause)*
        {
            $($body)*
        }
    };

    // Function with lifetime parameters but no where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($lifetime:lifetime),+ > ($($param:ident: $param_ty:ty),* $(,)?) -> impl Iterator<Item = $item:ty> + $output_lifetime:lifetime
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($param: $param_ty),*) -> Box<dyn Iterator<Item = $item> + Send + Sync + $output_lifetime>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($lifetime),+ > ($($param: $param_ty),*) -> impl Iterator<Item = $item> + $output_lifetime
        {
            $($body)*
        }
    };

    // Function without generics, empty params, with where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident () -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
        where $($where_clause:tt)*
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name () -> Box<dyn Iterator<Item = $item> + Send + Sync>
        where $($where_clause)*
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name () -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        where $($where_clause)*
        {
            $($body)*
        }
    };

    // Function without generics but with where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident ($($param:ident: $param_ty:ty),+ $(,)?) -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
        where $($where_clause:tt)*
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name ($($param: $param_ty),+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        where $($where_clause)*
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name ($($param: $param_ty),+) -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        where $($where_clause)*
        {
            $($body)*
        }
    };

    // Function without generics, empty params, no where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident () -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name () -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name () -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        {
            $($body)*
        }
    };

    // Function without generics and no where clause
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident ($($param:ident: $param_ty:ty),+ $(,)?) -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name ($($param: $param_ty),+) -> Box<dyn Iterator<Item = $item> + Send + Sync>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name ($($param: $param_ty),+) -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        {
            $($body)*
        }
    };
}
