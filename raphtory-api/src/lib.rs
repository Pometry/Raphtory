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
/// ### Note on Complex Lifetime Bounds:
/// 
/// For functions with complex lifetime bounds like:
/// ```ignore
/// fn edge_iter<'a, 'b: 'a>(
///     &'a self,
///     layer_ids: &'b LayerIds,
/// ) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a
/// ```
/// 
/// You can manually apply the box_on_debug pattern:
/// ```ignore
/// #[cfg(debug_assertions)]
/// fn edge_iter<'a, 'b: 'a>(
///     &'a self,
///     layer_ids: &'b LayerIds,
/// ) -> Box<dyn Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a> {
///     let iter = match layer_ids {
///         LayerIds::None => Iter4::I(std::iter::empty()),
///         LayerIds::All => Iter4::J(self.edge_iter_layer(0)),
///         LayerIds::One(layer_id) => Iter4::K(self.edge_iter_layer(*layer_id)),
///         LayerIds::Multiple(multiple) => Iter4::L(
///             self.edge_iter_layer(0).filter(|pos| pos.has_layers(multiple)),
///         ),
///     };
///     Box::new(iter)
/// }
/// 
/// #[cfg(not(debug_assertions))]
/// fn edge_iter<'a, 'b: 'a>(
///     &'a self,
///     layer_ids: &'b LayerIds,
/// ) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
///     match layer_ids {
///         LayerIds::None => Iter4::I(std::iter::empty()),
///         LayerIds::All => Iter4::J(self.edge_iter_layer(0)),
///         LayerIds::One(layer_id) => Iter4::K(self.edge_iter_layer(*layer_id)),
///         LayerIds::Multiple(multiple) => Iter4::L(
///             self.edge_iter_layer(0).filter(|pos| pos.has_layers(multiple)),
///         ),
///     }
/// }
/// ```
///
#[macro_export]
macro_rules! box_on_debug {
    // Function with Send + Sync + lifetime bounds (most specific pattern)
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $($generics:tt)* > ($($params:tt)+) -> impl Iterator<Item = $item:ty> + Send + Sync + $output_lifetime:lifetime
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $($generics)* > ($($params)+) -> Box<dyn Iterator<Item = $item> + Send + Sync + $output_lifetime>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $($generics)* > ($($params)+) -> impl Iterator<Item = $item> + Send + Sync + $output_lifetime
        {
            $($body)*
        }
    };

    // General fallback pattern - no explicit generics in pattern to avoid ambiguity
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident ($($params:tt)+) -> impl Iterator<Item = $item:ty> $(+ $($bounds:tt)*)?
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
        $vis fn $name ($($params)+) -> impl Iterator<Item = $item> $(+ $($bounds)*)?
        {
            $($body)*
        }
    };
}

///
/// A specialized variant of box_on_debug for functions with complex lifetime parameters.
/// This macro handles functions that have explicit lifetime parameters and complex bounds.
///
/// # Usage
///
/// ## Function consuming self with lifetime parameter:
/// ```rust
/// use raphtory_api::box_on_debug_lifetime;
///
/// struct EdgeStorage;
/// struct LayerIds;
/// struct EdgeStorageEntry<'a>(&'a str);
///
/// impl EdgeStorage {
///     box_on_debug_lifetime! {
///         pub fn iter<'a>(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
///             std::iter::once(EdgeStorageEntry("test"))
///         }
///     }
/// }
/// ```
///
/// ## Method with complex lifetime bounds:
/// ```rust
/// use raphtory_api::box_on_debug_lifetime;
///
/// struct Graph;
/// struct LayerIds;
/// struct EntryRef<'a>(&'a str);
///
/// impl Graph {
///     box_on_debug_lifetime! {
///         fn edge_iter<'a, 'b: 'a>(
///             &'a self,
///             layer_ids: &'b LayerIds,
///         ) -> impl Iterator<Item = EntryRef<'a>> + Send + Sync + 'a {
///             std::iter::once(EntryRef("test"))
///         }
///     }
/// }
/// ```
/// 
/// ## Usage with your specific functions:
/// 
/// For a function like:
/// ```ignore
/// pub fn iter(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a
/// ```
/// 
/// Use:
/// ```ignore
/// box_on_debug_lifetime! {
///     pub fn iter<'a>(self, layer_ids: &'a LayerIds) -> impl Iterator<Item = EdgeStorageEntry<'a>> + 'a {
///         // your implementation
///     }
/// }
/// ```
/// 
/// For a function like:
/// ```ignore
/// fn edge_iter<'a, 'b: 'a>(&'a self, layer_ids: &'b LayerIds) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a
/// ```
/// 
/// Use:
/// ```ignore
/// box_on_debug_lifetime! {
///     fn edge_iter<'a, 'b: 'a>(&'a self, layer_ids: &'b LayerIds) -> impl Iterator<Item = Self::EntryRef<'a>> + Send + Sync + 'a {
///         // your implementation
///     }
/// }
/// ```
///
#[macro_export]
macro_rules! box_on_debug_lifetime {
    // Pattern for functions with single lifetime and bounds
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $lifetime:lifetime > (
            $($params:tt)*
        ) -> impl Iterator<Item = $item:ty> + $lifetime2:lifetime
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $lifetime > (
            $($params)*
        ) -> Box<dyn Iterator<Item = $item> + $lifetime2>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $lifetime > (
            $($params)*
        ) -> impl Iterator<Item = $item> + $lifetime2
        {
            $($body)*
        }
    };

    // Pattern for functions with multiple lifetimes and complex bounds like Send + Sync
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $lifetime1:lifetime , $lifetime2:lifetime : $lifetime3:lifetime > (
            $($params:tt)*
        ) -> impl Iterator<Item = $item:ty> + Send + Sync + $lifetime4:lifetime
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $lifetime1 , $lifetime2 : $lifetime3 > (
            $($params)*
        ) -> Box<dyn Iterator<Item = $item> + Send + Sync + $lifetime4>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $lifetime1 , $lifetime2 : $lifetime3 > (
            $($params)*
        ) -> impl Iterator<Item = $item> + Send + Sync + $lifetime4
        {
            $($body)*
        }
    };

    // Pattern for functions with multiple lifetimes without Send + Sync
    (
        $(#[$attr:meta])*
        $vis:vis fn $name:ident < $lifetime1:lifetime , $lifetime2:lifetime : $lifetime3:lifetime > (
            $($params:tt)*
        ) -> impl Iterator<Item = $item:ty> + $lifetime4:lifetime
        {
            $($body:tt)*
        }
    ) => {
        #[cfg(debug_assertions)]
        $(#[$attr])*
        $vis fn $name < $lifetime1 , $lifetime2 : $lifetime3 > (
            $($params)*
        ) -> Box<dyn Iterator<Item = $item> + $lifetime4>
        {
            let iter = { $($body)* };
            Box::new(iter)
        }

        #[cfg(not(debug_assertions))]
        $(#[$attr])*
        $vis fn $name < $lifetime1 , $lifetime2 : $lifetime3 > (
            $($params)*
        ) -> impl Iterator<Item = $item> + $lifetime4
        {
            $($body)*
        }
    };
}
