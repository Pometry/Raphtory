macro_rules! impl_dynamic_from {
    ($name:ident) => {
        impl<G: StaticGraphViewOps + IntoDynamic + Clone + Static> From<$name<G>>
            for $name<DynamicGraph>
        {
            fn from(v: $name<G>) -> Self {
                let graph = v.current_filter().clone().into_dynamic();
                v.one_hop_filtered(graph)
            }
        }
    };
}

pub(crate) mod history;
pub(crate) mod node;
mod properties;

use crate::db::api::view::{
    internal::{OneHopFilter, Static},
    DynamicGraph, IntoDynamic,
};
pub use history::*;
pub use node::*;
pub use properties::*;
