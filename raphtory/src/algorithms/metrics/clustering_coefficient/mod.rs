use crate::{core::entities::nodes::node_ref::AsNodeRef, db::api::view::*};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

pub mod global_clustering_coefficient;
pub mod local_clustering_coefficient;
pub mod local_clustering_coefficient_batch;
