pub mod accumulator_id;
pub mod agg;
pub mod compute_state;
pub mod container;
pub mod morcel_state;
pub mod shuffle_state;

pub trait StateType: PartialEq + Clone + std::fmt::Debug + Send + Sync + 'static {}

impl<T: PartialEq + Clone + std::fmt::Debug + Send + Sync + 'static> StateType for T {}
