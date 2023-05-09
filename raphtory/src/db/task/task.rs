use crate::core::state::compute_state::ComputeState;
use crate::db::view_api::GraphViewOps;
use crate::db::view_api::internal::GraphViewInternalOps;

use super::context::GlobalState;
use super::eval_vertex::EvalVertexView;

pub trait Task<G, CS>
where
    G: GraphViewOps,
    CS: ComputeState,
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step;
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub struct ATask<G, CS, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    f: F,
    _g: std::marker::PhantomData<G>,
    _cs: std::marker::PhantomData<CS>,
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum Job<G, CS: ComputeState> {
    Read(Box<dyn Task<G, CS> + Sync + Send>),
    Write(Box<dyn Task<G, CS> + Sync + Send>),
    Check(Box<dyn Fn(&GlobalState<CS>) -> Step + Send + Sync + 'static>),
}

impl<G: GraphViewInternalOps + Send + Sync + Clone + 'static, CS: ComputeState> Job<G, CS> {
    pub fn new<T: Task<G, CS> + Send + Sync + 'static>(t: T) -> Self {
        Self::Write(Box::new(t))
    }

    pub fn read_only<T: Task<G, CS> + Send + Sync + 'static>(t: T) -> Self {
        Self::Read(Box::new(t))
    }
}

impl<G, CS, F> ATask<G, CS, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _g: std::marker::PhantomData,
            _cs: std::marker::PhantomData,
        }
    }
}

impl<G, CS, F> Task<G, CS> for ATask<G, CS, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&EvalVertexView<G, CS>) -> Step,
{
    fn run(&self, vv: &EvalVertexView<G, CS>) -> Step {
        (self.f)(vv)
    }
}
