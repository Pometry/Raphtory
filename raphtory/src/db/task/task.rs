use super::context::GlobalState;
use crate::{
    core::state::compute_state::ComputeState,
    db::{api::view::GraphViewOps, task::vertex::eval_vertex::EvalVertexView},
};
use std::marker::PhantomData;

pub trait Task<G, CS, S>
where
    G: GraphViewOps,
    CS: ComputeState,
{
    fn run(&self, vv: &mut EvalVertexView<G, CS, S>) -> Step;
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub struct ATask<G, CS, S, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&mut EvalVertexView<G, CS, S>) -> Step,
{
    f: F,
    _g: PhantomData<G>,
    _cs: PhantomData<CS>,
    _s: PhantomData<S>,
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum Job<G, CS: ComputeState, S> {
    Read(Box<dyn Task<G, CS, S> + Sync + Send>),
    Write(Box<dyn Task<G, CS, S> + Sync + Send>),
    Check(Box<dyn Fn(&GlobalState<CS>) -> Step + Send + Sync + 'static>),
}

impl<G: GraphViewOps, CS: ComputeState, S> Job<G, CS, S> {
    pub fn new<T: Task<G, CS, S> + Send + Sync + 'static>(t: T) -> Self {
        Self::Write(Box::new(t))
    }

    pub fn read_only<T: Task<G, CS, S> + Send + Sync + 'static>(t: T) -> Self {
        Self::Read(Box::new(t))
    }
}

impl<G, CS, S, F> ATask<G, CS, S, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&mut EvalVertexView<G, CS, S>) -> Step,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _g: PhantomData,
            _cs: PhantomData,
            _s: PhantomData,
        }
    }
}

impl<G, CS, S, F> Task<G, CS, S> for ATask<G, CS, S, F>
where
    G: GraphViewOps,
    CS: ComputeState,
    F: Fn(&mut EvalVertexView<G, CS, S>) -> Step,
{
    fn run(&self, vv: &mut EvalVertexView<G, CS, S>) -> Step {
        (self.f)(vv)
    }
}
