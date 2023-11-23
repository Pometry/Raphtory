use super::context::GlobalState;
use crate::{
    core::state::compute_state::ComputeState,
    db::{api::view::StaticGraphViewOps, task::vertex::eval_vertex::EvalVertexView},
};
use std::marker::PhantomData;

pub trait Task<G, CS, S>
where
    G: StaticGraphViewOps,
    CS: ComputeState,
{
    fn run<'a>(&self, vv: &mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step;
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub struct ATask<G, CS, S: 'static, F>
where
    G: StaticGraphViewOps,
    CS: ComputeState,
    F: for<'a> Fn(&mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step,
{
    f: F,
    _g: PhantomData<G>,
    _cs: PhantomData<CS>,
    _s: PhantomData<S>,
}

impl<
        G: StaticGraphViewOps,
        CS: ComputeState,
        S: 'static,
        F: for<'a> Fn(&mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step + Clone,
    > Clone for ATask<G, CS, S, F>
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _g: PhantomData,
            _cs: PhantomData,
            _s: PhantomData,
        }
    }
}

// determines if the task is executed for all vertices or only for updated vertices (vertices that had a state change since last sync)
pub enum Job<G, CS: ComputeState, S> {
    Read(Box<dyn Task<G, CS, S> + Sync + Send>),
    Write(Box<dyn Task<G, CS, S> + Sync + Send>),
    Check(Box<dyn Fn(&GlobalState<CS>) -> Step + Send + Sync + 'static>),
}

impl<G: StaticGraphViewOps, CS: ComputeState, S> Job<G, CS, S> {
    pub fn new<T: Task<G, CS, S> + Send + Sync + 'static>(t: T) -> Self {
        Self::Write(Box::new(t))
    }

    pub fn read_only<T: Task<G, CS, S> + Send + Sync + 'static>(t: T) -> Self {
        Self::Read(Box::new(t))
    }
}

impl<G, CS, S, F> ATask<G, CS, S, F>
where
    G: StaticGraphViewOps,
    CS: ComputeState,
    F: for<'a> Fn(&mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step,
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
    G: StaticGraphViewOps,
    CS: ComputeState,
    F: for<'a> Fn(&mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step + Clone,
{
    fn run<'a>(&self, vv: &mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step {
        (self.f)(vv)
    }
}
