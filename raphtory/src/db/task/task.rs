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
    fn run(&self, vv: &mut EvalVertexView<&G, S, &G, CS>) -> Step;
}

#[derive(Debug, PartialEq)]
pub enum Step {
    Done,
    Continue,
}

pub struct ATask<'a, G, CS, S: 'static, F>
where
    G: StaticGraphViewOps,
    CS: ComputeState + 'a,
    F: Fn(&'a mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step,
{
    f: F,
    _g: PhantomData<&'a G>,
    _cs: PhantomData<CS>,
    _s: PhantomData<S>,
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

impl<'a, G, CS, S, F> ATask<'a, G, CS, S, F>
where
    G: StaticGraphViewOps,
    CS: ComputeState,
    F: Fn(&mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step,
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

impl<'a, G, CS, S, F> Task<G, CS, S> for ATask<'a, G, CS, S, F>
where
    G: StaticGraphViewOps,
    CS: ComputeState,
    F: Fn(&'a mut EvalVertexView<'a, &'a G, S, &'a G, CS>) -> Step,
{
    fn run(&self, vv: &mut EvalVertexView<&G, S, &G, CS>) -> Step {
        (self.f)(vv)
    }
}
