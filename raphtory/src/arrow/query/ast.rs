use std::sync::{mpsc::Sender, Arc};

use crate::{
    arrow::{edge::Edge, nodes::Node},
    core::{entities::VID, Direction},
};

use super::state::HopState;

pub enum Sink<S> {
    Channel(Sender<(S, VID)>),
    Void,
    Print,
}

pub struct Hop<S> {
    pub dir: Direction,
    pub filter: Option<Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
    pub variable: bool,
}

pub struct Query<S> {
    pub sink: Sink<S>,
    pub hops: Vec<Hop<S>>,
}

impl<S: HopState> Query<S> {
    pub fn new() -> Self {
        Self {
            sink: Sink::Void,
            hops: vec![],
        }
    }

    pub fn get_hop(&self, step: usize) -> Option<&Hop<S>> {
        self.hops.get(step)
    }

    pub fn hop(
        mut self,
        dir: Direction,
        filter: Option<Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
        variable: bool,
    ) -> Self {
        self.hops.push(Hop { dir, filter, variable });
        self
    }

    pub fn vhop(
        self,
        dir: Direction,
        filter: Option<Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
    ) -> Self {
        self.hop(dir, filter, true)
    }

    pub fn out(self) -> Self {
        self.hop(Direction::OUT, None, false)
    }

    pub fn out_var(self) -> Self{
        self.hop(Direction::OUT, None, true)
    }

    pub fn out_filter(
        self,
        filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>,
    ) -> Self {
        self.hop(Direction::OUT, Some(filter), false)
    }

    pub fn into(self) -> Self {
        self.hop(Direction::IN, None, false)
    }

    pub fn into_filter(
        self,
        filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>,
    ) -> Self {
        self.hop(Direction::IN, Some(filter), false)
    }

    pub fn with_sink(mut self, sink: Sink<S>) -> Self {
        self.sink = sink;
        self
    }

    pub fn channel(mut self, sender: Sender<(S, VID)>) -> Self {
        self.sink = Sink::Channel(sender);
        self
    }
}
