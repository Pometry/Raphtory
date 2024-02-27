use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{mpsc::Sender, Arc},
};

use crate::{
    arrow::{edge::Edge, nodes::Node},
    core::{entities::VID, Direction},
};

use super::state::HopState;

pub enum Sink<S> {
    Channel(Vec<Sender<(S, VID)>>),
    Path(PathBuf, Arc<dyn Fn(&mut BufWriter<File>, S) + Send + Sync>),
    Void,
    Print,
}

pub struct Hop<S> {
    pub dir: Direction,
    pub filter: Option<Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
    pub limit: Option<usize>,
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
        limit: Option<usize>,
    ) -> Self {
        self.hops.push(Hop {
            dir,
            filter,
            variable,
            limit,
        });
        self
    }

    pub fn vhop(
        self,
        dir: Direction,
        filter: Option<Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>>,
        limit: Option<usize>,
    ) -> Self {
        self.hop(dir, filter, true, limit)
    }

    pub fn out(self) -> Self {
        self.hop(Direction::OUT, None, false, None)
    }

    pub fn out_limit(self, limit: usize) -> Self {
        self.hop(Direction::OUT, None, false, Some(limit))
    }

    pub fn out_var(self) -> Self {
        self.hop(Direction::OUT, None, true, None)
    }

    pub fn out_filter(self, filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>) -> Self {
        self.hop(Direction::OUT, Some(filter), false, None)
    }

    pub fn out_filter_limit(
        self,
        limit: usize,
        filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>,
    ) -> Self {
        self.hop(Direction::OUT, Some(filter), false, Some(limit))
    }

    pub fn into(self) -> Self {
        self.hop(Direction::IN, None, false, None)
    }

    pub fn into_filter(self, filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>) -> Self {
        self.hop(Direction::IN, Some(filter), false, None)
    }

    pub fn into_filter_limit(
        self,
        limit: usize,
        filter: Arc<dyn (Fn(Node, Edge, &S) -> bool) + Send + Sync>,
    ) -> Self {
        self.hop(Direction::IN, Some(filter), false, Some(limit))
    }

    pub fn with_sink(mut self, sink: Sink<S>) -> Self {
        self.sink = sink;
        self
    }

    pub fn channel(mut self, senders: impl IntoIterator<Item = Sender<(S, VID)>>) -> Self {
        self.sink = Sink::Channel(senders.into_iter().collect());
        self
    }

    pub fn void(mut self) -> Self {
        self.sink = Sink::Void;
        self
    }

    pub fn path(
        mut self,
        path: impl AsRef<Path>,
        writer: impl Fn(&mut BufWriter<File>, S) + Send + Sync + 'static,
    ) -> Self {
        self.sink = Sink::Path(path.as_ref().to_path_buf(), Arc::new(writer));
        self
    }
}
