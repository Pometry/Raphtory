use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{mpsc::Sender, Arc},
};

use crate::core::{entities::VID, Direction};

pub enum Sink<S> {
    Channel(Vec<Sender<(S, VID)>>),
    Path(PathBuf, Arc<dyn Fn(&mut BufWriter<File>, S) + Send + Sync>),
    Void,
    Print,
}

#[derive(Clone)]
pub struct Hop {
    pub dir: Direction,
    pub limit: Option<usize>,
    pub layer: Box<str>,
    pub variable: bool,
}

#[derive(Clone)]
pub struct Query<S> {
    pub sink: Arc<Sink<S>>,
    pub hops: Vec<Hop>,
}

impl<S> Default for Query<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Query<S> {
    pub fn new() -> Self {
        Self {
            sink: Arc::new(Sink::Void),
            hops: vec![],
        }
    }

    pub fn get_hop(&self, step: usize) -> Option<&Hop> {
        self.hops.get(step)
    }

    pub fn hop(
        mut self,
        dir: Direction,
        layer: &str,
        variable: bool,
        limit: Option<usize>,
    ) -> Self {
        self.hops.push(Hop {
            dir,
            variable,
            layer: layer.into(),
            limit,
        });
        self
    }

    pub fn vhop(self, dir: Direction, layer: &str, limit: Option<usize>) -> Self {
        self.hop(dir, layer, true, limit)
    }

    pub fn out(self, layer: &str) -> Self {
        self.hop(Direction::OUT, layer, false, None)
    }

    pub fn out_limit(self, layer: &str, limit: usize) -> Self {
        self.hop(Direction::OUT, layer, false, Some(limit))
    }

    pub fn into_limit(self, layer: &str, limit: usize) -> Self {
        self.hop(Direction::IN, layer, false, Some(limit))
    }

    pub fn out_var(self, layer: &str) -> Self {
        self.hop(Direction::OUT, layer, true, None)
    }

    pub fn into(self, layer: &str) -> Self {
        self.hop(Direction::IN, layer, false, None)
    }

    pub fn with_sink(mut self, sink: Sink<S>) -> Self {
        self.sink = Arc::new(sink);
        self
    }

    pub fn channel(mut self, senders: impl IntoIterator<Item = Sender<(S, VID)>>) -> Self {
        self.sink = Arc::new(Sink::Channel(senders.into_iter().collect()));
        self
    }

    pub fn void(mut self) -> Self {
        self.sink = Arc::new(Sink::Void);
        self
    }

    pub fn print(mut self) -> Self {
        self.sink = Arc::new(Sink::Print);
        self
    }

    pub fn path(
        mut self,
        path: impl AsRef<Path>,
        writer: impl Fn(&mut BufWriter<File>, S) + Send + Sync + 'static,
    ) -> Self {
        self.sink = Arc::new(Sink::Path(path.as_ref().to_path_buf(), Arc::new(writer)));
        self
    }
}
