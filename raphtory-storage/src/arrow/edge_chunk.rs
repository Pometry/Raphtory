use arrow2::{chunk::Chunk, array::Array};

struct EdgeChunk(Chunk<Box<dyn Array>>);