use std::fs;
use std::fs::File;
use std::io::ErrorKind::InvalidData;
use std::io::Read;
use std::io::{BufReader, Error as IoError};

pub struct FVecs {
    pub dimensions: usize,
    pub vectors: Vec<f32>,
}

impl FVecs {
    pub fn from_file(file_name: &str) -> Result<Self, std::io::Error> {
        let data = fs::read(file_name)?;
        let (dim_data, vector_data) = data.split_at(4);
        let dim = dim_data
            .try_into()
            .map_err(|e| IoError::new(InvalidData, e))?;
        let dimensions = u32::from_le_bytes(dim) as usize;
        let vectors: Vec<_> = vector_data
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        Ok(Self {
            dimensions,
            vectors,
        })
    }

    pub fn split(&self) -> impl Iterator<Item = &[f32]> {
        self.vectors.chunks(self.dimensions)
    }

    pub fn get(&self, index: usize) -> Option<&[f32]> {
        self.split().nth(index)
    }
}

pub struct FVecsReader {
    pub dimensions: usize,
    reader: BufReader<File>,
    chunk_size: usize,
}

impl FVecsReader {
    pub fn from_file(filename: &str, chunk_size: usize) -> Result<Self, IoError> {
        let file = File::open(filename)?;
        let mut reader = BufReader::new(file);

        let mut buffer = vec![0; 4];
        let dimensions = match reader.read(&mut buffer) {
            Ok(4) => Ok(u32::from_le_bytes(buffer.try_into().unwrap()) as usize),
            Ok(_) => Err(IoError::from(InvalidData)),
            Err(e) => Err(e),
        }?;

        Ok(Self {
            dimensions,
            reader,
            chunk_size,
        })
    }
}

impl Iterator for FVecsReader {
    type Item = Vec<f32>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0; self.chunk_size * self.dimensions * 4];
        match self.reader.read(&mut buffer) {
            Err(_) => None,
            Ok(0) => None,
            Ok(n) => {
                buffer.truncate(n);
                let floats = buffer
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
                    .collect();
                Some(floats)
            }
        }
    }
}
