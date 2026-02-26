use crate::LocalPOS;

#[derive(Debug)]
pub struct LayerStats {
    nodes: roaring::RoaringBitmap
}

impl LayerStats {
    pub fn new() -> Self {
        Self {
            nodes: roaring::RoaringBitmap::new()
        }
    }

    pub fn add_node(&mut self, node_pos: LocalPOS) {
        self.nodes.insert(node_pos.0);
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len() as usize
    }

    pub fn has_node(&self, node_pos: LocalPOS) -> bool {
        self.nodes.contains(node_pos.0)
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = LocalPOS> + '_ {
        self.nodes.iter().map(LocalPOS)
    }
}