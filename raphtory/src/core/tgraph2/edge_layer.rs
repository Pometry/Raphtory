use serde::{Deserialize, Serialize};

use super::adj::Adj;

#[derive(Serialize, Deserialize)]
pub(crate) struct EdgeLayer {
    adj: Adj,
}

impl EdgeLayer {
    pub fn new() -> Self {
        Self { adj: Adj::Solo }
    }
}
