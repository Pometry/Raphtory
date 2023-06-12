use serde::{Deserialize, Serialize};

use super::adj::Adj;

#[derive(Serialize, Deserialize)]
pub(crate) struct EdgeLayer {
    adj: Adj,
}
