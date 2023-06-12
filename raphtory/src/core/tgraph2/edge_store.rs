use serde::{Deserialize, Serialize};

use super::props::Props;

#[derive(Serialize, Deserialize)]
pub(crate) struct EdgeStore<const N: usize> {
    src: usize,
    dst: usize,
    // time_index: TimeIndex,
    props: Props,
}
