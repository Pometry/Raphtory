//! SHACL validation of graph views: the SHACL Core engine of rudof's `shacl` crate reads the
//! triples [`to_rdf`](crate::RdfView::to_rdf) writes straight from the view, one lookup at a time.
mod nesting;
mod report;
mod shapes;
mod store;

pub use report::{ShaclReport, ShaclResult};
pub use shapes::{ShaclLimit, ShaclOptions, ShaclShapes};
