use arrow_schema::SchemaRef;
use datafusion::{
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        PlanProperties,
    },
};

pub mod edge;
pub mod node;
pub fn plan_properties(schema: SchemaRef, num_partitions: usize) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema);
    let partitioning = datafusion::physical_plan::Partitioning::UnknownPartitioning(num_partitions);
    PlanProperties::new(
        eq_properties,
        partitioning,
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}
