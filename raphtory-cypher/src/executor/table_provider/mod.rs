use arrow_schema::SchemaRef;
use datafusion::{physical_expr::EquivalenceProperties, physical_plan::PlanProperties};

pub mod edge;
pub mod node;
// FIXME this error shows up in datafusion 37 raised https://github.com/apache/datafusion/issues/10421
// called `Result::unwrap()` on an `Err` value: Context("EnforceDistribution", Internal("PhysicalOptimizer rule 'EnforceDistribution' failed, due to generate a different schema,
// schema: Schema { fields: [Field { name: \"name\", data_type: LargeUtf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"name\", data_type: LargeUtf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"name\", data_type: LargeUtf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} },
// schema: Schema { fields: [Field { name: \"name\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"name\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"name\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }], metadata: {} }"))
pub fn plan_properties(schema: SchemaRef, num_partitions: usize) -> PlanProperties {
    let eq_properties = EquivalenceProperties::new(schema);
    let partitioning = datafusion::physical_plan::Partitioning::UnknownPartitioning(num_partitions);
    let execution_mode = datafusion::physical_plan::ExecutionMode::Bounded;
    PlanProperties::new(eq_properties, partitioning, execution_mode)
}

