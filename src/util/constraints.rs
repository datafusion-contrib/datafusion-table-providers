use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    common::{Constraint, Constraints},
    execution::context::SessionContext,
    functions_aggregate::count::count,
    logical_expr::{col, lit, utils::COUNT_STAR_EXPANSION},
    prelude::ident,
};
use futures::future;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Incoming data violates uniqueness constraint on column(s): {}", unique_cols.join(", ")))]
    BatchViolatesUniquenessConstraint { unique_cols: Vec<String> },

    #[snafu(display("{source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The goal for this function is to determine if all of the data described in `batches` conforms to the constraints described in `constraints`.
///
/// It does this by creating a memory table from the record batches and then running a query against the table to validate the constraints.
pub async fn validate_batch_with_constraints(
    batches: &[RecordBatch],
    constraints: &Constraints,
) -> Result<()> {
    if batches.is_empty() || constraints.is_empty() {
        return Ok(());
    }

    let mut futures = Vec::new();
    for constraint in &**constraints {
        let fut = validate_batch_with_constraint(batches.to_vec(), constraint.clone());
        futures.push(fut);
    }

    future::try_join_all(futures).await?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip(batches))]
async fn validate_batch_with_constraint(
    batches: Vec<RecordBatch>,
    constraint: Constraint,
) -> Result<()> {
    let unique_cols = match constraint {
        Constraint::PrimaryKey(cols) | Constraint::Unique(cols) => cols,
    };

    let schema = batches[0].schema();
    let unique_fields = unique_cols
        .iter()
        .map(|col| schema.field(*col))
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches).context(DataFusionSnafu)?;

    let count_name = count(lit(COUNT_STAR_EXPANSION)).schema_name().to_string();

    // This is equivalent to:
    // ```sql
    // SELECT COUNT(1), <unique_field_names> FROM mem_table GROUP BY <unique_field_names> HAVING COUNT(1) > 1
    // ```
    let num_rows = df
        .aggregate(
            unique_fields.iter().map(|f| ident(f.name())).collect(),
            vec![count(lit(COUNT_STAR_EXPANSION))],
        )
        .context(DataFusionSnafu)?
        .filter(col(count_name).gt(lit(1)))
        .context(DataFusionSnafu)?
        .count()
        .await
        .context(DataFusionSnafu)?;

    if num_rows > 0 {
        BatchViolatesUniquenessConstraintSnafu {
            unique_cols: unique_fields
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<_>>(),
        }
        .fail()?;
    }

    Ok(())
}

#[must_use]
pub fn get_primary_keys_from_constraints(
    constraints: &Constraints,
    schema: &SchemaRef,
) -> Vec<String> {
    let mut primary_keys: Vec<String> = Vec::new();
    for constraint in constraints.clone() {
        if let Constraint::PrimaryKey(cols) = constraint {
            cols.iter()
                .map(|col| schema.field(*col).name())
                .for_each(|col| {
                    primary_keys.push(col.to_string());
                });
        }
    }
    primary_keys
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::{
        common::{Constraint, Constraints},
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    };

    #[tokio::test]
    async fn test_validate_batch_with_constraints() -> Result<(), Box<dyn std::error::Error>> {
        let parquet_bytes = reqwest::get("https://public-data.spiceai.org/taxi_sample.parquet")
            .await?
            .bytes()
            .await?;

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)?.build()?;

        let records = parquet_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        let schema = records[0].schema();

        let constraints = get_unique_constraints(
            &["VendorID", "tpep_pickup_datetime", "PULocationID"],
            Arc::clone(&schema),
        );

        let result = super::validate_batch_with_constraints(&records, &constraints).await;
        assert!(
            result.is_ok(),
            "{}",
            result.expect_err("this returned an error")
        );

        let invalid_constraints = get_unique_constraints(&["VendorID"], Arc::clone(&schema));
        let result = super::validate_batch_with_constraints(&records, &invalid_constraints).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("this returned an error").to_string(),
            "Incoming data violates uniqueness constraint on column(s): VendorID"
        );

        let invalid_constraints =
            get_unique_constraints(&["VendorID", "tpep_pickup_datetime"], Arc::clone(&schema));
        let result = super::validate_batch_with_constraints(&records, &invalid_constraints).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("this returned an error").to_string(),
            "Incoming data violates uniqueness constraint on column(s): VendorID, tpep_pickup_datetime"
        );

        Ok(())
    }

    pub(crate) fn get_unique_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        // Convert column names to their corresponding indices
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|&col_name| schema.column_with_name(col_name).map(|(index, _)| index))
            .collect();

        Constraints::new_unverified(vec![Constraint::Unique(indices)])
    }

    pub(crate) fn get_pk_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        // Convert column names to their corresponding indices
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|&col_name| schema.column_with_name(col_name).map(|(index, _)| index))
            .collect();

        Constraints::new_unverified(vec![Constraint::PrimaryKey(indices)])
    }

    /// Builder for creating test RecordBatches with specific data patterns
    #[derive(Debug)]
    pub struct TestDataBuilder {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl TestDataBuilder {
        pub fn new(schema: SchemaRef) -> Self {
            Self {
                schema,
                batches: Vec::new(),
            }
        }

        pub fn with_columns(columns: &[(&str, DataType)]) -> Self {
            let fields: Vec<Field> = columns
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            Self::new(schema)
        }

        pub fn add_string_batch(
            self,
            data: Vec<Vec<Option<&str>>>,
        ) -> Result<Self, arrow::error::ArrowError> {
            let columns: Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                (0..self.schema.fields().len())
                    .map(|col_idx| {
                        let column_data: Vec<Option<String>> = data
                            .iter()
                            .map(|row| row.get(col_idx).and_then(|v| v.map(|s| s.to_string())))
                            .collect();
                        Ok(Arc::new(StringArray::from(column_data)) as ArrayRef)
                    })
                    .collect();

            let batch = RecordBatch::try_new(self.schema.clone(), columns?)?;
            let mut new_self = self;
            new_self.batches.push(batch);
            Ok(new_self)
        }

        pub fn add_int_batch(
            self,
            data: Vec<Vec<Option<i32>>>,
        ) -> Result<Self, arrow::error::ArrowError> {
            let columns: Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                (0..self.schema.fields().len())
                    .map(|col_idx| {
                        let column_data: Vec<Option<i32>> = data
                            .iter()
                            .map(|row| row.get(col_idx).copied().flatten())
                            .collect();
                        Ok(Arc::new(Int32Array::from(column_data)) as ArrayRef)
                    })
                    .collect();

            let batch = RecordBatch::try_new(self.schema.clone(), columns?)?;
            let mut new_self = self;
            new_self.batches.push(batch);
            Ok(new_self)
        }

        pub fn build(self) -> Vec<RecordBatch> {
            self.batches
        }
    }

    /// Builder for creating different types of constraints
    #[derive(Debug)]
    pub struct ConstraintBuilder {
        schema: SchemaRef,
    }

    impl ConstraintBuilder {
        pub fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }

        /// Create a unique constraint on the specified columns
        pub fn unique_on(&self, column_names: &[&str]) -> Constraints {
            get_unique_constraints(column_names, self.schema.clone())
        }

        /// Create a primary key constraint on the specified columns
        pub fn primary_key_on(&self, column_names: &[&str]) -> Constraints {
            get_pk_constraints(column_names, self.schema.clone())
        }
    }

    pub enum Expect {
        Pass,
        Fail,
    }

    /// Helper for testing constraint validation scenarios
    pub struct ConstraintTestCase {
        pub name: String,
        pub batches: Vec<RecordBatch>,
        pub constraints: Constraints,
        pub should_pass: Expect,
        pub expected_error_contains: Option<String>,
    }

    impl ConstraintTestCase {
        /// Create a new test case
        pub fn new(
            name: &str,
            batches: Vec<RecordBatch>,
            constraints: Constraints,
            should_pass: Expect,
        ) -> Self {
            Self {
                name: name.to_string(),
                batches,
                constraints,
                should_pass,
                expected_error_contains: None,
            }
        }

        /// Set expected error message substring
        pub fn with_expected_error(mut self, error_substr: &str) -> Self {
            self.expected_error_contains = Some(error_substr.to_string());
            self
        }

        /// Run the test case
        pub async fn run(self) -> Result<(), anyhow::Error> {
            let result =
                super::validate_batch_with_constraints(&self.batches, &self.constraints).await;

            match (self.should_pass, result) {
                (Expect::Pass, Ok(())) => {
                    println!("✓ Test '{}' passed as expected", self.name);
                    Ok(())
                }
                (Expect::Fail, Err(err)) => {
                    if let Some(expected_substr) = &self.expected_error_contains {
                        let err_str = err.to_string();
                        if err_str.contains(expected_substr) {
                            println!(
                                "✓ Test '{}' failed as expected with error: {}",
                                self.name, err_str
                            );
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!(
                                "Test '{}' failed with unexpected error. Expected substring '{}', got: {}",
                                self.name, expected_substr, err_str
                            ))
                        }
                    } else {
                        println!(
                            "✓ Test '{}' failed as expected with error: {}",
                            self.name, err
                        );
                        Ok(())
                    }
                }
                (Expect::Pass, Err(err)) => Err(anyhow::anyhow!(
                    "Test '{}' was expected to pass but failed with error: {}",
                    self.name,
                    err
                )),
                (Expect::Fail, Ok(())) => Err(anyhow::anyhow!(
                    "Test '{}' was expected to fail but passed",
                    self.name
                )),
            }
        }
    }

    #[tokio::test]
    async fn test_valid_unique_constraint_no_duplicates() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("id", DataType::Utf8),
            ("name", DataType::Utf8),
            ("category", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice"), Some("A")],
                vec![Some("2"), Some("Bob"), Some("B")],
                vec![Some("3"), Some("Charlie"), Some("A")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "valid unique constraint on id",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_unique_constraint_duplicate_values() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("name", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice")],
                vec![Some("1"), Some("Bob")],
                vec![Some("2"), Some("Charlie")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "invalid unique constraint - duplicate ids",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_valid_composite_unique_constraint() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("user_id", DataType::Utf8),
            ("product_id", DataType::Utf8),
            ("rating", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("A"), Some("5")],
                vec![Some("1"), Some("B"), Some("4")],
                vec![Some("2"), Some("A"), Some("3")],
                vec![Some("2"), Some("B"), Some("5")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["user_id", "product_id"]);

        ConstraintTestCase::new(
            "valid composite unique constraint",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_composite_unique_constraint() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("user_id", DataType::Utf8),
            ("product_id", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("A")],
                vec![Some("1"), Some("B")],
                vec![Some("1"), Some("A")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["user_id", "product_id"]);

        ConstraintTestCase::new(
            "invalid composite unique constraint",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): user_id, product_id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_valid_primary_key_constraint() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("pk", DataType::Utf8), ("data", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("pk1"), Some("data1")],
                vec![Some("pk2"), Some("data2")],
                vec![Some("pk3"), Some("data3")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.primary_key_on(&["pk"]);

        ConstraintTestCase::new(
            "valid primary key constraint",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_primary_key_constraint() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("pk", DataType::Utf8), ("data", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("pk1"), Some("data1")],
                vec![Some("pk1"), Some("data2")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.primary_key_on(&["pk"]);

        ConstraintTestCase::new(
            "invalid primary key constraint",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): pk")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_multiple_batches_with_valid_constraints() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![vec![Some("1"), Some("A")], vec![Some("2"), Some("B")]])?
            .add_string_batch(vec![vec![Some("3"), Some("C")], vec![Some("4"), Some("D")]])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "multiple batches with valid constraints",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_multiple_batches_with_cross_batch_violations() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![vec![Some("1"), Some("A")], vec![Some("2"), Some("B")]])?
            .add_string_batch(vec![
                vec![Some("1"), Some("C")],
                vec![Some("4"), Some("D")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "multiple batches with cross-batch violations",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_integer_data_with_valid_constraints() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("int_id", DataType::Int32),
            ("value", DataType::Int32),
        ]);

        let batches = data_builder
            .add_int_batch(vec![
                vec![Some(1), Some(100)],
                vec![Some(2), Some(200)],
                vec![Some(3), Some(300)],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["int_id"]);

        ConstraintTestCase::new(
            "integer data with valid constraints",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_empty_batch_handling() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[("id", DataType::Utf8)]);
        let empty_batches = data_builder.build();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let constraint_builder = ConstraintBuilder::new(schema);
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "empty batches should pass",
            empty_batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }
}
