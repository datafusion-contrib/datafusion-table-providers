use datafusion::arrow::{array::RecordBatch, datatypes::SchemaRef};
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

    use datafusion::arrow::datatypes::SchemaRef;
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

        let records =
            parquet_reader.collect::<Result<Vec<_>, datafusion::arrow::error::ArrowError>>()?;
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
}

/// Configuration options for upsert behavior
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpsertOptions {
    /// Remove duplicates after validation to resolve primary key conflicts
    pub remove_duplicates: bool,
    /// Use "last write wins" behavior - when duplicates are found, keep the row with the highest row number
    pub last_write_wins: bool,
}

impl UpsertOptions {
    /// Create a new instance with default settings
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_remove_duplicates(mut self, remove_duplicates: bool) -> Self {
        self.remove_duplicates = remove_duplicates;
        self
    }

    pub fn with_last_write_wins(mut self, last_write_wins: bool) -> Self {
        self.last_write_wins = last_write_wins;
        self
    }

    pub fn is_default(&self) -> bool {
        !self.remove_duplicates && !self.last_write_wins
    }
}
