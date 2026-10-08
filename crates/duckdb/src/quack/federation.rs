use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::cast::as_boolean_array;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::StreamExt;

use crate::quack::sql_table::QuackTable;

impl QuackTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_reference = self.base_table.table_reference.clone();
        let schema = self.base_table.schema();
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_reference),
            schema,
        )))
    }

    pub(crate) fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[async_trait]
impl SQLExecutor for QuackTable {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.base_table.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect()
    }

    /// Runs `query` on the server. `filters` are predicates DataFusion pushed into the
    /// federated scan after planning and no longer applies itself, so they are evaluated
    /// here on every batch.
    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.base_table.execute(query, Arc::clone(&schema), &[])?;
        if filters.is_empty() {
            return Ok(stream);
        }
        let filters = filters.to_vec();
        let filtered = stream.map(move |batch| apply_filters(&batch?, &filters));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, filtered)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        self.base_table.table_names().await
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        self.base_table.get_table_schema(table_name).await
    }
}

/// Keeps the rows for which every filter is true; NULL counts as false.
fn apply_filters(
    batch: &RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> DataFusionResult<RecordBatch> {
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        let result = filter.evaluate(batch)?.into_array(batch.num_rows())?;
        let result = as_boolean_array(&result)?;
        mask = Some(match mask {
            Some(mask) => and(&mask, result)?,
            None => result.clone(),
        });
    }
    match mask {
        Some(mask) => Ok(filter_record_batch(batch, &mask)?),
        None => Ok(batch.clone()),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{col, is_not_null, lit, BinaryExpr};

    use super::*;

    #[test]
    fn apply_filters_keeps_rows_passing_every_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(5), None, Some(7)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                ])),
            ],
        )
        .unwrap();
        let greater: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("i", &schema).unwrap(),
            Operator::Gt,
            lit(2i32),
        ));
        let not_null = is_not_null(col("s", &schema).unwrap()).unwrap();

        let out = apply_filters(&batch, &[greater, not_null]).unwrap();
        assert_eq!(out.schema(), schema);
        assert_eq!(
            out.column(0).as_ref(),
            &Int32Array::from(vec![7]) as &dyn datafusion::arrow::array::Array
        );
    }
}
