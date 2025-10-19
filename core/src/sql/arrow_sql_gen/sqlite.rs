/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::sync::Arc;

use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder;
use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
        Int16Builder, Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder, NullBuilder,
        RecordBatch, RecordBatchOptions, StringBuilder, UInt16Builder, UInt32Builder,
        UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use rusqlite::{types::Type, Row, Rows};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("No builder found for index {index}"))]
    NoBuilderForIndex { index: usize },

    #[snafu(display("Failed to downcast builder for {sqlite_type}"))]
    FailedToDowncastBuilder { sqlite_type: String },

    #[snafu(display("Failed to extract row value: {source}"))]
    FailedToExtractRowValue { source: rusqlite::Error },

    #[snafu(display("Failed to extract column name: {source}"))]
    FailedToExtractColumnName { source: rusqlite::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts Sqlite `Row`s to an Arrow `RecordBatch`. Assumes that all rows have the same schema and
/// sets the schema based on the first row.
///
/// # Errors
///
/// Returns an error if there is a failure in converting the rows to a `RecordBatch`.
pub fn rows_to_arrow(
    mut rows: Rows,
    num_cols: usize,
    projected_schema: Option<SchemaRef>,
) -> Result<RecordBatch> {
    let mut arrow_fields: Vec<Field> = Vec::new();
    let mut arrow_columns_builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    let mut arrow_types: Vec<DataType> = Vec::new();
    let mut row_count = 0;

    if let Ok(Some(row)) = rows.next() {
        for i in 0..num_cols {
            let mut column_type = row
                .get_ref(i)
                .context(FailedToExtractRowValueSnafu)?
                .data_type();
            let column_name = row
                .as_ref()
                .column_name(i)
                .context(FailedToExtractColumnNameSnafu)?
                .to_string();

            // SQLite can store floating point values without a fractional component as integers.
            // Therefore, we need to verify if the column is actually a floating point type
            // by examining the projected schema.
            // Note: The same column may contain both integer and floating point values.
            // Reading values as Float is safe even if the value is stored as an integer.
            // Refer to the rusqlite type handling documentation for more details:
            // https://github.com/rusqlite/rusqlite/blob/95680270eca6f405fb51f5fbe6a214aac5fdce58/src/types/mod.rs#L21C1-L22C75
            //
            // `REAL` to integer: always returns an [`Error::InvalidColumnType`](crate::Error::InvalidColumnType) error.
            // `INTEGER` to float: casts using `as` operator. Never fails.
            // `REAL` to float: casts using `as` operator. Never fails.

            if column_type == Type::Integer {
                if let Some(projected_schema) = projected_schema.as_ref() {
                    match projected_schema.fields[i].data_type() {
                        DataType::Decimal128(..)
                        | DataType::Float16
                        | DataType::Float32
                        | DataType::Float64 => {
                            column_type = Type::Real;
                        }
                        _ => {}
                    }
                }
            }

            let data_type = match &projected_schema {
                Some(schema) => {
                    to_sqlite_decoding_type(schema.fields()[i].data_type(), &column_type)
                }
                None => map_column_type_to_data_type(column_type),
            };

            arrow_types.push(data_type.clone());
            arrow_columns_builders.push(map_data_type_to_array_builder(&data_type));
            arrow_fields.push(Field::new(column_name, data_type, true));
        }

        add_row_to_builders(row, &arrow_types, &mut arrow_columns_builders)?;
        row_count += 1;
    };

    while let Ok(Some(row)) = rows.next() {
        add_row_to_builders(row, &arrow_types, &mut arrow_columns_builders)?;
        row_count += 1;
    }

    let columns = arrow_columns_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<ArrayRef>>();

    let options = &RecordBatchOptions::new().with_row_count(Some(row_count));
    match RecordBatch::try_new_with_options(Arc::new(Schema::new(arrow_fields)), columns, options) {
        Ok(record_batch) => Ok(record_batch),
        Err(e) => Err(e).context(FailedToBuildRecordBatchSnafu),
    }
}

fn to_sqlite_decoding_type(data_type: &DataType, sqlite_type: &Type) -> DataType {
    if *sqlite_type == Type::Text {
        // Text is a special case as it can represent different types while correctly decoded to
        // desired Arrow type during additional type casting step.
        return DataType::Utf8;
    }
    // Other SQLite types are Integer, Real, Blob, Null are safe to decode based on target Arrow type
    match data_type {
        DataType::Null => DataType::Null,
        DataType::Int8 => DataType::Int8,
        DataType::Int16 => DataType::Int16,
        DataType::Int32 => DataType::Int32,
        DataType::Int64 => DataType::Int64,
        DataType::UInt8 => DataType::UInt8,
        DataType::UInt16 => DataType::UInt16,
        DataType::UInt32 => DataType::UInt32,
        DataType::UInt64 => DataType::UInt64,
        DataType::Boolean => DataType::Boolean,
        DataType::Float16 => DataType::Float16,
        DataType::Float32 => DataType::Float32,
        DataType::Float64 => DataType::Float64,
        DataType::Utf8 => DataType::Utf8,
        DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => DataType::Binary,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => DataType::Float64,
        DataType::Duration(_) => DataType::Int64,

        // Timestamp, Date32, Date64, Time32, Time64, List, Struct, Union, Dictionary, Map
        _ => DataType::Utf8,
    }
}

macro_rules! append_value {
    ($builder:expr, $row:expr, $index:expr, $type:ty, $builder_type:ty, $sqlite_type:expr) => {{
        let Some(builder) = $builder.as_any_mut().downcast_mut::<$builder_type>() else {
            FailedToDowncastBuilderSnafu {
                sqlite_type: format!("{}", $sqlite_type),
            }
            .fail()?
        };
        let value: Option<$type> = $row.get($index).context(FailedToExtractRowValueSnafu)?;
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }};
}

fn add_row_to_builders(
    row: &Row,
    arrow_types: &[DataType],
    arrow_columns_builders: &mut [Box<dyn ArrayBuilder>],
) -> Result<()> {
    for (i, arrow_type) in arrow_types.iter().enumerate() {
        let Some(builder) = arrow_columns_builders.get_mut(i) else {
            return NoBuilderForIndexSnafu { index: i }.fail();
        };

        match *arrow_type {
            DataType::Null => {
                let Some(builder) = builder.as_any_mut().downcast_mut::<NullBuilder>() else {
                    return FailedToDowncastBuilderSnafu {
                        sqlite_type: format!("{}", Type::Null),
                    }
                    .fail();
                };
                builder.append_null();
            }
            DataType::Int8 => append_value!(builder, row, i, i8, Int8Builder, Type::Integer),
            DataType::Int16 => append_value!(builder, row, i, i16, Int16Builder, Type::Integer),
            DataType::Int32 => append_value!(builder, row, i, i32, Int32Builder, Type::Integer),
            DataType::Int64 => append_value!(builder, row, i, i64, Int64Builder, Type::Integer),
            DataType::UInt8 => append_value!(builder, row, i, u8, UInt8Builder, Type::Integer),
            DataType::UInt16 => append_value!(builder, row, i, u16, UInt16Builder, Type::Integer),
            DataType::UInt32 => append_value!(builder, row, i, u32, UInt32Builder, Type::Integer),
            DataType::UInt64 => append_value!(builder, row, i, u64, UInt64Builder, Type::Integer),

            DataType::Boolean => {
                append_value!(builder, row, i, bool, BooleanBuilder, Type::Integer)
            }

            DataType::Float32 => append_value!(builder, row, i, f32, Float32Builder, Type::Real),
            DataType::Float64 => append_value!(builder, row, i, f64, Float64Builder, Type::Real),

            DataType::Utf8 => append_value!(builder, row, i, String, StringBuilder, Type::Text),
            DataType::LargeUtf8 => {
                append_value!(builder, row, i, String, LargeStringBuilder, Type::Text)
            }

            DataType::Binary => append_value!(builder, row, i, Vec<u8>, BinaryBuilder, Type::Blob),
            _ => {
                unimplemented!("Unsupported data type {arrow_type} for column index {i}")
            }
        }
    }

    Ok(())
}

fn map_column_type_to_data_type(column_type: Type) -> DataType {
    match column_type {
        Type::Null => DataType::Null,
        Type::Integer => DataType::Int64,
        Type::Real => DataType::Float64,
        Type::Text => DataType::Utf8,
        Type::Blob => DataType::Binary,
    }
}
