use snafu::Snafu;

pub mod arrow;
pub mod schema;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build record batch: {source}"))]
    FailedToBuildRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("Failed to find field {column_name} in schema"))]
    FailedToFindFieldInSchema { column_name: String },

    #[snafu(display("Unsupported Trino type: {trino_type}"))]
    UnsupportedTrinoType { trino_type: String },

    #[snafu(display("Unsupported Arrow type: {arrow_type}"))]
    UnsupportedArrowType { arrow_type: String },

    #[snafu(display("Invalid date value: {value}"))]
    InvalidDateValue { value: String },

    #[snafu(display("Invalid time value: {value}"))]
    InvalidTimeValue { value: String },

    #[snafu(display("Invalid timestamp value: {value}"))]
    InvalidTimestampValue { value: String },

    #[snafu(display("Failed to parse decimal value: {value}"))]
    FailedToParseDecimal { value: String },

    #[snafu(display("Failed to downcast builder to expected type: {expected}"))]
    BuilderDowncastError { expected: String },

    #[snafu(display("Invalid or unsupported data type: {data_type}"))]
    InvalidDataType { data_type: String },

    #[snafu(display("Failed to parse precision from type: '{}'", trino_type))]
    InvalidPrecision { trino_type: String },

    #[snafu(display("Failed to compile regex pattern: {}", source))]
    RegexError { source: regex::Error },
}
