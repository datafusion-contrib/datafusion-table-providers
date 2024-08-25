use arrow::array::RecordBatch;
use arrow::{
    array::*,
    compute::cast,
    datatypes::{
        i256, DataType, Date32Type, Date64Type, Field, Fields, IntervalDayTime,
        IntervalMonthDayNano, IntervalUnit, Schema, SchemaRef, TimeUnit,
    },
};
use chrono::NaiveDate;
use std::sync::Arc;

// Helper functions to create arrow record batches of different types

// Binary/LargeBinary/FixedSizeBinary
pub(crate) fn get_arrow_binary_record_batch() -> (RecordBatch, SchemaRef) {
    // Binary/LargeBinary/FixedSizeBinary Array
    let values: Vec<&[u8]> = vec![b"one", b"two", b""];
    let binary_array = BinaryArray::from_vec(values.clone());
    let large_binary_array = LargeBinaryArray::from_vec(values);
    let input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let fixed_size_binary_array =
        FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("binary", DataType::Binary, false),
        Field::new("large_binary", DataType::LargeBinary, false),
        Field::new("fixed_size_binary", DataType::FixedSizeBinary(2), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(binary_array),
            Arc::new(large_binary_array),
            Arc::new(fixed_size_binary_array),
        ],
    )
    .expect("Failed to created arrow binary record batch");

    (record_batch, schema)
}

// All Int types
pub(crate) fn get_arrow_int_record_batch() -> (RecordBatch, SchemaRef) {
    // Arrow Integer Types
    let int8_arr = Int8Array::from(vec![1, 2, 3]);
    let int16_arr = Int16Array::from(vec![1, 2, 3]);
    let int32_arr = Int32Array::from(vec![1, 2, 3]);
    let int64_arr = Int64Array::from(vec![1, 2, 3]);
    let uint8_arr = UInt8Array::from(vec![1, 2, 3]);
    let uint16_arr = UInt16Array::from(vec![1, 2, 3]);
    let uint32_arr = UInt32Array::from(vec![1, 2, 3]);
    let uint64_arr = UInt64Array::from(vec![1, 2, 3]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("int8", DataType::Int8, false),
        Field::new("int16", DataType::Int16, false),
        Field::new("int32", DataType::Int32, false),
        Field::new("int64", DataType::Int64, false),
        Field::new("uint8", DataType::UInt8, false),
        Field::new("uint16", DataType::UInt16, false),
        Field::new("uint32", DataType::UInt32, false),
        Field::new("uint64", DataType::UInt64, false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(int8_arr),
            Arc::new(int16_arr),
            Arc::new(int32_arr),
            Arc::new(int64_arr),
            Arc::new(uint8_arr),
            Arc::new(uint16_arr),
            Arc::new(uint32_arr),
            Arc::new(uint64_arr),
        ],
    )
    .expect("Failed to created arrow binary record batch");

    (record_batch, schema)
}

// All Float Types
pub(crate) fn get_arrow_float_record_batch() -> (RecordBatch, SchemaRef) {
    // Arrow Float Types
    let float32_arr = Float32Array::from(vec![1.0, 2.0, 3.0]);
    let float64_arr = Float64Array::from(vec![1.0, 2.0, 3.0]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("float32", DataType::Float32, false),
        Field::new("float64", DataType::Float64, false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(float32_arr), Arc::new(float64_arr)],
    )
    .expect("Failed to created arrow float record batch");

    (record_batch, schema)
}

// Utf8/LargeUtf8
pub(crate) fn get_arrow_utf8_record_batch() -> (RecordBatch, SchemaRef) {
    // Utf8, LargeUtf8 Types
    let string_arr = StringArray::from(vec!["foo", "bar", "baz"]);
    let large_string_arr = LargeStringArray::from(vec!["foo", "bar", "baz"]);
    let bool_arr: BooleanArray = vec![true, true, false].into();

    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8", DataType::Utf8, false),
        Field::new("largeutf8", DataType::LargeUtf8, false),
        Field::new("boolean", DataType::Boolean, false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(string_arr),
            Arc::new(large_string_arr),
            Arc::new(bool_arr),
        ],
    )
    .expect("Failed to created arrow utf8 record batch");

    (record_batch, schema)
}

// Time32, Time64
#[allow(clippy::identity_op)]
#[allow(clippy::erasing_op)]
pub(crate) fn get_arrow_time_record_batch() -> (RecordBatch, SchemaRef) {
    // Time32, Time64 Types
    let time32_milli_array: Time32MillisecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000,
        (11 * 3600 + 0 * 60 + 15) * 1_000,
    ]
    .into();
    let time32_sec_array: Time32SecondArray = vec![
        (10 * 3600 + 30 * 60),
        (10 * 3600 + 45 * 60 + 15),
        (11 * 3600 + 00 * 60 + 15),
    ]
    .into();
    let time64_micro_array: Time64MicrosecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000_000,
        (11 * 3600 + 0 * 60 + 15) * 1_000_000,
    ]
    .into();
    let time64_nano_array: Time64NanosecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000_000_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000_000_000,
        (11 * 3600 + 00 * 60 + 15) * 1_000_000_000,
    ]
    .into();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time32_milli",
            DataType::Time32(TimeUnit::Millisecond),
            false,
        ),
        Field::new("time32_sec", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "time64_micro",
            DataType::Time64(TimeUnit::Microsecond),
            false,
        ),
        Field::new("time64_nano", DataType::Time64(TimeUnit::Nanosecond), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(time32_milli_array),
            Arc::new(time32_sec_array),
            Arc::new(time64_micro_array),
            Arc::new(time64_nano_array),
        ],
    )
    .expect("Failed to created arrow time record batch");

    (record_batch, schema)
}

// Timestamp (with/without TZ),
pub(crate) fn get_arrow_timestamp_record_batch() -> (RecordBatch, SchemaRef) {
    // Timestamp Types
    let timestamp_second_array =
        TimestampSecondArray::from(vec![1_680_000_000, 1_680_040_000, 1_680_080_000]);
    let timestamp_milli_array = TimestampMillisecondArray::from(vec![
        1_680_000_000_000,
        1_680_040_000_000,
        1_680_080_000_000,
    ])
    .with_timezone("+10:00".to_string());
    let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
        1_680_000_000_000_000,
        1_680_040_000_000_000,
        1_680_080_000_000_000,
    ])
    .with_timezone("+10:00".to_string());
    let timestamp_nano_array = TimestampNanosecondArray::from(vec![
        1_680_000_000_000_000_000,
        1_680_040_000_000_000_000,
        1_680_080_000_000_000_000,
    ])
    .with_timezone("+10:00".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new(
            "timestamp_milli",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
        Field::new(
            "timestamp_micro",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
        Field::new(
            "timestamp_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(timestamp_second_array),
            Arc::new(timestamp_milli_array),
            Arc::new(timestamp_micro_array),
            Arc::new(timestamp_nano_array),
        ],
    )
    .expect("Failed to created arrow timestamp record batch");

    (record_batch, schema)
}

// Date32, Date64
pub(crate) fn get_arrow_date_record_batch() -> (RecordBatch, SchemaRef) {
    let date32_array = Date32Array::from(vec![
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2015, 3, 14).unwrap_or_default()),
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2016, 1, 12).unwrap_or_default()),
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2017, 9, 17).unwrap_or_default()),
    ]);
    let date64_array = Date64Array::from(vec![
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2015, 3, 14).unwrap_or_default()),
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2016, 1, 12).unwrap_or_default()),
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2017, 9, 17).unwrap_or_default()),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("date32", DataType::Date32, false),
        Field::new("date64", DataType::Date64, false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(date32_array), Arc::new(date64_array)],
    )
    .expect("Failed to created arrow date record batch");

    (record_batch, schema)
}

// struct
pub(crate) fn get_arrow_struct_record_batch() -> (RecordBatch, SchemaRef) {
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(Fields::from(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ])),
        false,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)])
        .expect("Failed to created arrow struct record batch");

    (record_batch, schema)
}

// Decimal128/Decimal256
pub(crate) fn get_arrow_decimal_record_batch() -> (RecordBatch, SchemaRef) {
    let decimal128_array =
        Decimal128Array::from(vec![i128::from(123), i128::from(222), i128::from(321)]);
    let decimal256_array =
        Decimal256Array::from(vec![i256::from(-123), i256::from(222), i256::from(0)]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal128", DataType::Decimal128(38, 10), false),
        Field::new("decimal256", DataType::Decimal256(76, 10), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(decimal128_array), Arc::new(decimal256_array)],
    )
    .expect("Failed to created arrow decimal record batch");

    (record_batch, schema)
}

// Duration
pub(crate) fn get_arrow_duration_record_batch() -> (RecordBatch, SchemaRef) {
    let duration_nano_array = DurationNanosecondArray::from(vec![1, 2, 3]);
    let duration_micro_array = DurationMicrosecondArray::from(vec![1, 2, 3]);
    let duration_milli_array = DurationMillisecondArray::from(vec![1, 2, 3]);
    let duration_sec_array = DurationSecondArray::from(vec![1, 2, 3]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "duration_nano",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        ),
        Field::new(
            "duration_micro",
            DataType::Duration(TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "duration_milli",
            DataType::Duration(TimeUnit::Millisecond),
            false,
        ),
        Field::new("duration_sec", DataType::Duration(TimeUnit::Second), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(duration_nano_array),
            Arc::new(duration_micro_array),
            Arc::new(duration_milli_array),
            Arc::new(duration_sec_array),
        ],
    )
    .expect("Failed to created arrow duration record batch");

    (record_batch, schema)
}

// Interval
pub(crate) fn get_arrow_interval_record_batch() -> (RecordBatch, SchemaRef) {
    let interval_daytime_array = IntervalDayTimeArray::from(vec![
        IntervalDayTime::new(1, 1000),
        IntervalDayTime::new(33, 0),
        IntervalDayTime::new(0, 12 * 60 * 60 * 1000),
    ]);
    let interval_monthday_nano_array = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 1000),
        IntervalMonthDayNano::new(12, 1, 0),
        IntervalMonthDayNano::new(0, 0, 12 * 1000 * 1000),
    ]);
    let interval_yearmonth_array = IntervalYearMonthArray::from(vec![2, 25, -1]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "interval_daytime",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
        Field::new(
            "interval_monthday_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new(
            "interval_yearmonth",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(interval_daytime_array),
            Arc::new(interval_monthday_nano_array),
            Arc::new(interval_yearmonth_array),
        ],
    )
    .expect("Failed to created arrow interval record batch");

    (record_batch, schema)
}

//  List/FixedSizeList/LargeList
pub(crate) fn get_arrow_list_record_batch() -> (RecordBatch, SchemaRef) {
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.append_value([Some(1), Some(2), Some(3)]);
    list_builder.append_value([Some(4)]);
    list_builder.append_value([Some(6)]);
    let list_array = list_builder.finish();

    let mut large_list_builder = LargeListBuilder::new(Int32Builder::new());
    large_list_builder.append_value([Some(1), Some(2), Some(3)]);
    large_list_builder.append_value([Some(4)]);
    large_list_builder.append_value([Some(6)]);
    let large_list_array = large_list_builder.finish();

    let mut fixed_size_list_builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);
    fixed_size_list_builder.values().append_value(0);
    fixed_size_list_builder.values().append_value(1);
    fixed_size_list_builder.values().append_value(2);
    fixed_size_list_builder.append(true);
    fixed_size_list_builder.values().append_value(3);
    fixed_size_list_builder.values().append_value(4);
    fixed_size_list_builder.values().append_value(5);
    fixed_size_list_builder.append(true);
    fixed_size_list_builder.values().append_value(6);
    fixed_size_list_builder.values().append_value(7);
    fixed_size_list_builder.values().append_value(8);
    fixed_size_list_builder.append(true);
    let fixed_size_list_array = fixed_size_list_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "list",
            DataType::List(Field::new("item", DataType::Int32, true).into()),
            false,
        ),
        Field::new(
            "large_list",
            DataType::LargeList(Field::new("item", DataType::Int32, true).into()),
            false,
        ),
        Field::new(
            "fixed_size_list",
            DataType::FixedSizeList(Field::new("item", DataType::Int32, true).into(), 3),
            false,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(list_array),
            Arc::new(large_list_array),
            Arc::new(fixed_size_list_array),
        ],
    )
    .expect("Failed to created arrow list record batch");

    (record_batch, schema)
}

// Null
pub(crate) fn get_arrow_null_record_batch() -> (RecordBatch, SchemaRef) {
    let null_arr = Int8Array::from(vec![Some(1), None, Some(3)]);
    let schema = Arc::new(Schema::new(vec![Field::new("int8", DataType::Int8, true)]));
    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(null_arr)])
        .expect("Failed to created arrow null record batch");
    (record_batch, schema)
}

// BYTEA_ARRAY
pub(crate) fn get_arrow_bytea_array_record_batch() -> (RecordBatch, SchemaRef) {
    let mut bytea_array_builder = ListBuilder::new(BinaryBuilder::new());
    bytea_array_builder.append_value([Some(b"1"), Some(b"2"), Some(b"3")]);
    bytea_array_builder.append_value([Some(b"4")]);
    bytea_array_builder.append_value([Some(b"6")]);
    let bytea_array_builder = bytea_array_builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "bytea_array",
        DataType::List(Field::new("item", DataType::Binary, true).into()),
        false,
    )]));

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(bytea_array_builder)])
            .expect("Failed to created arrow bytea array record batch");

    (record_batch, schema)
}

pub(crate) fn get_pg_interval_expected_result() -> RecordBatch {
    let col1 = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(0, 1, 1000000000),
        IntervalMonthDayNano::new(0, 33, 0),
        IntervalMonthDayNano::new(0, 0, 43200000000000),
    ]);
    let col2 = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 1000),
        IntervalMonthDayNano::new(12, 1, 0),
        IntervalMonthDayNano::new(0, 0, 12 * 1000 * 1000),
    ]);
    let col3 = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(2, 0, 0),
        IntervalMonthDayNano::new(25, 0, 0),
        IntervalMonthDayNano::new(-1, 0, 0),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "interval_daytime",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new(
            "interval_monthday_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new(
            "interval_yearmonth",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
    ]));

    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(col1), Arc::new(col2), Arc::new(col3)],
    )
    .expect("Failed to created arrow interval record batch")
}

// Reference: https://github.com/spiceai/datafusion-federation/blob/spiceai-41/datafusion-federation/src/schema_cast/record_convert.rs#L44
// TODO: Switch to use the try_cast_to in datafusion-federation when the function becomes public
pub(crate) fn try_cast_to(
    record_batch: RecordBatch,
    expected_schema: SchemaRef,
) -> Result<RecordBatch, String> {
    let actual_schema = record_batch.schema();

    if actual_schema.fields().len() != expected_schema.fields().len() {
        return Err("Length mismatch".to_string());
    }

    let cols = expected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, expected_field)| {
            let record_batch_col = record_batch.column(i);

            return cast(&Arc::clone(record_batch_col), expected_field.data_type())
                .map_err(|e| "Failed to cast".to_string());
        })
        .collect::<Result<Vec<Arc<dyn Array>>, String>>()?;

    RecordBatch::try_new(expected_schema, cols)
        .map_err(|e| "Fail to create casted record batch".to_string())
}

// Custom Test Case for Sqlite <-> Arrow Decimal Roundtrip
// SQLite supports up to 16 precision for decimal numbers through REAL type, conforming to IEEE 754 Binary-64 format - https://www.sqlite.org/floatingpoint.html
pub(crate) fn get_sqlite_arrow_decimal_record_batch() -> (RecordBatch, SchemaRef) {
    let decimal128_array =
        Decimal128Array::from(vec![i128::from(123), i128::from(222), i128::from(321)])
            .with_precision_and_scale(16, 10)
            .expect("Fail to create Decimal128 array");
    let decimal256_array =
        Decimal256Array::from(vec![i256::from(-123), i256::from(222), i256::from(0)])
            .with_precision_and_scale(16, 10)
            .expect("Fail to create Decimal256 array");

    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal128", DataType::Decimal128(16, 10), false),
        Field::new("decimal256", DataType::Decimal256(16, 10), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(decimal128_array), Arc::new(decimal256_array)],
    )
    .expect("Failed to created arrow decimal record batch");

    (record_batch, schema)
}
