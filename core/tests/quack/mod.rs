//! Integration tests against a live Quack server (DuckDB 2.0, Quack protocol v3, running `quack_serve`).
//! They run when `QUACK_SERVER_URI` (and, if the server has one, `QUACK_AUTH_TOKEN`) is
//! set, and pass without doing anything otherwise.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::sql::TableReference;
use datafusion_table_providers::quack::{QuackTableFactory, QuackTableProviderFactory};
use datafusion_table_providers::{UnsupportedTypeAction, SOURCE_TYPE_METADATA_KEY};
use futures::StreamExt;

mod common;
use common::*;

async fn provider(
    pool: &Arc<datafusion_table_providers::sql::db_connection_pool::quackpool::QuackConnectionPool>,
    table: &str,
) -> Arc<dyn TableProvider> {
    QuackTableFactory::new(Arc::clone(pool))
        .table_provider(TableReference::bare(table))
        .await
        .unwrap_or_else(|e| panic!("{table}: {e}"))
}

/// Each column of one row, formatted as `name: value`.
fn row(batches: &[RecordBatch], index: usize) -> Vec<String> {
    let batch = batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(move |r| (b, r)))
        .nth(index)
        .expect("row");
    let (batch, r) = batch;
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| format!("{}: {}", f.name(), array_value_to_string(c, r).unwrap()))
        .collect()
}

const TYPES_TABLE: &str = "(
    c_bool BOOLEAN, c_i8 TINYINT, c_i16 SMALLINT, c_i32 INTEGER, c_i64 BIGINT,
    c_u8 UTINYINT, c_u16 USMALLINT, c_u32 UINTEGER, c_u64 UBIGINT, c_i128 HUGEINT, c_u128 UHUGEINT,
    c_f32 FLOAT, c_f64 DOUBLE, c_dec4 DECIMAL(4,2), c_dec18 DECIMAL(18,3), c_dec38 DECIMAL(38,10),
    c_varchar VARCHAR, c_blob BLOB, c_uuid UUID, c_enum ENUM('b','a'), c_json JSON, c_bit BIT,
    c_date DATE, c_time TIME, c_time_ns TIME_NS, c_ts TIMESTAMP, c_ts_s TIMESTAMP_S,
    c_ts_ms TIMESTAMP_MS, c_ts_ns TIMESTAMP_NS, c_tstz TIMESTAMPTZ, c_interval INTERVAL,
    c_list INTEGER[], c_list_str VARCHAR[], c_struct STRUCT(a INTEGER, b VARCHAR),
    c_map MAP(VARCHAR, INTEGER), c_array INTEGER[3], c_variant VARIANT, c_geom GEOMETRY)";

const TYPES_ROW: &str = "(
    true, -128, -32768, -2147483648, -9223372036854775808,
    255, 65535, 4294967295, 18446744073709551615,
    -170141183460469231731687303715884105728, 340282366920938463463374607431768211455,
    1.5, -2.25, 12.34, -123456789012345.678, 1234567890123456789012345678.0123456789,
    'héllo ''q''', '\\x00\\xFF'::BLOB, '6f1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d', 'a',
    '{\"k\": [1, 2]}', '10110',
    DATE '1970-01-02', TIME '23:59:59.999999', '01:02:03.123456789'::TIME_NS,
    TIMESTAMP '2024-02-29 12:34:56.789012', TIMESTAMP_S '2024-02-29 12:34:56',
    TIMESTAMP_MS '2024-02-29 12:34:56.789', '2024-02-29 12:34:56.123456789'::TIMESTAMP_NS,
    TIMESTAMPTZ '2024-02-29 12:34:56+05:00', INTERVAL '1 year 2 months 3 days 04:05:06.000007',
    [1, NULL, 3], ['x', NULL], {'a': 1, 'b': 'z'}, MAP {'k1': 1, 'k2': NULL}, [7, 8, 9],
    42::VARIANT, 'POINT(1 2)')";

#[tokio::test]
async fn round_trips_every_supported_type() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let t = table_name("types");
    run(&pool, &format!("CREATE TABLE {t} {TYPES_TABLE}")).await;
    run(&pool, &format!("INSERT INTO {t} VALUES {TYPES_ROW}")).await;
    run(&pool, &format!("INSERT INTO {t} DEFAULT VALUES")).await;

    let table = provider(&pool, &t).await;
    let schema = table.schema();
    let expected_types = [
        ("c_bool", DataType::Boolean),
        ("c_i8", DataType::Int8),
        ("c_u64", DataType::UInt64),
        ("c_i128", DataType::Decimal256(39, 0)),
        ("c_u128", DataType::Decimal256(39, 0)),
        ("c_dec38", DataType::Decimal128(38, 10)),
        ("c_varchar", DataType::Utf8),
        ("c_blob", DataType::Binary),
        ("c_uuid", DataType::Utf8),
        ("c_enum", DataType::Utf8),
        ("c_time_ns", DataType::Time64(TimeUnit::Nanosecond)),
        ("c_ts_s", DataType::Timestamp(TimeUnit::Second, None)),
        ("c_ts_ns", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        (
            "c_tstz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ];
    for (name, data_type) in expected_types {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &data_type,
            "{name}"
        );
    }
    for field in schema.fields() {
        assert!(
            field.metadata().contains_key(SOURCE_TYPE_METADATA_KEY),
            "{} has no source type",
            field.name()
        );
    }
    assert_eq!(
        schema.field_with_name("c_enum").unwrap().metadata()[SOURCE_TYPE_METADATA_KEY],
        "ENUM('b', 'a')"
    );

    let expected_values = [
        "c_bool: true",
        "c_i8: -128",
        "c_i16: -32768",
        "c_i32: -2147483648",
        "c_i64: -9223372036854775808",
        "c_u8: 255",
        "c_u16: 65535",
        "c_u32: 4294967295",
        "c_u64: 18446744073709551615",
        "c_i128: -170141183460469231731687303715884105728",
        "c_u128: 340282366920938463463374607431768211455",
        "c_f32: 1.5",
        "c_f64: -2.25",
        "c_dec4: 12.34",
        "c_dec18: -123456789012345.678",
        "c_dec38: 1234567890123456789012345678.0123456789",
        "c_varchar: héllo 'q'",
        "c_blob: 00ff",
        "c_uuid: 6f1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
        "c_enum: a",
        "c_json: {\"k\": [1, 2]}",
        // DuckDB's bitstring layout: a padding-count byte, then the bits padded with 1s.
        "c_bit: 03f6",
        "c_date: 1970-01-02",
        "c_time: 23:59:59.999999",
        "c_time_ns: 01:02:03.123456789",
        "c_ts: 2024-02-29T12:34:56.789012",
        "c_ts_s: 2024-02-29T12:34:56",
        "c_ts_ms: 2024-02-29T12:34:56.789",
        "c_ts_ns: 2024-02-29T12:34:56.123456789",
        "c_tstz: 2024-02-29T07:34:56Z",
        "c_interval: 14 mons 3 days 4 hours 5 mins 6.000007000 secs",
        "c_list: [1, , 3]",
        "c_list_str: [x, ]",
        "c_struct: {a: 1, b: z}",
        "c_map: [{key: k1, value: 1}, {key: k2, value: }]",
        "c_array: [7, 8, 9]",
        // quack_protocol's provisional VARIANT mapping: DuckDB's shredded layout.
        "c_variant: {keys: [], children: [], values: [{type_id: 5, byte_offset: 0}], data: 2a000000}",
        // WKB for POINT(1 2).
        "c_geom: 0101000000000000000000f03f0000000000000040",
    ];

    let sql = "SELECT * FROM t ORDER BY c_bool NULLS LAST";
    for ctx in [SessionContext::new(), federated_context()] {
        ctx.register_table("t", Arc::clone(&table)).unwrap();
        let batches = query(&ctx, sql).await;
        assert_eq!(row(&batches, 0), expected_values);
        assert!(
            row(&batches, 1).iter().all(|value| value.ends_with(": ")),
            "{:?}",
            row(&batches, 1)
        );
    }
}

#[tokio::test]
async fn unsupported_types_follow_the_unsupported_type_action() {
    let Some(server) = server() else { return };
    let t = table_name("unsupported");
    let pool = shared(server.pool(&[]).await);
    run(
        &pool,
        &format!(
            "CREATE TABLE {t} (id INTEGER, t TIMETZ, u UNION(i INTEGER, s VARCHAR), \
             st STRUCT(x TIMETZ), n BIGNUM, v VARCHAR)"
        ),
    )
    .await;
    run(
        &pool,
        &format!("INSERT INTO {t} VALUES (1, '01:02:03+04', 1, {{'x': '01:02:03+04'}}, 5, 'a')"),
    )
    .await;

    for action in [UnsupportedTypeAction::Error, UnsupportedTypeAction::String] {
        let pool = Arc::new(server.pool(&[]).await.with_unsupported_type_action(action));
        let Err(err) = QuackTableFactory::new(pool)
            .table_provider(t.as_str())
            .await
        else {
            panic!("{action:?} should reject the table");
        };
        let message = err.to_string();
        assert!(message.contains("'t'"), "{message}");
        assert!(message.contains("TIME WITH TIME ZONE"), "{message}");
    }

    for action in [UnsupportedTypeAction::Warn, UnsupportedTypeAction::Ignore] {
        let pool = Arc::new(server.pool(&[]).await.with_unsupported_type_action(action));
        let table = provider(&pool, &t).await;
        let names: Vec<_> = table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, ["id", "v"], "{action:?}");

        let ctx = SessionContext::new();
        ctx.register_table("t", table).unwrap();
        assert_eq!(
            row(&query(&ctx, "SELECT * FROM t").await, 0),
            ["id: 1", "v: a"]
        );
    }
}

#[tokio::test]
async fn nullability_comes_from_the_catalog_and_batches_match_the_plan_schema() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let t = table_name("nullability");
    run(
        &pool,
        &format!(
            "CREATE TABLE {t} (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, note VARCHAR, \
             amount DECIMAL(10,2) NOT NULL)"
        ),
    )
    .await;
    run(
        &pool,
        &format!("INSERT INTO {t} VALUES (1, 'a', NULL, 1.25), (2, 'b', 'n', 2.5)"),
    )
    .await;

    let table = provider(&pool, &t).await;
    let nullable: Vec<_> = table
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.is_nullable()))
        .collect();
    assert_eq!(
        nullable,
        [
            ("id".to_string(), false),
            ("name".to_string(), false),
            ("note".to_string(), true),
            ("amount".to_string(), false),
        ]
    );

    for ctx in [SessionContext::new(), federated_context()] {
        ctx.register_table("t", Arc::clone(&table)).unwrap();
        for sql in [
            "SELECT * FROM t",
            "SELECT amount, note, id FROM t WHERE id > 0",
            "SELECT count(*) FROM t",
        ] {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
                .await
                .unwrap();
            assert!(!batches.is_empty(), "{sql}");
            for batch in &batches {
                assert_eq!(batch.schema(), plan.schema(), "{sql}");
            }
        }
        let batches = query(&ctx, "SELECT amount, id FROM t").await;
        assert_eq!(
            batches[0].schema().field(0).metadata()[SOURCE_TYPE_METADATA_KEY],
            "DECIMAL(10,2)"
        );
    }
}

/// Rows chosen so that pushing any `Unsupported` filter to DuckDB would change the answer:
/// a NOCASE collation, NaN and -0.0, ENUM order that differs from label order, nanosecond
/// timestamps and HUGEINT values beyond 64 bits.
const PUSHDOWN_TABLE: &str = "(id INTEGER, i INTEGER, u UBIGINT, dec DECIMAL(10,2), d DATE,
    ts TIMESTAMP, ts_s TIMESTAMP_S, ts_ns TIMESTAMP_NS, tstz TIMESTAMPTZ, b BOOLEAN,
    s VARCHAR COLLATE NOCASE, f DOUBLE, e ENUM('z', 'a'), h HUGEINT)";

const PUSHDOWN_ROWS: &str = "
    (1, 1, 1, 1.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP_S '2024-01-01 00:00:00',
     '2024-01-01 00:00:00.000000001'::TIMESTAMP_NS, TIMESTAMPTZ '2024-01-01 00:00:00+00', true, 'x', 'NaN', 'z', 1),
    (2, 2, 18446744073709551615, -2.25, DATE '2024-06-30', TIMESTAMP '2024-06-30 12:00:00.5', TIMESTAMP_S '2024-06-30 12:00:00',
     '2024-06-30 12:00:00.123456789'::TIMESTAMP_NS, TIMESTAMPTZ '2024-06-30 12:00:00+02', false, 'X', '-0.0', 'a',
     170141183460469231731687303715884105727),
    (3, 3, 5, 0.00, DATE '2024-12-31', TIMESTAMP '2024-12-31 23:59:59.999999', TIMESTAMP_S '2024-12-31 23:59:59',
     '2024-12-31 23:59:59.999999999'::TIMESTAMP_NS, TIMESTAMPTZ '2024-12-31 23:59:59+00', true, 'y', 0.0, 'z', -5),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (5, 5, 0, 99.99, DATE '1999-01-01', TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP_S '1999-01-01 00:00:00',
     '1999-01-01 00:00:00'::TIMESTAMP_NS, TIMESTAMPTZ '1999-01-01 00:00:00+00', false, 'Xylophone', 2.5, 'a', 0)";

/// A session with the table registered as `t` through the provider, and the same rows
/// in a `MemTable` registered as `expected` that DataFusion filters itself.
async fn pushdown_context(server: &Server) -> SessionContext {
    let pool = shared(server.pool(&[]).await);
    let t = table_name("pushdown");
    run(&pool, &format!("CREATE TABLE {t} {PUSHDOWN_TABLE}")).await;
    run(&pool, &format!("INSERT INTO {t} VALUES {PUSHDOWN_ROWS}")).await;

    let ctx = SessionContext::new();
    let table = provider(&pool, &t).await;
    ctx.register_table("t", Arc::clone(&table)).unwrap();
    let rows = query(&ctx, "SELECT * FROM t").await;
    let copy = MemTable::try_new(table.schema(), vec![rows]).unwrap();
    ctx.register_table("expected", Arc::new(copy)).unwrap();
    ctx
}

#[tokio::test]
async fn filters_are_pushed_down_only_when_exact() {
    let Some(server) = server() else { return };
    let ctx = pushdown_context(&server).await;

    let cases = [
        // (filter, pushed down to DuckDB as Exact)
        ("i = 3", true),
        ("i <> 3", true),
        ("i < 3 AND i IS NOT NULL", true),
        ("i IN (1, 5)", true),
        ("i NOT IN (1, 5)", true),
        ("NOT (i > 2)", true),
        ("i > 1 OR b = true", true),
        ("u >= 5", true),
        ("u = 18446744073709551615", true),
        ("dec >= 1.50", true),
        ("dec < 0", true),
        ("d = DATE '2024-06-30'", true),
        ("d > DATE '2024-01-01'", true),
        ("ts > TIMESTAMP '2024-01-01 00:00:00'", true),
        ("ts <= TIMESTAMP '2024-06-30 12:00:00.5'", true),
        ("b = false", true),
        ("s IS NULL", true),
        ("f IS NOT NULL", true),
        ("e IS NULL", true),
        ("s = 'x'", false),
        ("s < 'y'", false),
        ("s IN ('x', 'y')", false),
        ("s LIKE 'x%'", false),
        ("f > 1.0", false),
        ("f = 0.0", false),
        ("e < 'b'", false),
        ("e = 'a'", false),
        ("ts_ns = TIMESTAMP '2024-06-30 12:00:00.123456789'", false),
        ("ts_ns > TIMESTAMP '2024-06-30 12:00:00.123456'", false),
        ("h > 5", false),
        ("tstz > TIMESTAMP '2024-06-30 00:00:00'", false),
        ("i + 1 > 2", false),
        // DataFusion unwraps a lossless cast before asking, so this one arrives as `i > 2`.
        ("CAST(i AS BIGINT) > 2", true),
        ("CAST(ts AS DATE) = DATE '2024-06-30'", false),
        ("abs(i) > 2", false),
    ];

    for (filter, exact) in cases {
        let sql = format!("SELECT id FROM t WHERE {filter} ORDER BY id");
        let plan = physical_plan(&ctx, &sql).await;
        let scan = plan
            .lines()
            .find(|line| line.contains("QuackSqlExec"))
            .unwrap_or_else(|| panic!("{filter}: no Quack scan in\n{plan}"));
        if exact {
            assert!(
                !plan.contains("FilterExec"),
                "{filter}: kept a FilterExec\n{plan}"
            );
            assert!(
                scan.contains(" WHERE "),
                "{filter}: not pushed down\n{plan}"
            );
        } else {
            assert!(
                plan.contains("FilterExec"),
                "{filter}: no FilterExec\n{plan}"
            );
            assert!(!scan.contains(" WHERE "), "{filter}: pushed down\n{plan}");
        }

        let actual = pretty(&query(&ctx, &sql).await);
        let expected = pretty(
            &query(
                &ctx,
                &format!("SELECT id FROM expected WHERE {filter} ORDER BY id"),
            )
            .await,
        );
        assert_eq!(actual, expected, "{filter}");
    }
}

#[tokio::test]
async fn limit_projection_count_and_sort() {
    let Some(server) = server() else { return };
    let ctx = pushdown_context(&server).await;

    let scan = |plan: &str| {
        plan.lines()
            .find(|line| line.contains("QuackSqlExec"))
            .map(str::to_string)
            .unwrap_or_else(|| panic!("no Quack scan in\n{plan}"))
    };

    // Limit, with and without an exact filter.
    let sql = "SELECT id FROM t LIMIT 3";
    assert!(scan(&physical_plan(&ctx, sql).await)
        .trim_end()
        .ends_with("LIMIT 3"));
    assert_eq!(
        query(&ctx, sql)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    let sql = "SELECT id FROM t WHERE i > 1 LIMIT 2";
    let plan = physical_plan(&ctx, sql).await;
    assert!(
        scan(&plan).contains(" WHERE ") && scan(&plan).trim_end().ends_with("LIMIT 2"),
        "{plan}"
    );
    assert_eq!(
        query(&ctx, sql)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );

    // Projection: only the needed columns are fetched.
    let plan = physical_plan(&ctx, "SELECT dec, id FROM t").await;
    let scanned = scan(&plan);
    let columns = &scanned[scanned.find("SELECT").unwrap()..scanned.find(" FROM ").unwrap()];
    assert_eq!(columns.matches(", ").count(), 1, "{plan}");
    assert!(
        columns.contains(r#"."id""#) && columns.contains(r#"."dec""#),
        "{plan}"
    );

    // Empty projection.
    let plan = physical_plan(&ctx, "SELECT count(*) FROM t").await;
    assert!(scan(&plan).contains("SELECT 1 FROM"), "{plan}");
    assert_eq!(
        pretty(&query(&ctx, "SELECT count(*) AS n FROM t").await),
        pretty(&query(&ctx, "SELECT count(*) AS n FROM expected").await)
    );
    assert_eq!(
        row(
            &query(&ctx, "SELECT count(*) AS n FROM t WHERE i >= 2").await,
            0
        ),
        ["n: 3"]
    );

    // Sorts on DuckDB-ordered types run remotely; DataFusion keeps its TopK.
    let sql = "SELECT id FROM t ORDER BY i DESC NULLS LAST LIMIT 2";
    let plan = physical_plan(&ctx, sql).await;
    assert!(
        scan(&plan).contains(r#"ORDER BY "i" DESC NULLS LAST"#),
        "{plan}"
    );
    assert_eq!(
        pretty(&query(&ctx, sql).await),
        pretty(&query(&ctx, &sql.replace(" t ", " expected ")).await)
    );

    // VARCHAR (collation) and DOUBLE (NaN, -0.0) sorts stay in DataFusion.
    for sql in [
        "SELECT id, s FROM t ORDER BY s, id",
        "SELECT id, f FROM t ORDER BY f DESC, id LIMIT 3",
    ] {
        let plan = physical_plan(&ctx, sql).await;
        assert!(!scan(&plan).contains("ORDER BY"), "{sql}\n{plan}");
        assert_eq!(
            pretty(&query(&ctx, sql).await),
            pretty(&query(&ctx, &sql.replace(" t ", " expected ")).await),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn federated_joins_run_on_the_server_when_tables_share_a_pool() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let (orders, customers) = (table_name("orders"), table_name("customers"));
    run(&pool, &format!("CREATE TABLE {orders} AS SELECT x AS id, x % 3 AS customer_id, x * 10 AS amount FROM range(9) r(x)")).await;
    run(
        &pool,
        &format!("CREATE TABLE {customers} (id BIGINT, name VARCHAR)"),
    )
    .await;
    run(
        &pool,
        &format!("INSERT INTO {customers} VALUES (0, 'ann'), (1, 'bo'), (2, 'cy')"),
    )
    .await;

    let sql = "SELECT c.name, sum(o.amount) AS total FROM o JOIN c ON o.customer_id = c.id \
               GROUP BY c.name ORDER BY c.name";
    let expected = "+------+-------+\n\
                    | name | total |\n\
                    +------+-------+\n\
                    | ann  | 90    |\n\
                    | bo   | 120   |\n\
                    | cy   | 150   |\n\
                    +------+-------+";

    let ctx = federated_context();
    ctx.register_table("o", provider(&pool, &orders).await)
        .unwrap();
    ctx.register_table("c", provider(&pool, &customers).await)
        .unwrap();
    let plan = physical_plan(&ctx, sql).await;
    assert_eq!(plan.matches("VirtualExecutionPlan").count(), 1, "{plan}");
    assert!(plan.contains(" JOIN "), "{plan}");
    assert_eq!(pretty(&query(&ctx, sql).await), expected);

    // Tables on different pools are joined by DataFusion.
    let other_pool = shared(server.pool(&[]).await);
    let ctx = federated_context();
    ctx.register_table("o", provider(&pool, &orders).await)
        .unwrap();
    ctx.register_table("c", provider(&other_pool, &customers).await)
        .unwrap();
    let plan = physical_plan(&ctx, sql).await;
    assert_eq!(plan.matches("VirtualExecutionPlan").count(), 2, "{plan}");
    assert_eq!(pretty(&query(&ctx, sql).await), expected);
}

#[tokio::test]
async fn federated_scan_applies_filters_pushed_into_it_at_execution() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let t = table_name("runtime_filters");
    run(
        &pool,
        &format!("CREATE TABLE {t} AS SELECT x AS k FROM range(1000) r(x)"),
    )
    .await;

    let ctx = federated_context();
    ctx.register_table("q", provider(&pool, &t).await).unwrap();
    let local = query(&ctx, "SELECT * FROM (VALUES (5), (17), (123)) v(k)").await;
    ctx.register_table(
        "m",
        Arc::new(MemTable::try_new(local[0].schema(), vec![local]).unwrap()),
    )
    .unwrap();

    // Federation leaves these filters above the federated scan; DataFusion then pushes
    // them into it and drops its own FilterExec, so the executor must apply them.
    for (sql, expected) in [
        (
            "SELECT count(*) AS n FROM q WHERE k < (SELECT max(k) FROM m)",
            "n: 123",
        ),
        (
            "SELECT count(*) AS n FROM (SELECT k FROM q UNION ALL SELECT k FROM m) u WHERE k < 10",
            "n: 11",
        ),
    ] {
        let plan = physical_plan(&ctx, sql).await;
        let above_scan = plan
            .lines()
            .take_while(|line| !line.contains("VirtualExecutionPlan"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!above_scan.contains("FilterExec: k@0"), "{sql}\n{plan}");
        assert_eq!(row(&query(&ctx, sql).await, 0), [expected], "{sql}\n{plan}");
    }

    // A hash join's dynamic filter reaches the executor too.
    let sql = "SELECT q.k FROM m JOIN q ON m.k = q.k ORDER BY q.k";
    assert_eq!(
        pretty(&query(&ctx, sql).await),
        "+-----+\n| k   |\n+-----+\n| 5   |\n| 17  |\n| 123 |\n+-----+"
    );
}

#[tokio::test]
async fn federated_aggregate_that_overflows_the_plan_type_is_an_error() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let t = table_name("overflow");
    run(&pool, &format!("CREATE TABLE {t} (v BIGINT)")).await;
    run(
        &pool,
        &format!("INSERT INTO {t} VALUES (9223372036854775807), (9223372036854775807)"),
    )
    .await;

    let ctx = federated_context();
    ctx.register_table("t", provider(&pool, &t).await).unwrap();
    // DuckDB sums BIGINT into a HUGEINT; the plan expects Int64.
    let err = ctx
        .sql("SELECT sum(v) AS total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("the sum does not fit Int64");
    let message = err.to_string();
    assert!(message.contains("Cannot convert column"), "{message}");
    assert!(message.contains("Int64"), "{message}");
}

#[tokio::test]
async fn exhausted_pool_times_out_and_a_dropped_stream_frees_its_session() {
    let Some(server) = server() else { return };
    let pool = shared(
        server
            .pool(&[
                ("connection_pool_size", "1"),
                ("connection_pool_acquire_timeout", "1"),
            ])
            .await,
    );
    let t = table_name("exhaust");
    run(
        &pool,
        &format!("CREATE TABLE {t} AS SELECT x AS k FROM range(1000000) r(x)"),
    )
    .await;

    let ctx = SessionContext::new();
    ctx.register_table("t", provider(&pool, &t).await).unwrap();

    // An open scan holds the only session.
    let mut stream = ctx
        .sql("SELECT k FROM t")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let first = stream.next().await.expect("a batch").expect("no error");
    assert!(first.num_rows() > 0);

    let err = ctx
        .sql("SELECT count(*) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("the pool is exhausted");
    let message = err.to_string();
    assert!(message.contains("connection_pool_size"), "{message}");
    assert!(message.contains("all 1 are in use"), "{message}");

    // Dropping the stream partway returns the session, which serves the next query.
    drop(stream);
    assert_eq!(
        row(
            &query(
                &ctx,
                "SELECT count(*) AS n, max(k) AS m FROM t WHERE k < 10"
            )
            .await,
            0
        ),
        ["n: 10", "m: 9"]
    );
    assert_eq!(
        row(&query(&ctx, "SELECT count(*) AS n FROM t").await, 0),
        ["n: 1000000"]
    );
}

#[tokio::test]
async fn table_names_resolve_like_duckdb_and_missing_tables_are_reported() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let name = format!("Mixed{}", table_name("Case"));
    run(&pool, &format!("CREATE TABLE \"{name}\" (id INTEGER)")).await;
    run(&pool, &format!("INSERT INTO \"{name}\" VALUES (7)")).await;
    let view = table_name("view");
    run(
        &pool,
        &format!("CREATE VIEW {view} AS SELECT id FROM \"{name}\""),
    )
    .await;

    for reference in [
        TableReference::bare(name.as_str()),
        TableReference::bare(name.to_lowercase()),
        TableReference::partial("main", name.as_str()),
        TableReference::bare(view.as_str()),
    ] {
        let table = QuackTableFactory::new(Arc::clone(&pool))
            .table_provider(reference.clone())
            .await
            .unwrap_or_else(|e| panic!("{reference}: {e}"));
        let ctx = SessionContext::new();
        ctx.register_table("t", table).unwrap();
        assert_eq!(row(&query(&ctx, "SELECT id FROM t").await, 0), ["id: 7"]);
    }

    let missing = table_name("missing");
    let Err(err) = QuackTableFactory::new(Arc::clone(&pool))
        .table_provider(missing.as_str())
        .await
    else {
        panic!("{missing} should not exist");
    };
    let message = err.to_string();
    assert!(
        message.contains(&missing) && message.contains("not found"),
        "{message}"
    );
}

#[tokio::test]
async fn create_external_table_attaches_to_an_existing_table() {
    let Some(server) = server() else { return };
    let pool = shared(server.pool(&[]).await);
    let (a, b) = (table_name("ext_a"), table_name("ext_b"));
    run(
        &pool,
        &format!("CREATE TABLE {a} AS SELECT x AS id FROM range(3) r(x)"),
    )
    .await;
    run(
        &pool,
        &format!("CREATE TABLE {b} AS SELECT x AS id, 'n' || x AS name FROM range(3) r(x)"),
    )
    .await;

    let mut state =
        SessionStateBuilder::from(datafusion_federation::default_session_state()).build();
    state.table_factories_mut().insert(
        "QUACK".to_string(),
        Arc::new(QuackTableProviderFactory::new()),
    );
    let ctx = SessionContext::new_with_state(state);

    let options = server
        .options(&[])
        .iter()
        .map(|(k, v)| format!("'{k}' '{v}'"))
        .collect::<Vec<_>>()
        .join(", ");
    for (local, remote) in [("a", &a), ("b", &b)] {
        query(
            &ctx,
            &format!("CREATE EXTERNAL TABLE {local} STORED AS QUACK LOCATION 'main.{remote}' OPTIONS ({options})"),
        )
        .await;
    }
    let sql = "SELECT b.name FROM a JOIN b ON a.id = b.id WHERE a.id > 0 ORDER BY b.name";
    assert_eq!(
        pretty(&query(&ctx, sql).await),
        "+------+\n| name |\n+------+\n| n1   |\n| n2   |\n+------+"
    );
    // Same options, same pool: the join runs on the server.
    assert_eq!(
        physical_plan(&ctx, sql)
            .await
            .matches("VirtualExecutionPlan")
            .count(),
        1
    );

    for (statement, expected) in [
        (
            format!("CREATE EXTERNAL TABLE c (id BIGINT) STORED AS QUACK LOCATION '{a}' OPTIONS ({options})"),
            "take their schema from the server",
        ),
        (
            format!("CREATE EXTERNAL TABLE d STORED AS QUACK LOCATION '{a}' OPTIONS ({options}, 'tokn' 'x')"),
            "Unknown parameter 'tokn'",
        ),
        (
            format!("CREATE EXTERNAL TABLE e STORED AS QUACK LOCATION 'no_such_table' OPTIONS ({options})"),
            "not found",
        ),
    ] {
        let err = match ctx.sql(&statement).await {
            Ok(df) => df.collect().await.expect_err(&statement),
            Err(e) => e,
        };
        assert!(err.to_string().contains(expected), "{statement}: {err}");
    }
}
