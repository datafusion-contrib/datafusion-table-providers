use std::sync::Arc;

use arrow_schema::TimeUnit;
use datafusion::{
    common::HashMap,
    logical_expr::Expr,
    sql::{
        sqlparser::ast,
        unparser::{
            dialect::{
                DateFieldExtractStyle, Dialect, IntervalStyle, MySqlDialect as DFMySqlDialect,
                ScalarFnToSqlHandler,
            },
            Unparser,
        },
    },
};

pub struct MySqlDialect {
    inner: DFMySqlDialect,
    custom_scalar_fn_overrides: HashMap<String, ScalarFnToSqlHandler>,
}

impl Default for MySqlDialect {
    fn default() -> Self {
        Self {
            inner: DFMySqlDialect {},
            custom_scalar_fn_overrides: HashMap::new(),
        }
    }
}

impl Dialect for MySqlDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.inner.supports_nulls_first_in_sort()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.utf8_cast_dtype()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.large_utf8_cast_dtype()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.inner.int64_cast_dtype()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.inner.int32_cast_dtype()
    }

    fn timestamp_cast_dtype(&self, _time_unit: &TimeUnit, _tz: &Option<Arc<str>>) -> ast::DataType {
        self.inner.timestamp_cast_dtype(_time_unit, _tz)
    }

    fn requires_derived_table_alias(&self) -> bool {
        true
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> datafusion::error::Result<Option<ast::Expr>> {
        if let Some(handler) = self.custom_scalar_fn_overrides.get(func_name) {
            return handler(unparser, args);
        }

        self.inner
            .scalar_function_to_sql_overrides(unparser, func_name, args)
    }

    fn with_custom_scalar_overrides(mut self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self
    where
        Self: Sized,
    {
        for (func_name, handler) in handlers {
            self.custom_scalar_fn_overrides
                .insert(func_name.to_string(), handler);
        }
        self
    }
}
