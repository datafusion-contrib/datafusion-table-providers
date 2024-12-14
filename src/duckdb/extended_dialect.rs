use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Function, FunctionArgExpr, Ident, ObjectName,
};
use datafusion::sql::unparser::dialect::{CharacterLengthStyle, Dialect, DuckDBDialect};
use itertools::Itertools;
pub struct ExtendedDuckDBDialect {
    inner: DuckDBDialect,
}

impl ExtendedDuckDBDialect {
    pub fn new() -> Self {
        Self {
            inner: DuckDBDialect {},
        }
    }
}

impl Dialect for ExtendedDuckDBDialect {
    fn identifier_quote_style(&self, ident: &str) -> Option<char> {
        self.inner.identifier_quote_style(ident)
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.inner.character_length_style()
    }

    fn division_operator(&self) -> BinaryOperator {
        self.inner.division_operator()
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &datafusion::sql::unparser::Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
        if func_name == "cosine_distance" {
            cosine_distance_to_sql(unparser, args)
        } else {
            self.inner
                .scalar_function_to_sql_overrides(unparser, func_name, args)
        }
    }
}

/// Converts the `cosine_distance` UDF into DuckDB `array_cosine_distance` function:
/// `https://duckdb.org/docs/sql/functions/array.html#array_cosine_distancearray1-array2`
///
///  - replaces `make_array` function with the array constructor (`make_array` is not supported in DuckDB)
///  - adds required ::FLOAT[array_len] casting, otherwise DuckDB will throw an error:
///
/// SQL Error: java.sql.SQLException: Binder Error: No function matches the given name and argument types 'array_cosine_distance(FLOAT[384], DOUBLE[])'. You might need to add explicit type casts.
///  Candidate functions:
///  array_cosine_distance(FLOAT[ANY], FLOAT[ANY]) -> FLOAT
///  array_cosine_distance(DOUBLE[ANY], DOUBLE[ANY]) -> DOUBLE
///
fn cosine_distance_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    let ast_args: Vec<ast::Expr> = args
        .iter()
        .map(|arg| match arg {
            // embeddings array is wrapped in a make_array function, unwrap it
            Expr::ScalarFunction(scalar_func)
                if scalar_func.name().to_lowercase() == "make_array" =>
            {
                let num_elements = scalar_func.args.len() as u64;

                let array = ast::Expr::Array(ast::Array {
                    elem: scalar_func
                        .args
                        .iter()
                        .map(|x| unparser.expr_to_sql(x))
                        .try_collect()?,
                    named: false,
                });

                // Apply required ::FLOAT[] casting. Only FLOAT emneddings are curently supported
                Ok(ast::Expr::Cast {
                    expr: Box::new(array),
                    data_type: ast::DataType::Array(ast::ArrayElemTypeDef::SquareBracket(
                        Box::new(ast::DataType::Float(None)),
                        Some(num_elements),
                    )),
                    kind: ast::CastKind::DoubleColon,
                    format: None,
                })
            }
            // For all other expressions, directly convert them to SQL
            _ => unparser.expr_to_sql(arg),
        })
        .try_collect()?;

    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![Ident {
            value: "array_cosine_distance".to_string(),
            quote_style: None,
        }]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: ast_args
                .into_iter()
                .map(|x| ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(x)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
    });

    Ok(Some(ast_fn))
}
#[cfg(test)]
mod tests {
    use datafusion::{
        common::Column,
        functions_array::make_array::make_array_udf,
        logical_expr::expr::ScalarFunction,
        prelude::lit,
        scalar::ScalarValue,
        sql::{unparser::Unparser, TableReference},
    };

    use super::*;

    #[test]
    fn test_cosine_distance_to_sql_scalars() {
        let dialect = ExtendedDuckDBDialect::new();
        let unparser = Unparser::new(&dialect);
        let args = vec![
            // raw values
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![lit(1.0), lit(2.0), lit(3.0)],
            )),
            // values wrapped as literals
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![
                    Expr::Literal(ScalarValue::Float32(Some(4.0))),
                    Expr::Literal(ScalarValue::Float32(Some(5.0))),
                    Expr::Literal(ScalarValue::Float32(Some(6.0))),
                ],
            )),
        ];

        let result = cosine_distance_to_sql(&unparser, &args).unwrap();
        let expected =
            "array_cosine_distance([1.0, 2.0, 3.0]::FLOAT[3], [4.0, 5.0, 6.0]::FLOAT[3])";

        assert_eq!(result.unwrap().to_string(), expected);
    }

    #[test]
    fn test_cosine_distance_to_sql_column_and_scalar() {
        let dialect = ExtendedDuckDBDialect::new();
        let unparser = Unparser::new(&dialect);
        let args = vec![
            Expr::Column(Column {
                relation: Some(TableReference::from("table_name")),
                name: "column_name".to_string(),
            }),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![
                    Expr::Literal(ScalarValue::Float32(Some(4.0))),
                    Expr::Literal(ScalarValue::Float32(Some(5.0))),
                    Expr::Literal(ScalarValue::Float32(Some(6.0))),
                ],
            )),
        ];

        let result = cosine_distance_to_sql(&unparser, &args).unwrap();
        let expected =
            r#"array_cosine_distance("table_name"."column_name", [4.0, 5.0, 6.0]::FLOAT[3])"#;

        assert_eq!(result.unwrap().to_string(), expected);
    }
}
