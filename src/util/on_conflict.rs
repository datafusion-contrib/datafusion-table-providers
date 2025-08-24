use arrow::datatypes::SchemaRef;
use itertools::Itertools;
use sea_query::{self, Alias};
use snafu::prelude::*;
use std::fmt::Display;

use crate::util::constraints::{self, UpsertOptions};

use super::column_reference::{self, ColumnReference};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid column reference: {source}"))]
    InvalidColumnReference { source: column_reference::Error },

    #[snafu(display("Expected do_nothing or upsert, found: {token}"))]
    UnexpectedToken { token: String },

    #[snafu(display("Expected semicolon in: {token}"))]
    ExpectedSemicolon { token: String },

    #[snafu(display("Invalid upsert options: {source}"))]
    InvalidUpsertOptions { source: constraints::Error },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflict {
    DoNothingAll,
    DoNothing(ColumnReference),
    Upsert(ColumnReference, UpsertOptions),
}

impl OnConflict {
    #[must_use]
    pub fn build_on_conflict_statement(&self, schema: &SchemaRef) -> String {
        match self {
            OnConflict::DoNothingAll => "ON CONFLICT DO NOTHING".to_string(),
            OnConflict::DoNothing(column) => {
                format!(
                    r#"ON CONFLICT ("{}") DO NOTHING"#,
                    column.iter().join(r#"", ""#)
                )
            }
            OnConflict::Upsert(column, _) => {
                let non_constraint_columns = schema
                    .fields()
                    .iter()
                    .filter(|f| !column.contains(f.name()))
                    .map(|f| f.name().to_string())
                    .collect::<Vec<String>>();
                let mut update_cols = String::new();
                for (i, col) in non_constraint_columns.iter().enumerate() {
                    update_cols.push_str(&format!(r#""{col}" = EXCLUDED."{col}""#));
                    if i < non_constraint_columns.len() - 1 {
                        update_cols.push_str(", ");
                    }
                }
                // This means that all columns are constraint columns, so we should do nothing
                if update_cols.is_empty() {
                    return format!(
                        r#"ON CONFLICT ("{}") DO NOTHING"#,
                        column.iter().join(r#"", ""#)
                    );
                }
                format!(
                    r#"ON CONFLICT ("{}") DO UPDATE SET {update_cols}"#,
                    column.iter().join(r#"", ""#)
                )
            }
        }
    }

    #[must_use]
    pub fn build_sea_query_on_conflict(&self, schema: &SchemaRef) -> sea_query::OnConflict {
        match self {
            OnConflict::DoNothingAll => {
                let mut on_conflict = sea_query::OnConflict::new();
                on_conflict.do_nothing();
                on_conflict
            }
            OnConflict::DoNothing(column) => {
                let mut on_conflict = sea_query::OnConflict::columns::<Vec<Alias>, Alias>(
                    column.iter().map(Alias::new).collect(),
                );
                on_conflict.do_nothing();
                on_conflict
            }
            OnConflict::Upsert(column, _) => {
                let mut on_conflict = sea_query::OnConflict::columns::<Vec<Alias>, Alias>(
                    column.iter().map(Alias::new).collect(),
                );

                let non_constraint_columns = schema
                    .fields()
                    .iter()
                    .filter(|f| !column.contains(f.name()))
                    .map(|f| Alias::new(f.name()))
                    .collect::<Vec<Alias>>();

                on_conflict.update_columns(non_constraint_columns);

                on_conflict
            }
        }
    }
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflict::DoNothingAll => write!(f, "do_nothing_all"),
            OnConflict::DoNothing(column) => write!(f, "do_nothing:{column}"),
            OnConflict::Upsert(column, options) => write!(f, "upsert:{column}#{options}"),
        }
    }
}

impl TryFrom<&str> for OnConflict {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        if value == "do_nothing_all" {
            return Ok(OnConflict::DoNothingAll);
        }

        let parts: Vec<&str> = value.split(':').collect();
        if parts.len() != 2 {
            return ExpectedSemicolonSnafu {
                token: value.to_string(),
            }
            .fail();
        }

        let upsert_parts: Vec<&str> = parts[1].split('#').collect();

        let column_ref =
            ColumnReference::try_from(upsert_parts[0]).context(InvalidColumnReferenceSnafu)?;

        let upsert_options = if parts[0] == "upsert" {
            if upsert_parts.len() == 2 {
                UpsertOptions::try_from(upsert_parts[1]).context(InvalidUpsertOptionsSnafu)?
            } else {
                UpsertOptions::default()
            }
        } else {
            UpsertOptions::default()
        };

        let on_conflict_behavior = parts[0];
        match on_conflict_behavior {
            "do_nothing" => Ok(OnConflict::DoNothing(column_ref)),
            "upsert" => Ok(OnConflict::Upsert(column_ref, upsert_options)),
            _ => UnexpectedTokenSnafu {
                token: parts[0].to_string(),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::util::{
        column_reference::ColumnReference, constraints::UpsertOptions, on_conflict::OnConflict,
    };

    #[test]
    fn test_on_conflict_from_str() {
        let on_conflict = OnConflict::try_from("do_nothing_all").expect("valid on conflict");
        assert_eq!(on_conflict, OnConflict::DoNothingAll);

        let on_conflict = OnConflict::try_from("do_nothing:col1").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()]))
        );

        let on_conflict = OnConflict::try_from("upsert:col2").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col2".to_string()]),
                UpsertOptions::default()
            )
        );

        let err = OnConflict::try_from("do_nothing").expect_err("invalid on conflict");
        assert_eq!(
            err.to_string(),
            "Expected semicolon in: do_nothing".to_string()
        );
    }

    #[test]
    fn test_roundtrip() {
        let on_conflict = OnConflict::DoNothingAll.to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::DoNothingAll
        );

        let on_conflict =
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()])).to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()]))
        );

        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col2".to_string()]),
            UpsertOptions::default(),
        )
        .to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::Upsert(
                ColumnReference::new(vec!["col2".to_string()]),
                UpsertOptions::default()
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_default_options() {
        let on_conflict = OnConflict::try_from("upsert:col1").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::default()
            )
        );

        // Test explicit empty options string
        let on_conflict = OnConflict::try_from("upsert:col1#").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::default()
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_remove_duplicates() {
        let on_conflict =
            OnConflict::try_from("upsert:col1#remove_duplicates").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::new().with_remove_duplicates(true)
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_last_write_wins() {
        let on_conflict =
            OnConflict::try_from("upsert:col1#last_write_wins").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::new().with_last_write_wins(true)
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_both_options() {
        let on_conflict = OnConflict::try_from("upsert:col1#remove_duplicates,last_write_wins")
            .expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::new()
                    .with_remove_duplicates(true)
                    .with_last_write_wins(true)
            )
        );

        // Test reverse order
        let on_conflict = OnConflict::try_from("upsert:col1#last_write_wins,remove_duplicates")
            .expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::new()
                    .with_remove_duplicates(true)
                    .with_last_write_wins(true)
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_spaces_in_options() {
        let on_conflict = OnConflict::try_from("upsert:col1# remove_duplicates , last_write_wins ")
            .expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string()]),
                UpsertOptions::new()
                    .with_remove_duplicates(true)
                    .with_last_write_wins(true)
            )
        );
    }

    #[test]
    fn test_upsert_parsing_with_invalid_options() {
        let err =
            OnConflict::try_from("upsert:col1#invalid_option").expect_err("invalid upsert option");
        assert!(err.to_string().contains("Invalid upsert options"));

        let err = OnConflict::try_from("upsert:col1#remove_duplicates,invalid_option")
            .expect_err("invalid upsert option");
        assert!(err.to_string().contains("Invalid upsert options"));
    }

    #[test]
    fn test_upsert_parsing_with_composite_columns() {
        let on_conflict = OnConflict::try_from("upsert:(col1,col2)#remove_duplicates")
            .expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(
                ColumnReference::new(vec!["col1".to_string(), "col2".to_string()]),
                UpsertOptions::new().with_remove_duplicates(true)
            )
        );
    }

    #[test]
    fn test_upsert_roundtrip_with_options() {
        // Test default options
        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col1".to_string()]),
            UpsertOptions::default(),
        );
        let roundtrip =
            OnConflict::try_from(on_conflict.to_string().as_str()).expect("valid roundtrip");
        assert_eq!(roundtrip, on_conflict);

        // Test with remove_duplicates
        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col1".to_string()]),
            UpsertOptions::new().with_remove_duplicates(true),
        );
        let roundtrip =
            OnConflict::try_from(on_conflict.to_string().as_str()).expect("valid roundtrip");
        assert_eq!(roundtrip, on_conflict);

        // Test with last_write_wins
        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col1".to_string()]),
            UpsertOptions::new().with_last_write_wins(true),
        );
        let roundtrip =
            OnConflict::try_from(on_conflict.to_string().as_str()).expect("valid roundtrip");
        assert_eq!(roundtrip, on_conflict);

        // Test with both options
        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col1".to_string()]),
            UpsertOptions::new()
                .with_remove_duplicates(true)
                .with_last_write_wins(true),
        );
        let roundtrip =
            OnConflict::try_from(on_conflict.to_string().as_str()).expect("valid roundtrip");
        assert_eq!(roundtrip, on_conflict);
    }

    #[test]
    fn test_build_on_conflict_statement() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int64, false),
        ]));
        let on_conflict = OnConflict::DoNothingAll;
        assert_eq!(
            on_conflict.build_on_conflict_statement(&schema),
            "ON CONFLICT DO NOTHING".to_string()
        );

        let on_conflict = OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()]));
        assert_eq!(
            on_conflict.build_on_conflict_statement(&schema),
            r#"ON CONFLICT ("col1") DO NOTHING"#.to_string()
        );

        let on_conflict = OnConflict::Upsert(
            ColumnReference::new(vec!["col2".to_string()]),
            UpsertOptions::default(),
        );
        assert_eq!(
            on_conflict.build_on_conflict_statement(&schema),
            r#"ON CONFLICT ("col2") DO UPDATE SET "col1" = EXCLUDED."col1""#.to_string()
        );

        // Test that upsert options don't affect SQL statement generation
        // (the options are used during batch validation, not SQL generation)
        let on_conflict_with_options = OnConflict::Upsert(
            ColumnReference::new(vec!["col2".to_string()]),
            UpsertOptions::new()
                .with_remove_duplicates(true)
                .with_last_write_wins(true),
        );
        assert_eq!(
            on_conflict_with_options.build_on_conflict_statement(&schema),
            r#"ON CONFLICT ("col2") DO UPDATE SET "col1" = EXCLUDED."col1""#.to_string()
        );
    }
}
