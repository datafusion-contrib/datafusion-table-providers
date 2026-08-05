//! Connector-agnostic **schema projection** (JSON nesting).
//!
//! A single abstraction that unifies two column-shaping features behind one
//! config and one generic execution core, so adding a future connector is a
//! small [`RowShape`] adapter rather than a per-feature reimplementation:
//!
//! 1. **Declared schema** — pin/override column types ([`ColumnSource::Field`]
//!    columns).
//! 2. **JSON nesting** — collapse every field not claimed by a declared column
//!    into one catch-all JSON `Utf8` column (a single
//!    [`ColumnSource::JsonObject`] column).
//!
//! The two generic entry points are [`SchemaProjection::project_schema`] (build
//! the exposed Arrow schema) and [`SchemaProjection::project_row`] (reshape one
//! source row just before Arrow conversion). Each connector contributes a
//! [`RowShape`] adapter for its native value type (`serde_json::Value`, BSON,
//! DynamoDB `AttributeValue`, …); a nested object is simply a value that is
//! itself an object.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::util::schema::merge_inferred_and_declared_schemas;

/// The metadata key (under a column's `metadata:` block) that marks the
/// catch-all JSON column. Only the value `"*"` is supported.
pub const JSON_OBJECT_MARKER: &str = "json_object";

/// The only supported value for [`JSON_OBJECT_MARKER`].
pub const JSON_OBJECT_WILDCARD: &str = "*";

/// How a single projected (output) column draws its value from a source row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSource {
    /// A declared top-level field, read by its own name and kept as its own
    /// column (used for declared-schema type pinning).
    Field,
    /// The catch-all: every source field not claimed by a `Field` column,
    /// serialized as a single sorted-key JSON object string.
    JsonObject,
}

/// One declared output column and how it is sourced from the row.
#[derive(Debug, Clone)]
pub struct ProjectedColumn {
    /// The output/queryable column name (equal to the upstream key).
    pub output_name: String,
    /// Where the value comes from.
    pub source: ColumnSource,
    /// Optional pinned Arrow type. When `None`, the type is taken from the
    /// inferred schema (or `Utf8` for the catch-all / an unknown source).
    pub declared_type: Option<DataType>,
    /// Whether the output column is nullable.
    pub nullable: bool,
}

/// Errors raised while building a [`SchemaProjection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaProjectionError {
    /// More than one column carries the `json_object` catch-all marker.
    MultipleCatchAll { columns: Vec<String> },
    /// The same output column name is declared twice.
    DuplicateColumn { output_name: String },
    /// A required column (e.g. a primary key) was folded into the catch-all
    /// instead of being declared.
    RequiredColumnInCatchAll { output_name: String },
}

impl std::fmt::Display for SchemaProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultipleCatchAll { columns } => write!(
                f,
                "multiple columns carry the '{JSON_OBJECT_MARKER}' catch-all marker: {}. Only one column may be the JSON object column.",
                columns.join(", ")
            ),
            Self::DuplicateColumn { output_name } => {
                write!(f, "column '{output_name}' is declared more than once")
            }
            Self::RequiredColumnInCatchAll { output_name } => write!(
                f,
                "column '{output_name}' must be declared explicitly and cannot be folded into the '{JSON_OBJECT_MARKER}' catch-all"
            ),
        }
    }
}

impl std::error::Error for SchemaProjectionError {}

/// A fully-resolved projection: the declared columns plus the precomputed
/// structures that drive row reshaping.
#[derive(Debug, Clone)]
pub struct SchemaProjection {
    columns: Vec<ProjectedColumn>,
    /// Output names of the declared `Field` (kept-as-is) columns.
    static_fields: HashSet<String>,
    /// Output name of the catch-all column, if any.
    catch_all: Option<String>,
}

impl SchemaProjection {
    /// Build a JSON-nesting projection directly from a set of kept (declared)
    /// field names plus one catch-all column. Infallible: the field names are
    /// deduplicated into a set and there is exactly one catch-all, so none of
    /// [`SchemaProjectionError`]'s conditions can arise. Handy for connectors
    /// (e.g. HTTP) that already know their static/catch-all split and don't
    /// want a fallible constructor.
    #[must_use]
    pub fn nesting(
        static_fields: impl IntoIterator<Item = String>,
        catch_all: impl Into<String>,
    ) -> Self {
        let catch_all = catch_all.into();
        let mut seen = HashSet::new();
        let ordered_fields: Vec<String> = static_fields
            .into_iter()
            .filter(|name| seen.insert(name.clone()))
            .collect();
        let static_fields: HashSet<String> = ordered_fields.iter().cloned().collect();
        let mut columns: Vec<ProjectedColumn> = ordered_fields
            .into_iter()
            .map(|name| ProjectedColumn {
                output_name: name,
                source: ColumnSource::Field,
                declared_type: None,
                nullable: true,
            })
            .collect();
        columns.push(ProjectedColumn {
            output_name: catch_all.clone(),
            source: ColumnSource::JsonObject,
            declared_type: None,
            nullable: true,
        });
        Self {
            columns,
            static_fields,
            catch_all: Some(catch_all),
        }
    }

    /// Build a projection from the declared columns. Validates: at most one
    /// catch-all, no duplicate output names, and that none of
    /// `required_columns` is folded into the catch-all.
    ///
    /// `required_columns` are columns that must be declared explicitly (e.g.
    /// primary-key / CDC-key columns) — they must appear as declared `Field`
    /// columns and may not be the catch-all.
    pub fn new(
        columns: Vec<ProjectedColumn>,
        required_columns: &[String],
    ) -> Result<Self, SchemaProjectionError> {
        let mut catch_all: Option<String> = None;
        let mut catch_all_names: Vec<String> = Vec::new();
        let mut static_fields: HashSet<String> = HashSet::new();
        let mut seen: HashSet<&str> = HashSet::new();

        for col in &columns {
            if !seen.insert(col.output_name.as_str()) {
                return Err(SchemaProjectionError::DuplicateColumn {
                    output_name: col.output_name.clone(),
                });
            }
            match col.source {
                ColumnSource::JsonObject => {
                    catch_all_names.push(col.output_name.clone());
                    catch_all = Some(col.output_name.clone());
                }
                ColumnSource::Field => {
                    static_fields.insert(col.output_name.clone());
                }
            }
        }

        if catch_all_names.len() > 1 {
            return Err(SchemaProjectionError::MultipleCatchAll {
                columns: catch_all_names,
            });
        }

        // A required column must be a declared `Field` — never folded into the
        // catch-all. This only matters when a catch-all exists; without one,
        // non-declared columns pass through unchanged (open world), so there is
        // nothing to fold them into.
        if catch_all.is_some() {
            for req in required_columns {
                if !static_fields.contains(req.as_str()) {
                    return Err(SchemaProjectionError::RequiredColumnInCatchAll {
                        output_name: req.clone(),
                    });
                }
            }
        }

        Ok(Self {
            columns,
            static_fields,
            catch_all,
        })
    }

    /// True when the projection leaves every row unchanged — i.e. there is no
    /// catch-all, so only (optional) type-pinning declared columns apply.
    /// Callers can skip [`Self::project_row`] entirely on this path.
    #[must_use]
    pub fn is_identity(&self) -> bool {
        self.catch_all.is_none()
    }

    /// True when a catch-all (`json_object`) column is configured.
    #[must_use]
    pub fn has_catch_all(&self) -> bool {
        self.catch_all.is_some()
    }

    /// The declared output columns, in declared order.
    #[must_use]
    pub fn columns(&self) -> &[ProjectedColumn] {
        &self.columns
    }

    /// The output names of the declared `Field` (kept-as-is) columns. Useful
    /// for connectors that must validate declared columns against a source
    /// schema, or decide projection pushdown.
    #[must_use]
    pub fn static_fields(&self) -> &HashSet<String> {
        &self.static_fields
    }

    /// The catch-all column's output name, if a `json_object` column exists.
    #[must_use]
    pub fn catch_all_name(&self) -> Option<&str> {
        self.catch_all.as_deref()
    }

    /// Build the exposed Arrow schema for this projection.
    ///
    /// - **Closed world** (a catch-all exists): the schema is exactly the
    ///   declared columns in declared order; the catch-all is `Utf8`. Field
    ///   types come from `declared_type`, falling back to the inferred type of
    ///   the same-named source key, then `Utf8`.
    /// - **Open world** (no catch-all): preserves the legacy declared-schema
    ///   merge via [`merge_inferred_and_declared_schemas`] — inferred columns
    ///   pass through and declared columns override their types.
    #[must_use]
    pub fn project_schema(&self, inferred: SchemaRef) -> SchemaRef {
        let inferred_by_name: HashMap<&str, &Field> = inferred
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.as_ref()))
            .collect();

        if self.catch_all.is_some() {
            // Closed world: only the declared columns + catch-all.
            let fields: Vec<Field> = self
                .columns
                .iter()
                .map(|c| self.field_for(c, &inferred_by_name))
                .collect();
            return Arc::new(Schema::new(fields));
        }

        // Open world: build the declared schema and merge with inferred.
        let declared_fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| self.field_for(c, &inferred_by_name))
            .collect();
        if declared_fields.is_empty() {
            return inferred;
        }
        let declared_schema: SchemaRef = Arc::new(Schema::new(declared_fields));
        merge_inferred_and_declared_schemas(inferred, Some(&declared_schema))
    }

    /// Resolve the Arrow field for a declared column.
    fn field_for(&self, col: &ProjectedColumn, inferred_by_name: &HashMap<&str, &Field>) -> Field {
        let data_type = match (&col.source, &col.declared_type) {
            (_, Some(dt)) => dt.clone(),
            (ColumnSource::JsonObject, None) => DataType::Utf8,
            (ColumnSource::Field, None) => inferred_by_name
                .get(col.output_name.as_str())
                .map_or(DataType::Utf8, |f| f.data_type().clone()),
        };
        Field::new(&col.output_name, data_type, col.nullable)
    }

    /// Reshape one source row into the declared output columns: kept (declared)
    /// fields stay as-is and every other field is folded into the catch-all as
    /// a single sorted-key JSON object string. The returned value is the same
    /// native type, ready for the connector's existing row→Arrow conversion.
    ///
    /// When there is no catch-all the row is returned untouched (the projection
    /// only pins types, which is handled by [`Self::project_schema`]).
    #[must_use]
    pub fn project_row<R: RowShape>(&self, row: R) -> R {
        let Some(catch_all) = &self.catch_all else {
            return row;
        };

        let entries = match row.into_object() {
            Ok(entries) => entries,
            Err(scalar) => {
                // A non-object row (array/primitive): no declared field
                // matches, so the whole row goes to the catch-all.
                let json = serde_json::to_string(&scalar.to_json()).unwrap_or_default();
                return R::from_object(vec![(catch_all.clone(), R::from_json_string(json))]);
            }
        };

        let mut out: Vec<(String, R)> = Vec::new();
        // BTreeMap → the catch-all object has alphabetically sorted, stable keys.
        let mut rest: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        for (key, value) in entries {
            if self.static_fields.contains(&key) {
                out.push((key, value));
            } else {
                rest.insert(key, value.to_json());
            }
        }

        if !rest.is_empty() {
            let json = serde_json::to_string(&rest).unwrap_or_default();
            out.push((catch_all.clone(), R::from_json_string(json)));
        }
        // Empty remainder → omit the catch-all so it becomes SQL NULL.

        R::from_object(out)
    }
}

/// A connector's native row value, viewed as a possibly-nested object. One
/// impl per connector value type (`serde_json::Value`, `bson::Bson`, DynamoDB
/// `AttributeValue`, …) makes the generic core reusable.
pub trait RowShape: Sized {
    /// Enumerate one object level as `(key, value)` pairs, or return the value
    /// unchanged via `Err` when it is a scalar/array (not an object).
    ///
    /// # Errors
    /// Returns `Err(self)` when the value is not an object.
    fn into_object(self) -> Result<Vec<(String, Self)>, Self>;
    /// Rebuild one object level from `(key, value)` pairs.
    fn from_object(entries: Vec<(String, Self)>) -> Self;
    /// Convert (recursively) to `serde_json::Value` for the catch-all column.
    fn to_json(&self) -> serde_json::Value;
    /// Wrap an already-serialized catch-all JSON string as a native value
    /// (typically the connector's string variant).
    fn from_json_string(json: String) -> Self;
}

impl RowShape for serde_json::Value {
    fn into_object(self) -> Result<Vec<(String, Self)>, Self> {
        match self {
            serde_json::Value::Object(map) => Ok(map.into_iter().collect()),
            other => Err(other),
        }
    }

    fn from_object(entries: Vec<(String, Self)>) -> Self {
        serde_json::Value::Object(entries.into_iter().collect())
    }

    fn to_json(&self) -> serde_json::Value {
        self.clone()
    }

    fn from_json_string(json: String) -> Self {
        serde_json::Value::String(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn field(name: &str) -> ProjectedColumn {
        ProjectedColumn {
            output_name: name.to_string(),
            source: ColumnSource::Field,
            declared_type: None,
            nullable: true,
        }
    }

    fn catch_all(name: &str) -> ProjectedColumn {
        ProjectedColumn {
            output_name: name.to_string(),
            source: ColumnSource::JsonObject,
            declared_type: None,
            nullable: true,
        }
    }

    fn proj(columns: Vec<ProjectedColumn>) -> SchemaProjection {
        SchemaProjection::new(columns, &[]).expect("valid projection")
    }

    #[test]
    fn decomposes_object_into_static_and_catchall() {
        let p = proj(vec![field("id"), field("title"), catch_all("data")]);
        let out = p.project_row(json!({
            "id": "abc",
            "title": "hello",
            "description": "a value",
            "count": 42,
            "nested": {"x": 1, "y": [1, 2]}
        }));
        assert_eq!(out["id"], json!("abc"));
        assert_eq!(out["title"], json!("hello"));
        let catchall: serde_json::Value =
            serde_json::from_str(out["data"].as_str().expect("catchall string")).expect("parse");
        assert_eq!(catchall["description"], json!("a value"));
        assert_eq!(catchall["count"], json!(42));
        assert_eq!(catchall["nested"]["x"], json!(1));
    }

    #[test]
    fn missing_static_field_is_null() {
        let p = proj(vec![field("id"), field("title"), catch_all("data")]);
        let out = p.project_row(json!({"id": "abc"}));
        assert_eq!(out["id"], json!("abc"));
        assert!(out.get("title").is_none());
        assert!(out.get("data").is_none());
    }

    #[test]
    fn non_object_row_goes_to_catchall() {
        let p = proj(vec![field("id"), catch_all("data")]);
        let out = p.project_row(json!([1, 2, 3]));
        assert!(out.get("id").is_none());
        assert_eq!(out["data"], json!("[1,2,3]"));
    }

    #[test]
    fn catchall_keys_are_sorted() {
        let p = proj(vec![field("id"), catch_all("data")]);
        let out = p.project_row(json!({"id": "x", "zeta": 1, "alpha": 2, "mu": 3}));
        assert_eq!(out["data"], json!(r#"{"alpha":2,"mu":3,"zeta":1}"#));
    }

    #[test]
    fn null_value_is_preserved_in_catchall() {
        let p = proj(vec![field("id"), catch_all("data")]);
        let out = p.project_row(json!({"id": null, "extra": 1}));
        assert_eq!(out["id"], json!(null));
        assert_eq!(out["data"], json!(r#"{"extra":1}"#));
    }

    #[test]
    fn rejects_multiple_catch_all() {
        let err = SchemaProjection::new(vec![catch_all("a"), catch_all("b")], &[])
            .expect_err("two catch-alls");
        assert!(matches!(
            err,
            SchemaProjectionError::MultipleCatchAll { .. }
        ));
    }

    #[test]
    fn rejects_duplicate_output_name() {
        let err = SchemaProjection::new(vec![field("a"), field("a")], &[]).expect_err("duplicate");
        assert!(matches!(err, SchemaProjectionError::DuplicateColumn { .. }));
    }

    #[test]
    fn rejects_required_column_in_catch_all() {
        let err = SchemaProjection::new(vec![catch_all("data")], &["id".to_string()])
            .expect_err("pk in catch-all");
        assert!(matches!(
            err,
            SchemaProjectionError::RequiredColumnInCatchAll { .. }
        ));
    }

    #[test]
    fn no_catch_all_passes_row_through() {
        let p = proj(vec![field("id"), field("name")]);
        assert!(p.is_identity());
        let row = json!({"id": 1, "name": "x", "other": true});
        assert_eq!(p.project_row(row.clone()), row);
    }

    #[test]
    fn project_schema_closed_world_is_declared_columns_only() {
        let p = proj(vec![
            ProjectedColumn {
                output_name: "id".to_string(),
                source: ColumnSource::Field,
                declared_type: Some(DataType::Int64),
                nullable: false,
            },
            field("title"),
            catch_all("data"),
        ]);
        let inferred = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
            Field::new("junk", DataType::Utf8, true),
        ]));
        let schema = p.project_schema(inferred);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "title", "data"]);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }
}
