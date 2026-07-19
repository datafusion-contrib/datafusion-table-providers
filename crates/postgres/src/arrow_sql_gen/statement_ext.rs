use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_table_providers_common::sql::arrow_sql_gen::statement::CreateTableBuilder;
use sea_query::{ColumnType, PostgresQueryBuilder};

use crate::arrow_sql_gen::builder::TypeBuilder;
use crate::arrow_sql_gen::{
    get_postgres_composite_type_name, map_data_type_to_column_type_postgres,
};

/// Extends [`CreateTableBuilder`] with the ability to build a PostgreSQL-specific
/// `CREATE TABLE` statement, including any composite types (i.e. structs) that
/// need to be created ahead of the table itself.
pub trait CreateTableBuilderPostgresExt {
    fn build_postgres(self) -> Vec<String>;
}

impl CreateTableBuilderPostgresExt for CreateTableBuilder {
    fn build_postgres(self) -> Vec<String> {
        let schema = self.schema();
        let table_name = self.table_name().to_string();

        let main_table_creation =
            self.build(PostgresQueryBuilder, &|f: &Arc<Field>| -> ColumnType {
                map_data_type_to_column_type_postgres(f.data_type(), &table_name, f.name())
            });

        // Postgres supports composite types (i.e. Structs) but needs to have the type defined first
        // https://www.postgresql.org/docs/current/rowtypes.html
        let mut creation_stmts = Vec::new();
        for field in schema.fields() {
            let DataType::Struct(struct_inner_fields) = field.data_type() else {
                continue;
            };
            let type_builder = TypeBuilder::new(
                get_postgres_composite_type_name(&table_name, field.name()),
                struct_inner_fields,
            );
            creation_stmts.push(type_builder.build());
        }

        creation_stmts.push(main_table_creation);
        creation_stmts
    }
}
