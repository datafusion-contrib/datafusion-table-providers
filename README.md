# DataFusion Table Providers

Note: This is not an official Apache Software Foundation project.

The goal of this repo is to extend the capabilities of DataFusion to support additional data sources via implementations of the `TableProvider` trait.

Many of the table providers in this repo are for querying data from other database systems. Those providers also integrate with the `datafusion-federation` crate to allow for more efficient query execution, such as pushing down joins between multiple tables from the same database system, or efficiently implementing TopK style queries (`SELECT * FROM table ORDER BY foo LIMIT 10`).

## Table Providers

- PostgreSQL
- MySQL
- SQLite
- DuckDB