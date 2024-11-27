# DataFusion Table Providers

Note: This is not an official Apache Software Foundation project.

The goal of this repo is to extend the capabilities of DataFusion to support additional data sources via implementations of the `TableProvider` trait.

Many of the table providers in this repo are for querying data from other database systems. Those providers also integrate with the [`datafusion-federation`](https://github.com/datafusion-contrib/datafusion-federation/) crate to allow for more efficient query execution, such as pushing down joins between multiple tables from the same database system, or efficiently implementing TopK style queries (`SELECT * FROM table ORDER BY foo LIMIT 10`).

To use these table providers with efficient federation push-down, add the `datafusion-federation` crate and create a DataFusion `SessionContext` using the Federation optimizer rule and query planner with:

```rust
use datafusion::prelude::SessionContext;

let state = datafusion_federation::default_session_state();
let ctx = SessionContext::with_state(state);

// Register the specific table providers into ctx
// queries will now automatically be federated
```

## Table Providers

- PostgreSQL
- MySQL
- SQLite
- DuckDB
- Flight SQL
- ODBC

## Examples

Run the included examples to see how to use the table providers:

### DuckDB

```bash
# Read from a table in a DuckDB file
cargo run --example duckdb --features duckdb
# Create an external table backed by DuckDB directly in DataFusion
cargo run --example duckdb_external_table --features duckdb
# Use the result of a DuckDB function call as the source of a table
cargo run --example duckdb_function --features duckdb
```

### SQLite

```bash
cargo run --example sqlite --features sqlite
```

### Postgres

In order to run the Postgres example, you need to have a Postgres server running. You can use the following command to start a Postgres server in a Docker container the example can use:

```bash
docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
# Wait for the Postgres server to start
sleep 30

# Create a table in the Postgres server and insert some data
docker exec -i postgres psql -U postgres -d postgres_db <<EOF
CREATE TABLE companies (
   id INT PRIMARY KEY,
  name VARCHAR(100)
);

INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
EOF
```

```bash
cargo run --example postgres --features postgres
```

### MySQL

In order to run the MySQL example, you need to have a MySQL server running. You can use the following command to start a MySQL server in a Docker container the example can use:

```bash
docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=mysql_db -p 3306:3306 -d mysql:9.0
# Wait for the MySQL server to start
sleep 30

# Create a table in the MySQL server and insert some data
docker exec -i mysql mysql -uroot -ppassword mysql_db <<EOF
CREATE TABLE companies (
   id INT PRIMARY KEY,
  name VARCHAR(100)
);

INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
EOF
```

```bash
cargo run --example mysql --features mysql
```

### Flight SQL

```bash
brew install roapi
# or
# cargo install --locked --git https://github.com/roapi/roapi --branch main --bins roapi
roapi -t taxi=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet &

cargo run --example flight-sql --features flight
```

### ODBC

```bash
apt-get install unixodbc-dev libsqliteodbc
# or
# brew install unixodbc & brew install sqliteodbc
# If you use ARM Mac, please see https://github.com/pacman82/odbc-api#os-x-arm--mac-m1

cargo run --example odbc_sqlite --features odbc
```
