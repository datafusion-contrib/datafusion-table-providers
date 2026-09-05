all:
	cargo build --all-features

.PHONY: test
test:
	cargo test --features clickhouse,duckdb,flight,mysql,postgres,sqlite,adbc -p datafusion-table-providers --lib
	cargo test -p datafusion-table-providers-oracle

.PHONY: lint
lint:
	cargo clippy --all-features

.PHONY: test-integration
test-integration:
	RUST_LOG=$${RUST_LOG:-info} cargo test -p datafusion-table-providers --test integration --no-default-features --features postgres,sqlite,mysql,flight,clickhouse,duckdb,adbc -- --nocapture
