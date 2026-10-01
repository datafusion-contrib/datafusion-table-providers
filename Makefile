all:
	cargo build --features clickhouse,flight,mongodb,mysql,postgres,sqlite,oracle

.PHONY: test
test:
	cargo test --features clickhouse,flight,mysql,postgres,sqlite -p datafusion-table-providers --lib
	cargo test -p datafusion-table-providers-oracle

.PHONY: lint
lint:
	cargo clippy --features clickhouse,flight,mongodb,mysql,postgres,sqlite,oracle

.PHONY: test-integration
test-integration:
	RUST_LOG=$${RUST_LOG:-info} cargo test -p datafusion-table-providers --test integration --no-default-features --features postgres,sqlite,mysql,flight,clickhouse -- --nocapture
