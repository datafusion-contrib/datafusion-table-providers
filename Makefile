all:
	cargo build --all-features

.PHONY: test
test:
	cargo test --features duckdb-federation,flight,mysql-federation,postgres-federation,sqlite-federation

.PHONY: lint
lint:
	cargo clippy --all-features

.PHONY: test-integration
test-integration:
	RUST_LOG=debug cargo test --test integration --no-default-features --features postgres,sqlite,mysql,flight -- --nocapture
