all:
	cargo build --all-features

.PHONY: test
test:
	RUST_LOG=$${RUST_LOG:-info} cargo test --all-features --lib -- --nocapture
	cargo test -p datafusion-table-providers-duckdb --no-default-features --features quack,federation

.PHONY: lint
lint:
	cargo clippy --all-features

.PHONY: test-integration
test-integration:
	RUST_LOG=$${RUST_LOG:-info} cargo test -p datafusion-table-providers --test integration --all-features -- --nocapture
